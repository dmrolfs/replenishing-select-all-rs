use async_trait::async_trait;
use futures::future::{self, BoxFuture, FutureExt};
use std::fmt;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio::sync::Mutex;

#[async_trait]
pub trait Stage: fmt::Debug + Send {
    fn name(&self) -> &str;
    async fn run(&mut self);
    async fn close(&mut self);
}

#[derive(Debug)]
pub enum MergeMsg {
    Stop { tx: oneshot::Sender<()>},
}

/// MergeN multiple sources. Picks elements randomly if all sources has elements ready.
///
/// # Examples
///
/// ```rust
/// use rand::Rng;
/// use replenishing::telemetry::{get_subscriber, init_subscriber};
/// use replenishing::{MergeN, MergeMsg, Stage};
/// use std::fmt;
/// use std::time::Duration;
/// use tokio::sync::{mpsc, oneshot};
/// use tokio::task::JoinHandle;
/// use std:: sync:: Arc;
/// use tokio::sync::Mutex;
///
/// #[tokio::main]
/// async fn main() {
///     let subscriber = get_subscriber("simple", "trace");
///     init_subscriber(subscriber);
///
///     let main_span = tracing::info_span!("main");
///     let _main_span_guard = main_span.enter();
///
///     let (tx_0, rx_0) = mpsc::channel(8);
///     let (tx_1, rx_1) = mpsc::channel(8);
///     let (tx_2, rx_2) = mpsc::channel(8);
///     let merge_inlets = vec![rx_0, rx_1, rx_2];
///
///     let (tx_merge, mut rx_merge) = mpsc::channel(8);
///
///     let (mut merge, tx_merge_api) = MergeN::new("merge", merge_inlets, tx_merge);
///     let m = tokio::spawn(async move {
///         merge.run().await;
///     });
///
///     let stop = tokio::spawn(async move {
///         tokio::time::sleep(Duration::from_millis(500)).await;
///         let (tx, rx) = oneshot::channel();
///         tx_merge_api.send(MergeMsg::Stop { tx }).expect("failed to send stop to merge");
///         let _ = rx.await;
///         tracing::warn!("STOPPED MERGE");
///     });
///
///     let h0 = spawn_transmission("ONES", 1..9, tx_0);
///     let h1 = spawn_transmission("TENS", 11..99, tx_1);
///     let h2 = spawn_transmission("HUNDREDS", 101..=999, tx_2);
///
///     let count = Arc::new(Mutex::new(0));
///     let r_count = count.clone();
///     let r = tokio::spawn(async move {
///         while let Some(item) = rx_merge.recv().await {
///             let mut tally = r_count.lock().await;
///             *tally += 1;
///             tracing::info!(%item, nr_received=%tally, "Received item");
///         }
///     });
///
///     h0.await.unwrap();
///     h1.await.unwrap();
///     h2.await.unwrap();
///
///     m.await.unwrap();
///     r.await.unwrap();
///     stop.await.unwrap();
///
///     let tally: i32 = *count.lock().await;
///     tracing::info!(%tally, "Done!");
///     assert!(5 < tally);
///     assert!(tally < 15);
/// }
///
/// fn spawn_transmission<S, I, T>(name: S, data: I, tx: mpsc::Sender<T>) -> JoinHandle<()>
/// where
///     T: fmt::Debug + Send + 'static,
///     S: AsRef<str> + Send + 'static,
///     I: IntoIterator<Item = T> + Send + 'static,
///     <I as IntoIterator>::IntoIter: Send,
/// {
///     tokio::spawn(async move {
///         for item in data.into_iter() {
///             let delay = Duration::from_millis(rand::thread_rng().gen_range(25..=250));
///             tokio::time::sleep(delay).await;
///
///             let send_span = tracing::info_span!(
///                 "introducing item to channel",
///                 ?item,
///                 ?delay,
///                 channel=%name.as_ref()
///             );
///             let _send_span_guard = send_span.enter();
///
///             match tx.send(item).await {
///                 Ok(_) => tracing::info!("successfully introduced item to channel."),
///                 Err(err) => {
///                     tracing::error!(error=?err, "failed to introduce item to channel.");
///                     break;
///                 }
///             };
///         }
///     })
/// }
/// ```
pub struct MergeN<T>
where
    T: fmt::Debug + Send + Sync + 'static,
{
    name: String,
    inlets: Arc<Mutex<Vec<Arc<Mutex<mpsc::Receiver<T>>>>>>,
    outlet: mpsc::Sender<T>,
    rx_api: mpsc::UnboundedReceiver<MergeMsg>,
}

impl<T> MergeN<T>
where
    T: fmt::Debug + Send + Sync + 'static,
{
    pub fn new<S, R>(name: S, inlets: R, outlet: mpsc::Sender<T>) -> (Self, mpsc::UnboundedSender<MergeMsg>)
    where
        S: Into<String>,
        R: IntoIterator<Item = mpsc::Receiver<T>>,
    {
        let (tx_api, rx_api) = mpsc::unbounded_channel();
        let inlets = Arc::new(Mutex::new(inlets.into_iter().map(|i| Arc::new(Mutex::new(i))).collect()));
        ( Self { name: name.into(), inlets, outlet, rx_api }, tx_api )
    }
}

#[async_trait]
impl<T> Stage for MergeN<T>
where
    T: fmt::Debug + Send + Sync + 'static,
{
    fn name(&self) -> &str {
        self.name.as_str()
    }

    #[tracing::instrument(
        level="info",
        name="run merge",
        skip(self),
        fields(name=%self.name),
    )]
    async fn run(&mut self) {
        let mut active_inlets = self.initialize_active_inlets().await;
        let outlet = &self.outlet;
        let inlets = self.inlets.clone();
        let rx_api = &mut self.rx_api;

        while !active_inlets.is_empty() {
            let available_inlets = active_inlets;
            tracing::info!(nr_available_inlets=%available_inlets.len(), "1.selecting from active inlets");

            tokio::select! {
                ((inlet_idx, value), target_idx, remaining) = future::select_all(available_inlets) => {
                    let is_active = value.is_some();
                    active_inlets = MergeN::handle_inlet_selection( value, inlet_idx, target_idx, remaining, inlets.clone(), outlet ).await;
                    tracing::info!(nr_remaining=%active_inlets.len(), %is_active, ">>>> loop bottom <<<<");
                },

                Some(msg) = rx_api.recv() => match msg {
                    MergeMsg::Stop { tx } => {
                        tracing::info!("handling request to stop merge_n.");
                        let _ = tx.send(());
                        break;
                    }
                }
            }
        }
    }

    #[tracing::instrument(
        level="info",
        name="close merge",
        skip(self),
        fields(name=%self.name),
    )]
    async fn close(&mut self) {
        for i in self.inlets.lock().await.iter() {
            i.lock().await.close();
        }
        self.outlet.closed().await;
    }
}

impl<T> MergeN<T>
where
    T: fmt::Debug + Send + Sync + 'static,
{
    #[tracing::instrument(level = "info", skip(self))]
    async fn initialize_active_inlets<'a>(&self) -> Vec<BoxFuture<'a, (usize, Option<T>)>> {
        let inlets = self.inlets.lock().await;
        let mut active_inlets = Vec::with_capacity(inlets.len());
        for (idx, inlet) in inlets.iter().enumerate() {
            let rep = MergeN::replenish_inlet_recv(idx, inlet.clone()).boxed();
            active_inlets.push(rep);
        }

        active_inlets
    }

    #[tracing::instrument(level = "info", name = "replenish inlet recv", skip(inlet), fields(inlet_idx=%idx))]
    async fn replenish_inlet_recv(idx: usize, inlet: Arc<Mutex<mpsc::Receiver<T>>>) -> (usize, Option<T>) {
        let mut receiver = inlet.lock().await;
        (idx, receiver.recv().await)
    }

    #[tracing::instrument(
        level="info",
        name="handle inlet selection",
        skip(remaining, inlets, outlet,),
        fields(nr_remaining=%remaining.len(),),
    )]
    async fn handle_inlet_selection<'a>(
        value: Option<T>,
        inlet_idx: usize,
        pulled_idx: usize,
        remaining: Vec<BoxFuture<'a, (usize, Option<T>)>>,
        inlets: Arc<Mutex<Vec<Arc<Mutex<mpsc::Receiver<T>>>>>>,
        outlet: &mpsc::Sender<T>,
    ) -> Vec<BoxFuture<'a, (usize, Option<T>)>> {
        let mut remaining_inlets = remaining;
        let is_active = value.is_some();

        if let Some(item) = value {
            let send_active_span = tracing::info_span!("3.send item to outlet", ?item);
            let _send_active_guard = send_active_span.enter();
            outlet.send(item).await.expect("failed to send to outlet");
        }

        tracing::info!(nr_remaining=%remaining_inlets.len(), %is_active, "after send");

        if is_active {
            let run_active_span = tracing::info_span!(
                            "4.replenish active inlets",
                            nr_available_inlets=%remaining_inlets.len()
                        );
            let _run_active_guard = run_active_span.enter();

            if let Some(inlet) = inlets.lock().await.get(inlet_idx) {
                let rep = MergeN::replenish_inlet_recv(inlet_idx, inlet.clone()).boxed();
                remaining_inlets.push(rep);
                // remaining_targets.insert(idx, rep);
                tracing::info!(nr_available_inlets=%remaining_inlets.len(), "4.1.active_inlets replenished.");
            }
        }

        remaining_inlets
    }
}

impl<T> fmt::Debug for MergeN<T>
    where
        T: fmt::Debug + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MergeN")
            .field("name", &self.name)
            .field("inlets", &self.inlets)
            .field("outlet", &self.outlet)
            .finish()
    }
}
