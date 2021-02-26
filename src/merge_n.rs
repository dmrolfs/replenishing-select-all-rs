use async_trait::async_trait;
use futures::future::{BoxFuture, FutureExt};
use std::fmt;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex;

#[async_trait]
pub trait Stage: fmt::Debug + Send {
    fn name(&self) -> &str;
    async fn run(&mut self);
    async fn close(&mut self);
}

/// MergeN multiple sources. Picks elements randomly if all sources has elements ready.
///
/// # Examples
///
/// ```
/// ```
pub struct MergeN<T>
where
    T: fmt::Debug + Send + Sync + 'static,
{
    name: String,
    inlets: Arc<Mutex<Vec<Arc<Mutex<mpsc::Receiver<T>>>>>>,
    outlet: mpsc::Sender<T>,
}

impl<T> MergeN<T>
where
    T: fmt::Debug + Send + Sync + 'static,
{
    pub fn new<S, R>(name: S, inlets: R, outlet: mpsc::Sender<T>) -> Self
    where
        S: Into<String>,
        R: IntoIterator<Item = mpsc::Receiver<T>>,
    {
        let inlets = Arc::new(Mutex::new(inlets.into_iter().map(|i| Arc::new(Mutex::new(i))).collect()));
        Self {
            name: name.into(),
            inlets,
            outlet,
        }
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
        let mut targets = self.initial_targets().await;
        let outlet = &self.outlet;

        while !targets.is_empty() {
            let select_targets = targets;
            tracing::info!(nr_targets=%select_targets.len(), "1.selecting from targets");

            let (inlet_value, target_idx, remaining) = futures::future::select_all(select_targets).await;
            let (inlet_idx, value) = inlet_value;
            let is_active = value.is_some();

            let handle_target_span = tracing::info_span!(
                "handle selected recv target",
                ?value,
                %is_active,
                %inlet_idx,
                %target_idx,
                nr_remaining=%remaining.len(),
            );
            let _handle_target_guard = handle_target_span.enter();

            tracing::info!("2.selected next item.");

            if let Some(item) = value {
                let send_active_span = tracing::info_span!("3.send item to outlet", ?item);
                let _send_active_guard = send_active_span.enter();

                outlet.send(item).await.expect("failed to send to outlet");
                tracing::info!("3.1.item sent to outlet.");
            }

            let mut remaining_targets = remaining;
            tracing::info!(nr_remaining=%remaining_targets.len(), %is_active, "after send");

            if is_active {
                let run_active_span = tracing::info_span!("4.replenish targets");
                let _run_active_guard = run_active_span.enter();

                if let Some(inlet) = self.inlets.lock().await.get(inlet_idx) {
                    let rep = MergeN::replenish_inlet_recv(inlet_idx, inlet.clone()).boxed();
                    remaining_targets.push(rep);
                    // remaining_targets.insert(idx, rep);
                    tracing::info!(nr_targets=%remaining_targets.len(), "4.1.targets replenished.");
                }
            }

            targets = remaining_targets;
            tracing::info!(nr_remaining=%targets.len(), %is_active, ">>>> loop bottom <<<<");
        }
        // loop {
        //     tokio::select! {
        //         Some(t) = rx_0.recv() => {
        //             tracing::info!(item=?t, "inlet_0 receiving");
        //             let _ = outlet.send(t).await.unwrap();
        //         }
        //
        //         Some(t) = rx_1.recv() => {
        //             tracing::info!(item=?t, "inlet_1 receiving");
        //             let _ = outlet.send(t).await.unwrap();
        //         }
        //
        //         else => {
        //             tracing::warn!("merge done - breaking...");
        //             break;
        //         }
        //     }
        // }
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
    #[tracing::instrument(level = "info", name = "initial targets", skip(self))]
    async fn initial_targets<'a>(&self) -> Vec<BoxFuture<'a, (usize, Option<T>)>> {
        let inlets = self.inlets.lock().await;
        let mut targets = Vec::with_capacity(inlets.len());
        for (idx, inlet) in inlets.iter().enumerate() {
            let rep = MergeN::replenish_inlet_recv(idx, inlet.clone()).boxed();
            targets.push(rep);
        }

        targets
    }

    #[tracing::instrument(level = "info", name = "replenish inlet recv", skip(inlet), fields(inlet_idx=%idx))]
    async fn replenish_inlet_recv(idx: usize, inlet: Arc<Mutex<mpsc::Receiver<T>>>) -> (usize, Option<T>) {
        let mut receiver = inlet.lock().await;
        (idx, receiver.recv().await)
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
