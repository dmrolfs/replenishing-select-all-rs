use rand::Rng;
use replenishing::telemetry::{get_subscriber, init_subscriber};
use replenishing::{MergeN, MergeMsg, Stage};
use std::fmt;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use std:: sync:: Arc;
use tokio::sync::Mutex;

#[tokio::main]
async fn main() {
    let subscriber = get_subscriber("simple", "trace");
    init_subscriber(subscriber);

    let main_span = tracing::info_span!("main");
    let _main_span_guard = main_span.enter();

    let (tx_0, rx_0) = mpsc::channel(8);
    let (tx_1, rx_1) = mpsc::channel(8);
    let (tx_2, rx_2) = mpsc::channel(8);
    let merge_inlets = vec![rx_0, rx_1, rx_2];

    let (tx_merge, mut rx_merge) = mpsc::channel(8);

    let (mut merge, tx_merge_api) = MergeN::new("merge", merge_inlets, tx_merge);
    let m = tokio::spawn(async move {
        merge.run().await;
    });

    let stop = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(1000)).await;
        let (tx, rx) = oneshot::channel();
        tx_merge_api.send(MergeMsg::Stop { tx }).expect("failed to send stop to merge");
        let _ = rx.await;
        tracing::warn!("STOPPED MERGE");
    });

    let h0 = spawn_transmission("ONES", 1..9, tx_0);
    let h1 = spawn_transmission("TENS", 11..99, tx_1);
    let h2 = spawn_transmission("HUNDREDS", 101..=999, tx_2);

    let count = Arc::new(Mutex::new(0));
    let r_count = count.clone();
    let r = tokio::spawn(async move {
        while let Some(item) = rx_merge.recv().await {
            let mut tally = r_count.lock().await;
            *tally += 1;
            tracing::info!(%item, nr_received=%tally, "Received item");
        }
    });

    h0.await.unwrap();
    h1.await.unwrap();
    h2.await.unwrap();

    m.await.unwrap();
    r.await.unwrap();
    stop.await.unwrap();

    let tally: i32 = *count.lock().await;
    tracing::info!(%tally, "Done!");
    assert!(15 < tally);
    assert!(tally < 27);
}

fn spawn_transmission<S, I, T>(name: S, data: I, tx: mpsc::Sender<T>) -> JoinHandle<()>
where
    T: fmt::Debug + Send + 'static,
    S: AsRef<str> + Send + 'static,
    I: IntoIterator<Item = T> + Send + 'static,
    <I as IntoIterator>::IntoIter: Send,
{
    tokio::spawn(async move {
        for item in data.into_iter() {
            let delay = Duration::from_millis(rand::thread_rng().gen_range(25..=250));
            tokio::time::sleep(delay).await;

            let send_span = tracing::info_span!(
                "introducing item to channel",
                ?item,
                ?delay,
                channel=%name.as_ref()
            );
            let _send_span_guard = send_span.enter();

            match tx.send(item).await {
                Ok(_) => tracing::info!("successfully introduced item to channel."),
                Err(err) => {
                    tracing::error!(error=?err, "failed to introduce item to channel.");
                    break;
                }
            };
        }
    })
}