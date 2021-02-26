use rand::Rng;
use replenishing::telemetry::{get_subscriber, init_subscriber};
use replenishing::{MergeN, Stage};
use std::fmt;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

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

    let mut merge = MergeN::new("merge", merge_inlets, tx_merge);
    let m = tokio::spawn(async move {
        merge.run().await;
    });

    let h0 = spawn_transmission("ONES", 1..9, tx_0);
    let h1 = spawn_transmission("TENS", 11..99, tx_1);
    let h2 = spawn_transmission("HUNDREDS", 101..=999, tx_2);

    let mut count = 0;
    let r = tokio::spawn(async move {
        while let Some(item) = rx_merge.recv().await {
            count += 1;
            tracing::info!(%item, nr_received=%count, "Received item");
        }
    });

    h0.await.unwrap();
    h1.await.unwrap();
    h2.await.unwrap();

    m.await.unwrap();
    r.await.unwrap();
    tracing::info!("Done!");
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
            let delay = Duration::from_millis(rand::thread_rng().gen_range(25..=50));
            // tokio::time::sleep(delay).await;

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
                    panic!(err.to_string());
                }
            };
        }
    })
}
