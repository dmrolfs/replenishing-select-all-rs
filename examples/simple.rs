use futures::future::{BoxFuture, FutureExt};
use rand::Rng;
use replenishing::telemetry::{get_subscriber, init_subscriber};
use std::fmt::Debug;
use std::time::Duration;

#[tokio::main]
async fn main() {
    let subscriber = get_subscriber("simple", "trace");
    init_subscriber(subscriber);

    let main_span = tracing::info_span!("main");
    let _main_span_guard = main_span.enter();

    let mut v: Vec<BoxFuture<()>> = Vec::with_capacity(2);

    let mut rng = rand::thread_rng();
    let mut gen_delay = || rng.gen_range(1..=10);

    // one argument to test() is i32, the second argument is &str.
    let first = |c: i32, delay: Duration| test(c, delay).boxed();
    v.push(first(0, Duration::from_secs(gen_delay())));

    let second = |c: i32, delay: Duration| test(format!("five-{}", c), delay).boxed();
    v.push(second(0, Duration::from_secs(gen_delay())));

    let mut target: Vec<BoxFuture<()>> = v;

    let mut count = 0;
    while count < 10 {
        count += 1;
        let (_value, idx, remaining) = futures::future::select_all(target).await;
        tracing::info!(%idx, remaining=remaining.len(), "selected future");

        target = remaining;
        if idx % 2 == 0 {
            let delay = Duration::from_millis(gen_delay());
            tracing::info!(%idx, ?delay, "replenishing with first with delay");
            target.push(first(count, delay));
        } else {
            let delay = Duration::from_secs(gen_delay());
            tracing::info!(%idx, ?delay, "replenishing with second with delay");
            target.push(second(count, delay));
        };
    }
}

async fn test<T: Debug>(x: T, delay: Duration) {
    tokio::time::sleep(delay).await;
    async {
        tracing::info!(?x, ?delay, "OUTPUT");
    }
    .await;
}
