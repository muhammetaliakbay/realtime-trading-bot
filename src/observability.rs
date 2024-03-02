use futures::FutureExt;
use lazy_static::lazy_static;
use rand::distributions::{Alphanumeric, DistString};
use tokio::task::JoinError;
use tracing_loki::url::Url;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

lazy_static! {
    static ref INSTANCE: String = Alphanumeric.sample_string(&mut rand::thread_rng(), 16);
}

pub async fn setup_loki(
    url: String,
) -> Result<
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), JoinError>> + Send>>,
    Box<dyn std::error::Error>,
> {
    let (layer, controller, task) = tracing_loki::builder()
        .label("app", "trade-stream")?
        .label("instance", INSTANCE.to_owned())?
        .build_controller_url(Url::parse(&url)?)?;

    // We need to register our layer with `tracing`.
    tracing_subscriber::registry()
        .with(tracing_subscriber::filter::LevelFilter::INFO)
        .with(layer)
        .init();

    // The background task needs to be spawned so the logs actually get
    // delivered.
    let background_task = tokio::spawn(task);

    // Teardown
    return Ok(async move {
        controller.shutdown().await;
        background_task.await
    }
    .boxed());
}
