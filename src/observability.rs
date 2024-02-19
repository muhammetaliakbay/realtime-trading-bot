use futures::FutureExt;
use lazy_static::lazy_static;
use rand::distributions::{Alphanumeric, DistString};
use std::error::Error;
use tokio::task::{JoinError, JoinHandle};
use tracing_loki::url::Url;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

lazy_static! {
    static ref INSTANCE: String = Alphanumeric.sample_string(&mut rand::thread_rng(), 16);
}

pub async fn setup_prometheus_push(
    url: String,
    interval: std::time::Duration,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Box<dyn Error>>>>> {
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let task: JoinHandle<Result<(), prometheus::Error>> = tokio::spawn(async move {
        let mut done = false;
        while !done {
            tokio::select! {
                _ = tokio::time::sleep(interval) => (),

                _ = rx.recv() => {
                    done = true;
                },
            }

            let metric_families = prometheus::gather();
            loop {
                let metric_families = metric_families.clone();
                let err = match tokio::task::block_in_place(|| {
                    prometheus::push_metrics(
                        "trade-stream",
                        prometheus::labels! {
                            "app".to_owned() => "trade-stream".to_owned(),
                            "instance".to_owned() => INSTANCE.to_owned(),
                        },
                        &url,
                        metric_families,
                        None,
                    )
                }) {
                    Ok(()) => break,
                    Err(err) => err,
                };

                tracing::warn!("Failed to push metrics, retrying: {}", err);

                tokio::select! {
                    _ = tokio::time::sleep(interval) => (),

                    _ = rx.recv() => {
                        done = true;
                        tracing::error!("Failed to push metrics: {}", err);
                        break;
                    },
                }
            }
        }
        Ok(())
    });

    return async move {
        tx.send(()).await?;
        task.await??;
        Ok(())
    }
    .boxed();
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
