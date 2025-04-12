mod queue_handler;
mod router;

use queue_handler::QueueHandler;
use tokio::sync::Notify;
use tracing::{error, info};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() {
    // construct a subscriber that prints formatted traces to stdout
    // global subscriber with log level according to RUST_LOG
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .try_init()
        .unwrap();

    match start_server().await {
        Ok(_queue_handler) => {
            info!("Successfully started http frontend. Listening forever");
            let guard = Notify::new();
            guard.notified().await;
            error!("Http frontend process finished unexpectedly");
        }
        Err(e) => error!("Failed to start http frontend: \n{:?}", e),
    }
}

/// Starts server, retaining a reference to the queue handler so we don't drop the connection.
async fn start_server() -> anyhow::Result<QueueHandler> {
    let queue_handler = QueueHandler::new().await?;
    queue_handler.listen_for_responses().await?;
    router::serve_routes(queue_handler.clone()).await?;
    Ok(queue_handler)
}
