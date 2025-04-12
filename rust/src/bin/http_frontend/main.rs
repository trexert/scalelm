mod queue_handler;
mod router;

use queue_handler::QueueHandler;
use tokio::sync::Notify;
use tracing::error;
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

    if let Err(e) = start_server().await {
        error!("Failed to start http endpoint: \n{:?}", e)
    } else {
        let guard = Notify::new();
        guard.notified().await;
        error!("Fallen server process finished unexpectedly");
    }
}

async fn start_server() -> anyhow::Result<()> {
    let queue_handler = QueueHandler::new().await?;
    queue_handler.listen_for_responses().await?;
    router::serve_routes(queue_handler).await?;
    Ok(())
}
