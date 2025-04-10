use std::sync::Arc;

use amqprs::{channel::Channel, connection::Connection};
use axum::{Json, Router, extract::State, http::StatusCode, routing::post};
use scalelm::ConnectionConfig;
use serde::Deserialize;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::queue_handler::{self, QueueHandler};

async fn serve_routes() {
    let app = Router::new()
        // .route("/", get(root))
        .route("/api/generate", post(generate))
        .with_state(ServerState {
            queue_handler: QueueHandler {},
        });

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn generate(
    State(ServerState { queue_handler }): State<ServerState>,
    Json(args): Json<GenerateArgs>,
) -> (StatusCode, String) {
    let correlation_id = Uuid::new_v4().to_string();
    info!(correlation_id = correlation_id, "Received generate request");
    match queue_handler
        .make_generate_request(&correlation_id, &args.prompt)
        .await
    {
        Ok(response) => {
            debug!(
                correlation_id = correlation_id,
                "Returning OK response {}", response
            );
            (StatusCode::OK, response)
        }
        Err(e) => {
            warn!(
                correlation_id = correlation_id,
                "Returning error response caused by {:?}", e
            );
            (
                StatusCode::BAD_GATEWAY,
                "Error generating response".to_string(),
            )
        }
    }
}

#[derive(Deserialize)]
struct GenerateArgs {
    prompt: String,
}

#[derive(Clone)]
struct ServerState {
    queue_handler: Arc<QueueHandler>,
}
