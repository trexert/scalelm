use axum::{Json, Router, extract::State, http::StatusCode, routing::post};
use serde::{Deserialize, Serialize};
use tokio::time::Instant;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::queue_handler::QueueHandler;

pub async fn serve_routes(queue_handler: QueueHandler) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/api/generate", post(generate))
        .with_state(ServerState { queue_handler });

    let listener = tokio::net::TcpListener::bind("0.0.0.0:43196").await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn generate(
    State(ServerState { queue_handler }): State<ServerState>,
    Json(args): Json<GenerateArgs>,
) -> (StatusCode, Json<GenerateResponse>) {
    let correlation_id = Uuid::new_v4().to_string();
    info!(correlation_id = correlation_id, "Received generate request");
    let start_time = Instant::now();
    match queue_handler
        .make_generate_request(&correlation_id, &args.prompt)
        .await
    {
        Ok(response_text) => {
            info!(correlation_id = correlation_id, "Returning OK response");
            debug!(
                correlation_id = correlation_id,
                "Response: {}", response_text
            );
            let response = GenerateResponse::Success {
                response: response_text,
                duration: start_time.elapsed().as_nanos(),
            };
            (StatusCode::OK, Json(response))
        }
        Err(e) => {
            warn!(
                correlation_id = correlation_id,
                "Returning error response caused by:\n{:?}", e
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(GenerateResponse::Error(
                    "Error generating llm response\n".to_string(),
                )),
            )
        }
    }
}

#[derive(Deserialize)]
struct GenerateArgs {
    prompt: String,
}

#[derive(Serialize)]
enum GenerateResponse {
    Success { response: String, duration: u128 },
    Error(String),
}

#[derive(Clone)]
struct ServerState {
    queue_handler: QueueHandler,
}
