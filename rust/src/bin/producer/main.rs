mod queue_handler;
mod router;

use std::{
    env, io,
    time::{SystemTime, UNIX_EPOCH},
};

use amqprs::{
    BasicProperties, FieldTable,
    callbacks::DefaultChannelCallback,
    channel::{BasicPublishArguments, Channel, QueueBindArguments, QueueDeclareArguments},
    connection::{Connection, OpenConnectionArguments},
};
use anyhow::{bail, ensure};
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{get, post},
};
use scalelm::{ConnectionConfig, setup_connection, setup_jobs_channel};
use serde::Deserialize;
use tokio::sync::Notify;
use tracing::{debug, info, info_span, span, trace, trace_span, warn};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;

#[tokio::main]
async fn main() {
    // construct a subscriber that prints formatted traces to stdout
    // global subscriber with log level according to RUST_LOG
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .try_init()
        .ok();

    let config = ConnectionConfig::from_env().unwrap();
    let rabbitmq_connection = setup_connection(&config).await.unwrap();
    let rabbitmq_channel = setup_jobs_channel(&rabbitmq_connection, &config)
        .await
        .unwrap();
}

async fn root() -> (StatusCode, String) {
    debug!("Received request at root");
    (StatusCode::OK, "Make request to /api/generate".to_string())
}
