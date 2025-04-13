use std::env;

use amqprs::{
    callbacks::{DefaultChannelCallback, DefaultConnectionCallback},
    channel::{Channel, QueueBindArguments, QueueDeclareArguments},
    connection::{Connection, OpenConnectionArguments},
};
use anyhow::{Context, ensure};
use serde::{Deserialize, Serialize};
use tracing::trace;

pub const EXCHANGE: &str = "amq.topic";

/// Setup an initial connection to the RabbitMQ server.
pub async fn setup_connection(config: &ConnectionConfig) -> anyhow::Result<Connection> {
    trace!("setup_connection");
    let connection = Connection::open(&OpenConnectionArguments::new(
        &config.host,
        config.port,
        &config.user,
        &config.pass,
    ))
    .await?;

    connection
        .register_callback(DefaultConnectionCallback)
        .await?;

    Ok(connection)
}

/// Open a channel to the RabbitMQ server.
pub async fn setup_channel(connection: &Connection) -> anyhow::Result<Channel> {
    trace!("setup_channel");
    let channel = connection.open_channel(None).await?;

    channel.register_callback(DefaultChannelCallback).await?;

    Ok(channel)
}

/// Create a queue if it doesn't exist, then bind to it.
pub async fn setup_queue(
    channel: &Channel,
    queue_name: &str,
    exclusive: bool,
) -> anyhow::Result<()> {
    trace!("setup_queue");
    let (created_queue_name, _, _) = channel
        .queue_declare(
            QueueDeclareArguments::new(queue_name)
                .exclusive(exclusive)
                .finish(),
        )
        .await?
        .unwrap();

    ensure!(
        created_queue_name == queue_name,
        "Created queue must have the configured name. Configured - {}, Created - {}",
        queue_name,
        created_queue_name,
    );

    channel
        .queue_bind(QueueBindArguments::new(&queue_name, EXCHANGE, &queue_name))
        .await?;

    Ok(())
}

#[derive(Clone, Debug)]
pub struct ConnectionConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub pass: String,
    pub jobs_queue_name: String,
}

impl ConnectionConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        let host = env::var("RABBITMQ_CONNECTION_HOST")
            .with_context(|| "Error getting connection host")?;
        let port_string = env::var("RABBITMQ_CONNECTION_PORT")
            .with_context(|| "Error getting connection port")?;
        let port = port_string.parse()?;
        let user = env::var("RABBITMQ_CONNECTION_USER")
            .with_context(|| "Error getting connection host")?;
        let pass = env::var("RABBITMQ_CONNECTION_PASS")
            .with_context(|| "Error getting connection host")?;
        let jobs_queue_name =
            env::var("RABBITMQ_JOBS_QUEUE").with_context(|| "Error getting jobs queue name")?;
        Ok(Self {
            host,
            port,
            user,
            pass,
            jobs_queue_name,
        })
    }
}

#[derive(Serialize, Deserialize)]
pub struct RequestMessage {
    pub prompt: String,
}

#[derive(Serialize, Deserialize)]
pub struct ResponseMessage {
    pub response: String,
}
