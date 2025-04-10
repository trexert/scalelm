use std::env;

use amqprs::{
    callbacks::{DefaultChannelCallback, DefaultConnectionCallback},
    channel::{Channel, QueueBindArguments, QueueDeclareArguments},
    connection::{Connection, OpenConnectionArguments},
};
use anyhow::ensure;

pub async fn setup_connection(config: &ConnectionConfig) -> anyhow::Result<Connection> {
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

pub async fn setup_jobs_channel(
    connection: &Connection,
    config: &ConnectionConfig,
) -> anyhow::Result<Channel> {
    let channel = connection.open_channel(None).await?;

    channel.register_callback(DefaultChannelCallback).await?;

    let (jobs_queue_name, _, _) = channel
        .queue_declare(QueueDeclareArguments::new(&config.jobs_queue_name))
        .await?
        .unwrap();

    ensure!(jobs_queue_name == config.jobs_queue_name);

    channel
        .queue_bind(QueueBindArguments::new(
            &jobs_queue_name,
            "amq.topic",
            &jobs_queue_name,
        ))
        .await
        .unwrap();

    Ok(channel)
}

#[derive(Clone)]
pub struct ConnectionConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub pass: String,
    pub jobs_queue_name: String,
}

impl ConnectionConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        let host = env::var("RABBITMQ_CONNECTION_HOST").unwrap_or("localhost".to_string());
        // .with_context(|| "Error getting connection host")?;
        let port_string = env::var("RABBITMQ_CONNECTION_PORT").unwrap_or("5672".to_string());
        // .with_context(|| "Error getting connection port")?;
        let port = port_string.parse()?;
        let user = env::var("RABBITMQ_CONNECTION_USER").unwrap_or("guest".to_string());
        // .with_context(|| "Error getting connection host")?;
        let pass = env::var("RABBITMQ_CONNECTION_PASS").unwrap_or("guest".to_string());
        // .with_context(|| "Error getting connection host")?;
        let jobs_queue_name = env::var("RABBITMQ_JOBS_QUEUE").unwrap_or("jobs_queue_1".to_string());
        // .with_context(|| "Error getting jobs queue name")?;
        Ok(Self {
            host,
            port,
            user,
            pass,
            jobs_queue_name,
        })
    }
}
