use std::env;

use amqprs::{
    channel::{Channel, QueueBindArguments, QueueDeclareArguments},
    connection::Connection,
};
use anyhow::bail;
use scalelm::setup_connection;
use tokio::sync::Notify;

#[tokio::main]
async fn main() {
    let connection = setup_connection().await.unwrap();
    let channel = setup_channel(&connection).await.unwrap();
    listen_forever(channel).await.unwrap();
}

async fn listen_forever(channel: Channel) -> anyhow::Result<()> {
    channel.basic_consume(consumer, args);

    let guard = Notify::new();
    guard.notified().await;
    bail!("Fallen out of listen loop")
}

async fn setup_channel(connection: &Connection) -> anyhow::Result<Channel> {
    let rabbitmq_jobs_queue = env::var("RABBITMQ_JOBS_QUEUE")?;
    let channel = connection.open_channel(None).await?;

    let (queue_name, _, _) = channel
        .queue_declare(QueueDeclareArguments::new(&rabbitmq_jobs_queue))
        .await?
        .unwrap();

    channel
        .queue_bind(QueueBindArguments::new(&queue_name, "", ""))
        .await?;

    Ok(channel)
}
