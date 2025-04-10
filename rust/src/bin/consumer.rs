use std::str;

use amqprs::{
    channel::{
        BasicAckArguments, BasicConsumeArguments, BasicQosArguments, Channel, QueueBindArguments, QueueDeclareArguments
    }, connection::Connection, consumer::{AsyncConsumer, BlockingConsumer, DefaultConsumer}, BasicProperties, Deliver
};
use anyhow::bail;
use scalelm::{ConnectionConfig, setup_connection, setup_jobs_channel};
use tokio::sync::Notify;
use tracing::warn;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

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
    let connection = setup_connection(&config).await.unwrap();
    let channel = setup_jobs_channel(&connection, &config).await.unwrap();
    listen_forever(&channel, &config).await.unwrap();
}

async fn listen_forever(channel: &Channel, config: &ConnectionConfig) -> anyhow::Result<()> {
    let args = BasicConsumeArguments::new(&config.jobs_queue_name, "");

    channel.basic_qos(BasicQosArguments::default().prefetch_count(1).finish()).await?;

    channel
        .basic_consume_blocking(Consumer {}, args)
        .await
        .unwrap();

    let guard = Notify::new();
    guard.notified().await;
    bail!("Fallen out of listen loop")
}

struct Consumer {}

impl BlockingConsumer for Consumer {
    fn consume(
        &mut self, // use `&mut self` to make trait object to be `Sync`
        channel: &Channel,
        deliver: Deliver,
        basic_properties: BasicProperties,
        content: Vec<u8>,
    ) {
        warn!(
            "Received message -\n{:?}\n{:?}\n{:?}",
            deliver,
            basic_properties,
            str::from_utf8(&content)
        );
        channel
            .basic_ack_blocking(BasicAckArguments::new(deliver.delivery_tag(), false))
            .unwrap();
    }
}
