mod ollama_handler;

use std::str;

use amqprs::{
    BasicProperties, Deliver,
    channel::{
        BasicAckArguments, BasicConsumeArguments, BasicPublishArguments, BasicQosArguments, Channel,
    },
    connection::Connection,
    consumer::AsyncConsumer,
};
use anyhow::anyhow;
use async_trait::async_trait;
use ollama_handler::OllamaHandler;
use scalelm::{
    ConnectionConfig, EXCHANGE, RequestMessage, ResponseMessage, setup_channel, setup_connection,
    setup_queue,
};
use tokio::sync::Notify;
use tracing::{debug, error, info, trace, warn};
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

    match setup_listener().await {
        Ok(_queue_handler) => {
            info!("Successfully started llm handler. Listening forever");
            let guard = Notify::new();
            guard.notified().await;
            error!("Llm handler process finished unexpectedly");
        }
        Err(e) => error!("Failed to start llm handler: \n{:?}", e),
    }
}

/// Start listening for jobs on the queue. Returns a reference to the queue handler so we don't drop the connection.
async fn setup_listener() -> anyhow::Result<QueueHandler> {
    trace!("setup_listener");
    let config = ConnectionConfig::from_env()?;
    let connection = setup_connection(&config).await?;
    let channel = setup_channel(&connection).await?;
    setup_queue(&channel, &config.jobs_queue_name, false).await?;
    channel
        .basic_qos(BasicQosArguments::default().prefetch_count(1).finish())
        .await?;

    let consume_args = BasicConsumeArguments::default()
        .queue(config.jobs_queue_name.clone())
        .finish();
    let ollama_handler = OllamaHandler::new()?;
    let queue_handler = QueueHandler {
        _connection: connection.clone(),
        _channel: channel.clone(),
        ollama_handler,
    };
    channel
        .basic_consume(queue_handler.clone(), consume_args)
        .await?;

    Ok(queue_handler)
}

#[derive(Clone)]
struct QueueHandler {
    _connection: Connection,
    _channel: Channel,
    ollama_handler: OllamaHandler,
}

impl QueueHandler {
    async fn handle_request(
        &self,
        correlation_id: &str,
        channel: &Channel,
        deliver: &Deliver,
        basic_properties: &BasicProperties,
        content: &[u8],
    ) -> anyhow::Result<()> {
        trace!("handle_request");
        let content_string = str::from_utf8(content)?;
        info!(
            correlation_id = correlation_id,
            "Received content {}", content_string
        );
        let request_message: RequestMessage = serde_json::from_str(content_string)?;
        let reply_to = basic_properties
            .reply_to()
            .ok_or(anyhow!("Missing 'reply_to' in message"))?;

        let response = self
            .ollama_handler
            .make_generate_request(&request_message.prompt)
            .await?;

        let publish_properties = BasicProperties::default()
            .with_content_type("application/json")
            .with_correlation_id(correlation_id)
            .with_timestamp(chrono::Utc::now().timestamp_millis() as u64)
            .finish();
        let message_content = ResponseMessage { response };
        channel
            .basic_publish(
                publish_properties,
                serde_json::to_string(&message_content)?.into(),
                BasicPublishArguments::new(EXCHANGE, reply_to),
            )
            .await?;

        Ok(())
    }
}

#[async_trait]
impl AsyncConsumer for QueueHandler {
    async fn consume(
        &mut self,
        channel: &Channel,
        deliver: Deliver,
        basic_properties: BasicProperties,
        content: Vec<u8>,
    ) {
        trace!("consume");
        debug!(
            "Deliver: {:?}, Properties: {:?}, content: {:?}",
            deliver, basic_properties, content
        );
        let Some(correlation_id) = basic_properties.correlation_id() else {
            warn!("Received message with no correlation id");
            return;
        };
        info!(correlation_id = correlation_id, "Received job message");

        if let Err(e) = self
            .handle_request(
                correlation_id,
                channel,
                &deliver,
                &basic_properties,
                &content,
            )
            .await
        {
            warn!(
                correlation_id = correlation_id,
                "Error handling llm request: \n{:?}", e
            );
        } else {
            info!(
                correlation_id = correlation_id,
                "Successfully handled llm request"
            );
        }

        channel
            .basic_ack(BasicAckArguments::new(deliver.delivery_tag(), false))
            .await
            .unwrap_or_else(|e| {
                warn!(
                    correlation_id = correlation_id,
                    "Hit error acking that we've handled message {:?}", e
                )
            });
    }
}
