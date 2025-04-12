use core::str;
use std::{collections::HashMap, env, sync::Arc, time::Duration};

use amqprs::{
    BasicProperties, Deliver,
    channel::{BasicConsumeArguments, BasicPublishArguments, Channel},
    connection::Connection,
    consumer::AsyncConsumer,
};
use anyhow::Context;
use async_trait::async_trait;
use scalelm::{
    ConnectionConfig, EXCHANGE, RequestMessage, setup_channel, setup_connection, setup_queue,
};
use tokio::{
    sync::{Mutex, oneshot},
    time::timeout,
};
use tracing::{debug, warn};

const MESSAGE_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Clone)]
pub struct QueueHandler {
    _connection: Connection,
    channel: Channel,
    jobs_queue_name: String,
    response_queue_name: String,
    waiting_for_responses: Arc<Mutex<HashMap<String, oneshot::Sender<Vec<u8>>>>>,
}

impl QueueHandler {
    pub async fn new() -> anyhow::Result<Self> {
        let config = ConnectionConfig::from_env()?;
        let connection = setup_connection(&config).await?;
        let channel = setup_channel(&connection).await?;

        let response_queue_name = env::var("RABBITMQ_RESPONSE_QUEUE")
            .with_context(|| "Error getting response queue name")?;
        let jobs_queue_name = config.jobs_queue_name;
        setup_queue(&channel, &jobs_queue_name, false).await?;
        setup_queue(&channel, &response_queue_name, true).await?;

        let waiting_for_responses = Arc::new(Mutex::new(HashMap::new()));

        Ok(Self {
            _connection: connection,
            channel,
            jobs_queue_name,
            response_queue_name,
            waiting_for_responses,
        })
    }

    /// Publish the request to the llm jobs queue, then wait for a response via our queue consumer.
    pub async fn make_generate_request(
        &self,
        correlation_id: &str,
        prompt: &str,
    ) -> anyhow::Result<String> {
        // Register for notifications from response handler
        let (tx, rx) = oneshot::channel::<Vec<u8>>();
        let mut locked_waiting_map = self.waiting_for_responses.lock().await;
        locked_waiting_map.insert(correlation_id.to_string(), tx);
        drop(locked_waiting_map);

        let result = self
            .try_make_generate_request(correlation_id, prompt, rx)
            .await;

        // Cleanup our registration for responses
        let mut locked_waiting_map = self.waiting_for_responses.lock().await;
        locked_waiting_map.remove(correlation_id);

        result
    }

    async fn try_make_generate_request(
        &self,
        correlation_id: &str,
        prompt: &str,
        rx: oneshot::Receiver<Vec<u8>>,
    ) -> anyhow::Result<String> {
        let publish_properties = BasicProperties::default()
            .with_content_type("application/json")
            .with_correlation_id(correlation_id)
            .with_expiration(&MESSAGE_TIMEOUT.as_millis().to_string())
            .with_persistence(true)
            .with_reply_to(&self.response_queue_name)
            .with_timestamp(chrono::Utc::now().timestamp_millis() as u64)
            .finish();
        let publish_args = BasicPublishArguments::new(EXCHANGE, &self.jobs_queue_name);

        let message_content = RequestMessage {
            prompt: prompt.to_string(),
        };

        self.channel
            .basic_publish(
                publish_properties.clone(),
                serde_json::to_string(&message_content)?.into(),
                publish_args.clone(),
            )
            .await?;

        debug!(correlation_id = correlation_id, "Sent request");

        // Allow double message timeout for time waiting in queue,
        //  and then processing time.
        let response = timeout(MESSAGE_TIMEOUT * 2, rx).await??;
        Ok(String::from_utf8(response)?)
    }

    /// Register a consumer on our response queue.
    /// Returns once consumer has been registered with rabbitmq.
    pub async fn listen_for_responses(&self) -> anyhow::Result<()> {
        let args = BasicConsumeArguments::default()
            .queue(self.response_queue_name.clone())
            .exclusive(true)
            // We won't have anything else trying to handle this message if we die, so may as well ack as soon as we receive the message.
            .auto_ack(true)
            .finish();
        let _consumer_id = self.channel.basic_consume(self.clone(), args).await?;
        Ok(())
    }
}

#[async_trait]
impl AsyncConsumer for QueueHandler {
    #[must_use]
    async fn consume(
        &mut self,
        _channel: &Channel,
        _deliver: Deliver,
        basic_properties: BasicProperties,
        content: Vec<u8>,
    ) {
        let Some(correlation_id) = basic_properties.correlation_id() else {
            warn!("Received message with no correlation id");
            return;
        };

        let mut locked_waiting_map = self.waiting_for_responses.lock().await;
        let Some(tx) = locked_waiting_map.remove(correlation_id) else {
            warn!(
                correlation_id = correlation_id,
                "Received message with correlation id not matched in map of requests waiting for responses."
            );
            return;
        };
        drop(locked_waiting_map);

        tx.send(content).unwrap_or_else(|e| {
            warn!(
                correlation_id = correlation_id,
                "Hit error sending response on internal channel: {:?}", e
            );
            return;
        });
    }
}
