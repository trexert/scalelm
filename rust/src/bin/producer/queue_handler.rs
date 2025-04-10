use std::{collections::HashMap, sync::Arc};

use amqprs::{
    BasicProperties,
    callbacks::DefaultChannelCallback,
    channel::{BasicPublishArguments, Channel, QueueDeclareArguments},
    connection::Connection,
};
use tokio::sync::{Mutex, mpsc};
use tracing::debug;

pub struct QueueHandler {
    connection: Connection,
    channel: Channel,
    jobs_queue_name: String,
    response_queue_name: String,
    responses_waiting: Mutex<HashMap<String, mpsc::Sender<anyhow::Result<String>>>>,
}

impl QueueHandler {
    pub async fn make_generate_request(
        &self,
        correlation_id: &str,
        prompt: &str,
    ) -> anyhow::Result<String> {
        // Register for notifications from response handler
        {
            let (tx, rx) = mpsc::channel::<anyhow::Result<String>>(1);
            let waiting_map = self.responses_waiting.lock().await;
            waiting_map.insert(correlation_id.to_string(), tx);
        }

        let publish_properties = BasicProperties::default()
            .with_content_type("application/json")
            .with_correlation_id(correlation_id)
            .with_expiration("30000")
            .with_persistence(true)
            .with_reply_to(&self.response_queue_name)
            .with_timestamp(chrono::Utc::now().timestamp_millis() as u64)
            // .with_user_id("guest")
            .finish();
        let publish_args = BasicPublishArguments::new("amq.topic", &config.jobs_queue_name);

        self.channel
            .basic_publish(
                publish_properties.clone(),
                prompt.to_string().into_bytes(),
                publish_args.clone(),
            )
            .await?;

        debug!(correlation_id = correlation_id, "Sent request");

        Ok("".to_string())
    }
}
