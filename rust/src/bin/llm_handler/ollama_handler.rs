use std::{env, time::Duration};

use anyhow::Context;
use reqwest_middleware::ClientWithMiddleware;
use reqwest_retry::{RetryTransientMiddleware, policies::ExponentialBackoff};
use serde::{Deserialize, Serialize};
use tracing::debug;

const GENERATE_PATH: &str = "/api/generate";

#[derive(Clone)]
pub struct OllamaHandler {
    client: ClientWithMiddleware,
    config: OllamaConfig,
}

impl OllamaHandler {
    pub fn new() -> anyhow::Result<Self> {
        let config = OllamaConfig::from_env()?;
        let retry_policy = ExponentialBackoff::builder().build_with_max_retries(5);
        let raw_client = reqwest::Client::builder().timeout(config.timeout).build()?;
        let client = reqwest_middleware::ClientBuilder::new(raw_client)
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build();
        Ok(Self { client, config })
    }

    pub async fn make_generate_request(&self, prompt: &str) -> anyhow::Result<String> {
        let body = RequestBody::new(&self.config.model, prompt);
        let result = self
            .client
            .post(format!("{}{}", &self.config.host, GENERATE_PATH))
            .body(serde_json::to_string(&body)?)
            .send()
            .await?;
        let response_text = &result.text().await?;
        debug!("Got response from ollama container: {}", response_text);
        let response_body: ResponseBody = serde_json::from_str(&response_text)?;
        Ok(response_body.response)
    }
}

#[derive(Serialize, Deserialize)]
struct RequestBody {
    model: String,
    prompt: String,
    stream: bool,
}

impl RequestBody {
    fn new(model: &str, prompt: &str) -> Self {
        Self {
            model: model.to_string(),
            prompt: prompt.to_string(),
            stream: false,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct ResponseBody {
    model: String,
    created_at: String,
    response: String,
    done: bool,
    context: Vec<u32>,
    total_duration: u64,
    load_duration: u64,
    prompt_eval_count: u32,
    eval_count: u32,
    prompt_eval_duration: u64,
    eval_duration: u64,
}

#[derive(Clone)]
struct OllamaConfig {
    host: String,
    model: String,
    timeout: Duration,
}

impl OllamaConfig {
    fn from_env() -> anyhow::Result<Self> {
        let host = env::var("OLLAMA_HOST").with_context(|| "Error getting ollama host")?;
        let model = env::var("OLLAMA_MODEL").with_context(|| "Error getting ollama model")?;
        let timeout_str = env::var("REQUEST_TIMEOUT").with_context(|| "Error getting timeout")?;
        let timeout_int = timeout_str.parse()?;
        let timeout = Duration::from_secs(timeout_int);

        Ok(Self {
            host,
            model,
            timeout,
        })
    }
}
