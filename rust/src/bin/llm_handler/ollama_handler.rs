use std::time::Duration;

use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::warn;

const GENERATE_URL: &str = "http://localhost:11434/api/generate";
const TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Clone)]
pub struct OllamaHandler {
    http_client: Client,
}

impl OllamaHandler {
    pub fn new() -> anyhow::Result<Self> {
        let http_client = reqwest::Client::builder().timeout(TIMEOUT).build()?;
        Ok(Self { http_client })
    }

    pub async fn make_generate_request(&self, prompt: &str) -> anyhow::Result<String> {
        let body = RequestBody::new(prompt);
        let result = self
            .http_client
            .post(GENERATE_URL)
            .body(serde_json::to_string(&body)?)
            .send()
            .await?;
        let response_text = &result.text().await?;
        warn!("response text: {}", response_text);
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
    fn new(prompt: &str) -> Self {
        Self {
            model: "gemma3:1b".to_string(),
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
