use reqwest::Client;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::Semaphore;

use crate::{BoxError, UnexpectedStatusCodeErr};

static GPT_SEMAPHORE: Semaphore = Semaphore::const_new(3);

#[derive(Debug, Deserialize)]
struct GPTResponse {
    choices: Vec<Choice>,
}

#[derive(Debug, Deserialize)]
struct Choice {
    message: MessageResponse,
}

#[derive(Debug, Deserialize)]
struct MessageResponse {
    content: String,
}

#[tracing::instrument(skip(client, response_format))]
pub async fn query_gpt<T: for<'a> Deserialize<'a>>(client: &Client, response_format: Value, api_key: String, input: String) -> Result<T, BoxError> {
    let _permit = GPT_SEMAPHORE.acquire().await.unwrap();

    let request_body = json!({
        "model": "gpt-4o-mini",
        "messages": [{
            "role": "user",
            "content": input,
        }],
        "response_format": response_format,
    });

    let response = client
        .post("https://api.openai.com/v1/chat/completions")
        .header("Authorization", format!("Bearer {}", api_key))
        .json(&request_body)
        .send()
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    if !response.status().is_success() {
        return Err(Box::new(UnexpectedStatusCodeErr(response.status())));
    }

    let response = response.json::<GPTResponse>()
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    serde_json::from_str::<T>(&response.choices[0].message.content)
        .map_err(|err| Box::new(err) as BoxError)
}
