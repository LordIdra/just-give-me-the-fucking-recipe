use log::warn;
use reqwest::Client;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::Semaphore;

const API_KEY: &str = "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";

static GPT_SEMAPHORE: Semaphore = Semaphore::const_new(4);

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

pub async fn query_gpt<T: for<'a> Deserialize<'a>>(client: &Client, response_format: Value, input: String) -> Option<T> {
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
        .header("Authorization", format!("Bearer {}", API_KEY))
        .json(&request_body)
        .send()
        .await;

    if let Err(err) = response {
        warn!("Error while making request to OpenAI: {:?}", err);
        return None;
    }
    let response1 = response.unwrap();
    if !response1.status().is_success() {
        warn!("Request unsuccessful with status code {:?}", response1.status());
        return None;
    }

    let response2 = response1.json::<GPTResponse>().await;
    if let Err(err) = response2 {
        warn!("Error while deserializing response from OpenAI: {:?}", err);
        return None;
    }
    let response2 = response2.unwrap();

    let response3 = serde_json::from_str::<T>(&response2.choices[0].message.content);
    if let Err(err) = response3 {
        warn!("Error while deserializing response from OpenAI: {:?}", err);
        return None;
    }

    Some(response3.unwrap())
}
