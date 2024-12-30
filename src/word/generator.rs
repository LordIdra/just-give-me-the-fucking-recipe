use std::time::{Duration, Instant};

use log::info;
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use tokio_rusqlite::Connection;

use crate::{gpt, word::{self, WordStatus}};

use super::Word;

const POLL_INTERVAL_MILLIS: u128 = 100;

fn response_format() -> serde_json::Value {
    json!({
        "type": "json_schema",
        "json_schema": {
            "name": "Generator",
            "strict": true,
            "schema": {
                "type": "object",
                "properties": {
                    "output": {
                        "type": "array",
                        "items": {
                            "type": "string"
                        }
                    }
                },
                "additionalProperties": false,
                "required": [
                    "output"
                ]
            }
        }
    })
}

#[derive(Debug, Deserialize)]
struct Output {
    output: Vec<String>,
}

async fn generate(connection: Connection, job: Word) {
    let client = Client::new();
    let input = format!("List as many foods as you possibly can in the food category: {:?}. Just output the name of each food without extra detail.", job.word);
    let response = gpt::query_gpt::<Output>(&client, response_format(), input).await;

    let Some(response) = response else {
        info!("Failed to generate on category '{}'; status reset to WAITING_FOR_GENERATION", &job.word);
        word::set_status(connection.clone(), job.id, WordStatus::WaitingForGeneration).await;
        return;
    };

    info!("Generated from {}: {:?}", job.word, response.output);
    for output in response.output {
        if word::add(connection.clone(), &output, Some(job.id), -1, WordStatus::WaitingForClassification).await {
            info!("Added generated word {}", output)
        } else {
            info!("Rejected duplicate generated word {}", output)
        }
    }
}

pub async fn start(connection: Connection) {
    info!("Started generation task");

    loop {
        let start = Instant::now();

        while let Some(job) = word::next_job(connection.clone(), WordStatus::WaitingForGeneration, WordStatus::Generating).await {
            let connection = connection.clone();
            tokio::spawn(async move {
                generate(connection, job).await;
            });
        }

        let elapsed = (start - Instant::now()).as_millis();
        if elapsed < POLL_INTERVAL_MILLIS {
            let sleep_duration = Duration::from_millis((POLL_INTERVAL_MILLIS - elapsed) as u64);
            tokio::time::sleep(sleep_duration).await;
        }
    }
}

