use std::time::{Duration, Instant};

use log::{error, info};
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
            "name": "Classification",
            "strict": true,
            "schema": {
                "type": "object",
                "properties": {
                    "classification": {
                        "type": "string",
                        "enum": [
                            "specific",
                            "category",
                            "neither"
                        ]
                    }
                },
                "additionalProperties": false,
                "required": [
                    "classification"
                ]
            }
        }
    })
}

#[derive(Debug, Deserialize)]
struct Classification {
    classification: String,
}

async fn classify(connection: Connection, job: Word) {
    let client = Client::new();
    let input = format!("Is this a specific food, a category of foods, or neither? {}", job.word);
    let Some(response) = gpt::query_gpt::<Classification>(&client, response_format(), input).await else {
        info!("Failed to classify '{}'; status reset to WAITING_FOR_CLASSIFICATION", &job.word);
        word::set_status(connection.clone(), job.id, WordStatus::WaitingForClassification).await;
        return;
    };

    match response.classification.as_str() {
        "specific" => {
            word::set_status(connection, job.id, WordStatus::WaitingForSearch).await;
            info!("Classified '{}' as keyword", &job.word);
        }
        "category" => {
            word::set_status(connection, job.id, WordStatus::WaitingForGeneration).await;
            info!("Classified '{}' as category", &job.word);
        }
        "neither" => {
            word::set_status(connection, job.id, WordStatus::ClassifiedAsInvalid).await;
            info!("Classified '{}' as invalid", &job.word);
        }
        _ => {
            // Response is bounded so this should never happen
            word::set_status(connection, job.id, WordStatus::WaitingForSearch).await;
            error!("Failed to match classification '{}'", response.classification)
        }
    }
}

pub async fn start(connection: Connection) {
    info!("Started classification task");

    loop {
        let start = Instant::now();

        while let Some(job) = word::next_job(connection.clone(), WordStatus::WaitingForClassification, WordStatus::Classifying).await {
            let connection = connection.clone();
            tokio::spawn(async move {
                classify(connection, job).await;
            });
        }

        let elapsed = (start - Instant::now()).as_millis();
        if elapsed < POLL_INTERVAL_MILLIS {
            let sleep_duration = Duration::from_millis((POLL_INTERVAL_MILLIS - elapsed) as u64);
            tokio::time::sleep(sleep_duration).await;
        }
    }
}

