use std::{error::Error, fmt, sync::Arc, time::Duration};

use log::{info, trace, warn};
use redis::aio::MultiplexedConnection;
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use tokio::{sync::Semaphore, time::interval};

use crate::{gpt, word::{self, WordStatus}, BoxError};

const MIN_WAITING_FOR_SEARCH: usize = 50;

#[derive(Debug)]
pub struct InvalidClassificationErr(String);

impl fmt::Display for InvalidClassificationErr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Invalid classification '{}'", self.0)
    }
}

impl Error for InvalidClassificationErr {}

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

#[tracing::instrument(skip(pool, client, openai_key))]
async fn classify(pool: MultiplexedConnection, client: Client, openai_key: String, job: &str) -> Result<(), BoxError> {
    let input = format!("Is this a specific food, a category of foods, or neither? {}", job);
    let response = gpt::query_gpt::<Classification>(&client, response_format(), openai_key, input).await?;

    match response.classification.as_str() {
        "specific" => {
            trace!("Classified '{}' as keyword", &job);
            word::update_status(pool, job, WordStatus::WaitingForSearch).await?;
        }
        "category" => {
            trace!("Classified '{}' as category", &job);
            word::update_status(pool, job, WordStatus::WaitingForGeneration).await?
        }
        "neither" => {
            trace!("Classified '{}' as invalid", &job);
            word::update_status(pool, job, WordStatus::ClassifiedAsInvalid).await?
        }
        _ => {
            // Response is bounded so this should never happen
            return Err(Box::new(InvalidClassificationErr(response.classification)) as BoxError);
        }
    }

    Ok(())
}

pub async fn run(pool: MultiplexedConnection, openai_key: String) {
    info!("Started classifier");

    let client = Client::new();

    let semaphore = Arc::new(Semaphore::new(16));

    let mut interval = interval(Duration::from_millis(500));

    loop {
        interval.tick().await;

        let current_waiting_for_search = word::words_with_status(pool.clone(), WordStatus::WaitingForSearch).await;
        if let Err(err) = current_waiting_for_search {
            warn!("Error while getting words with status WAITING_FOR_SEARCH: {} (source: {:?})", err, err.source());
            continue;
        }

        if current_waiting_for_search.unwrap() >= MIN_WAITING_FOR_SEARCH {
            continue;
        }


        loop {
            if semaphore.available_permits() == 0 {
                break
            }

            let next_job = word::next_job(pool.clone(), WordStatus::WaitingForClassification, WordStatus::Classifying).await;
            if let Err(err) = next_job {
                warn!("Error while getting next job: {} (source: {:?})", err, err.source());
                break;
            }

            let Some(next_job) = next_job.unwrap() else {
                break;
            };
            
            let sempahore = semaphore.clone();
            let client = client.clone();
            let pool = pool.clone();
            let openai_key = openai_key.clone();

            tokio::spawn(async move {
                let _permit = sempahore.acquire().await.unwrap();
                if let Err(err) = classify(pool.clone(), client, openai_key, &next_job).await {
                    warn!("Classifier encountered error on word '{}': {} (source: {:?})", next_job, err, err.source());
                    if let Err(err) = word::update_status(pool, &next_job, WordStatus::ClassificationFailed).await {
                        warn!("Error while setting status to failed on word '{}': {} (source: {:?})", next_job, err, err.source());
                    }
                }
            });
        }
    }
}

