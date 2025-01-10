use std::{error::Error, fmt, sync::Arc, time::Duration};

use log::{info, warn};
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use sqlx::{MySql, Pool};
use tokio::{sync::Semaphore, time::interval};

use crate::{gpt, word::{self, WordStatus}, BoxError};

use super::Word;

const MIN_WAITING_FOR_SEARCH: i32 = 20;

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
async fn classify(pool: Pool<MySql>, client: Client, openai_key: String, job: Word) -> Result<(), BoxError> {
    let input = format!("Is this a specific food, a category of foods, or neither? {}", job.word);
    let response = gpt::query_gpt::<Classification>(&client, response_format(), openai_key, input).await?;

    match response.classification.as_str() {
        "specific" => {
            info!("Classified '{}' as keyword", &job.word);
            word::set_status(pool, job.id, WordStatus::WaitingForSearch).await?;
        }
        "category" => {
            info!("Classified '{}' as category", &job.word);
            word::set_status(pool, job.id, WordStatus::WaitingForGeneration).await?
        }
        "neither" => {
            info!("Classified '{}' as invalid", &job.word);
            word::set_status(pool, job.id, WordStatus::ClassifiedAsInvalid).await?
        }
        _ => {
            // Response is bounded so this should never happen
            return Err(Box::new(InvalidClassificationErr(response.classification)) as BoxError);
        }
    }

    Ok(())
}

pub async fn run(pool: Pool<MySql>, openai_key: String) {
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

            let Some(state) = next_job.unwrap() else {
                break;
            };
            
            let sempahore = semaphore.clone();
            let client = client.clone();
            let pool = pool.clone();
            let openai_key = openai_key.clone();

            tokio::spawn(async move {
                let _permit = sempahore.acquire().await.unwrap();
                if let Err(err) = classify(pool.clone(), client, openai_key, state.clone()).await {
                    warn!("Classifier encountered error on word #{} ('{}'): {} (source: {:?})", state.id, state.word, err, err.source());
                    if let Err(err) = word::set_status(pool, state.id, WordStatus::ClassificationFailed).await {
                        warn!("Error while setting status to failed on word #{} ('{}')@ {} (source: {:?})", state.id, state.word, err, err.source());
                    }
                }
            });
        }
    }
}

