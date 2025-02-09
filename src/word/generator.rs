use std::{sync::Arc, time::Duration};

use log::{info, trace, warn};
use redis::aio::MultiplexedConnection;
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use tokio::{sync::Semaphore, time::interval};

use crate::{gpt, word::{self, WordStatus}, BoxError};

const MIN_WAITING_FOR_SEARCH: usize = 10;

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

#[tracing::instrument(skip(pool, client, openai_key))]
async fn generate(pool: MultiplexedConnection, client: Client, openai_key: String, job: &str) -> Result<(), BoxError> {
    let input = format!("List as many foods as you possibly can in the food category: {:?}. Just output the name of each food without extra detail.", job);

    let response = gpt::query_gpt::<Output>(&client, response_format(), openai_key, input).await?;
    for output in response.output {
        if word::add(pool.clone(), &output, Some(job), -1.0, WordStatus::WaitingForClassification).await? {
            trace!("Added generated word {}", output)
        } else {
            trace!("Rejected duplicate generated word {}", output)
        }
    }

    word::update_status(pool.clone(), job, WordStatus::GenerationComplete).await?;

    Ok(())
}

pub async fn run(pool: MultiplexedConnection, openai_key: String) {
    info!("Started generator");

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

            let next_job = word::next_job(pool.clone(), WordStatus::WaitingForGeneration, WordStatus::Generating).await;
            if let Err(err) = next_job {
                warn!("Error while getting next job: {}", err);
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
                if let Err(err) = generate(pool.clone(), client, openai_key.clone(), &next_job).await {
                    warn!("Generator encountered error on word '{}': {} (source: {:?})", next_job, err, err.source());
                    if let Err(err) = word::update_status(pool, &next_job, WordStatus::GenerationFailed).await {
                        warn!("Error while setting status to failed on word '{}': {} (source: {:?})", &next_job, err, err.source());
                    }
                }
            });
        }
    }
}

