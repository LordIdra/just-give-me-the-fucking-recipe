use std::{error::Error, fmt, sync::Arc, time::Duration};

use log::{info, warn};
use regex::RegexBuilder;
use serde_json::Value;
use sqlx::{MySql, Pool};
use tokio::{sync::Semaphore, time::interval};

use crate::{page::{self, PageStatus}, BoxError};

use super::Page;

const MIN_WAITING_FOR_PARSING: i32 = 100;

#[derive(Debug)]
pub struct PageContentNullErr;

impl fmt::Display for PageContentNullErr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Page content null; constraint violated somewhere")
    }
}

impl Error for PageContentNullErr {}

#[tracing::instrument(skip(pool, page), fields(id = page.id))]
async fn extract(pool: Pool<MySql>, page: Page) -> Result<(), BoxError> {
    let script_regex = RegexBuilder::new(r"<script[^>]*>(.*?)<\/script>")
        .dot_matches_new_line(true)
        .build()
        .unwrap();

    let schema_regex = RegexBuilder::new(r#"\{.{0,1000}?(schema|("@type": "Recipe")).{0,1000}?@type.*\}"#)
        .dot_matches_new_line(true)
        .build()
        .unwrap();

    let Some(content) = page.content else {
        return Err(Box::new(PageContentNullErr))
    };

    let mut schema = None;

    for script in script_regex.captures_iter(&content) {
        let script = script.get(1).unwrap().into();

        let Some(other_schema) = schema_regex.captures(script) else {
            continue;
        };

        let other_schema = other_schema.get(0).unwrap();
        let other_schema: Result<Value, _> = serde_json::from_str(other_schema.as_str());
        if let Err(err) = other_schema {
            info!("Failed to deserialize extracted schema for {} (page {}): {}", page.link, page.id, err);
            page::set_status(pool.clone(), page.id, PageStatus::ExtractionFailed).await?;
            page::set_content(pool, page.id, None).await?;
            return Ok(());
        }

        schema = Some(other_schema.unwrap());

        break;
    }

    let Some(mut schema) = schema else {
        info!("Failed to extract schema from {} (page {})", page.link, page.id);
        page::set_status(pool.clone(), page.id, PageStatus::ExtractionFailed).await?;
        page::set_content(pool, page.id, None).await?;
        return Ok(());
    };

    if let Some(graph) = schema.get("@graph") {
        if let Some(arr) = graph.as_array() {
            let new_schema = arr.iter().find(|v| v.get("@type")
                .and_then(|v| v.as_str())
                .is_some_and(|v| v == "Recipe")
            );

            let Some(new_schema) = new_schema else {
                info!("Failed to find schema in array for {} (page {})", page.link, page.id);
                page::set_status(pool.clone(), page.id, PageStatus::ExtractionFailed).await?;
                page::set_content(pool, page.id, None).await?;
                return Ok(());
            };

            schema = new_schema.clone();
        }
    }

    let schema = serde_json::to_string(&schema)
        .map_err(|err| Box::new(err) as BoxError)?;

    page::set_schema(pool.clone(), page.id, Some(&schema)).await?;
    page::set_status(pool, page.id, PageStatus::WaitingForParsing).await?;

    info!("Extracted schema from {} (page {})", page.link, page.id);

    Ok(())
}

pub async fn run(pool: Pool<MySql>) {
    info!("Started extractor");

    let semaphore = Arc::new(Semaphore::new(256));

    let mut interval = interval(Duration::from_millis(500));

    loop {
        interval.tick().await;

        let current_waiting_for_parsing = page::pages_with_status(pool.clone(), PageStatus::WaitingForParsing).await;
        if let Err(err) = current_waiting_for_parsing {
            warn!("Error while getting words with status WAITING_FOR_PARSING: {} (source: {:?})", err, err.source());
            continue;
        }

        if current_waiting_for_parsing.unwrap() >= MIN_WAITING_FOR_PARSING {
            continue;
        }

        loop {
            if semaphore.available_permits() == 0 {
                break
            }

            let next_job = page::next_job(pool.clone(), PageStatus::WaitingForExtraction, PageStatus::Extracting).await;
            if let Err(err) = next_job {
                warn!("Error while getting next job: {} (source: {:?})", err, err.source());
                break;
            }

            let Some(state) = next_job.unwrap() else {
                break;
            };
            
            let sempahore = semaphore.clone();
            let pool = pool.clone();

            tokio::spawn(async move {
                let _permit = sempahore.acquire().await.unwrap();
                if let Err(err) = extract(pool.clone(), state.clone()).await {
                    warn!("Extractor encountered error on page #{} ('{}'): {} (source: {:?})", state.id, state.link, err, err.source());
                    if let Err(err) = page::set_status(pool.clone(), state.id, PageStatus::ExtractionFailed).await {
                        warn!("Error while setting status to failed on page #{} ('{}')@ {} (source: {:?})", state.id, state.link, err, err.source());
                    }
                    if let Err(err) = page::set_content(pool, state.id, None).await {
                        warn!("Error while deleting content on page #{} ('{}')@ {} (source: {:?})", state.id, state.link, err, err.source());
                    }
                }
            });
        }
    }
}

