use std::time::Duration;

use log::{info, warn};
use redis::aio::MultiplexedConnection;
use sqlx::{query, query_as, FromRow, MySql, Pool};
use tokio::time::interval;

use crate::{link::{self, LinkStatus}, word::{self, WordStatus}, BoxError};

#[derive(FromRow)]
struct OneBigInt(i64);

async fn fetch_count(pool: Pool<MySql>, table: &str) -> Result<i64, BoxError> {
    Ok(query_as::<_, OneBigInt>(&format!("SELECT COUNT(*) FROM {table}"))
        .fetch_one(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?
        .0)
}

#[tracing::instrument(skip(redis_pool, pool))]
#[must_use]
async fn update(redis_pool: MultiplexedConnection, pool: Pool<MySql>) -> Result<(), BoxError> {
    let timestamp = chrono::offset::Utc::now();

    query("INSERT INTO word_statistic (
timestamp, waiting_for_generation, generating, generation_failed, generation_complete, waiting_for_classification,
classifying, classification_failed, classified_as_invalid, waiting_for_search, searching, search_failed, search_complete
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .bind(timestamp)
        .bind(word::words_with_status(redis_pool.clone(), WordStatus::WaitingForGeneration).await? as i32)
        .bind(word::words_with_status(redis_pool.clone(), WordStatus::Generating).await? as i32)
        .bind(word::words_with_status(redis_pool.clone(), WordStatus::GenerationFailed).await? as i32)
        .bind(word::words_with_status(redis_pool.clone(), WordStatus::GenerationComplete).await? as i32)
        .bind(word::words_with_status(redis_pool.clone(), WordStatus::WaitingForClassification).await? as i32)
        .bind(word::words_with_status(redis_pool.clone(), WordStatus::Classifying).await? as i32)
        .bind(word::words_with_status(redis_pool.clone(), WordStatus::ClassificationFailed).await? as i32)
        .bind(word::words_with_status(redis_pool.clone(), WordStatus::ClassifiedAsInvalid).await? as i32)
        .bind(word::words_with_status(redis_pool.clone(), WordStatus::WaitingForSearch).await? as i32)
        .bind(word::words_with_status(redis_pool.clone(), WordStatus::Searching).await? as i32)
        .bind(word::words_with_status(redis_pool.clone(), WordStatus::SearchFailed).await? as i32)
        .bind(word::words_with_status(redis_pool.clone(), WordStatus::SearchComplete).await? as i32)
        .execute(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    query("INSERT INTO link_statistic (
timestamp, waiting_for_processing, processing, download_failed, extraction_failed, processed, total_content_size
) VALUES (?, ?, ?, ?, ?, ?, ?)")
        .bind(timestamp)
        .bind(link::links_with_status(redis_pool.clone(), LinkStatus::Waiting).await? as i32)
        .bind(link::links_with_status(redis_pool.clone(), LinkStatus::Processing).await? as i32)
        .bind(link::links_with_status(redis_pool.clone(), LinkStatus::DownloadFailed).await? as i32)
        .bind(link::links_with_status(redis_pool.clone(), LinkStatus::ExtractionFailed).await? as i32)
        .bind(link::links_with_status(redis_pool.clone(), LinkStatus::Processed).await? as i32)
        .bind(link::total_content_size(redis_pool.clone()).await? as u64)
        .execute(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    query("INSERT INTO recipe_statistic (
timestamp, recipe_count
) VALUES (?, ?)")
        .bind(timestamp)
        .bind(fetch_count(pool.clone(), "recipe").await?)
        .execute(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    info!("Statistics updated");

    Ok(())
}

pub async fn run(redis_pool: MultiplexedConnection, pool: Pool<MySql>) {
    info!("Started statistic updater");

    let mut interval = interval(Duration::from_secs(30));

    loop {
        interval.tick().await;

        if let Err(err) = update(redis_pool.clone(), pool.clone()).await {
            warn!("Error while updating statistics: {} (source: {:?})", err, err.source());
        }
    }
}

