use std::time::Duration;

use log::{info, warn};
use sqlx::{query, query_as, FromRow, MySql, Pool};
use tokio::time::interval;

use crate::{word::WordStatus, BoxError};

#[derive(FromRow)]
struct OneBigInt(i64);

async fn fetch_word_status(pool: Pool<MySql>, word: WordStatus) -> Result<i64, BoxError> {
    Ok(query_as::<_, OneBigInt>("SELECT COUNT(*) FROM word WHERE status = ?")
        .bind(word.to_string())
        .fetch_one(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?
        .0)
}

pub async fn fetch_table_count(pool: Pool<MySql>, table : &str) -> Result<i64, BoxError> {
    Ok(query_as::<_, OneBigInt>(&format!("SELECT COUNT(*) FROM {}", table))
        .fetch_one(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?
        .0)
}

async fn fetch_total_content_size(pool: Pool<MySql>) -> Result<i64, BoxError> {
    let mut count = query_as::<_, OneBigInt>("SELECT CAST(SUM(extraction_failed_link.content_size) AS INT) FROM extraction_failed_link")
        .fetch_one(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?
        .0;

    count += query_as::<_, OneBigInt>("SELECT CAST(SUM(processed_link.content_size) AS INT) FROM processed_link")
        .fetch_one(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?
        .0;

    Ok(count)
}

async fn fetch_count(pool: Pool<MySql>, table: &str) -> Result<i64, BoxError> {
    Ok(query_as::<_, OneBigInt>(&format!("SELECT COUNT(*) FROM {table}"))
        .fetch_one(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?
        .0)
}

#[tracing::instrument(skip(pool))]
#[must_use]
async fn update(pool: Pool<MySql>) -> Result<(), BoxError> {
    let timestamp = chrono::offset::Utc::now();

    query("INSERT INTO word_statistic (
timestamp, waiting_for_generation, generating, generation_failed, generation_complete, waiting_for_classification,
classifying, classification_failed, classified_as_invalid, waiting_for_search, searching, search_failed, search_complete
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .bind(timestamp)
        .bind(fetch_word_status(pool.clone(), WordStatus::WaitingForGeneration).await?)
        .bind(fetch_word_status(pool.clone(), WordStatus::Generating).await?)
        .bind(fetch_word_status(pool.clone(), WordStatus::GenerationFailed).await?)
        .bind(fetch_word_status(pool.clone(), WordStatus::GenerationComplete).await?)
        .bind(fetch_word_status(pool.clone(), WordStatus::WaitingForClassification).await?)
        .bind(fetch_word_status(pool.clone(), WordStatus::Classifying).await?)
        .bind(fetch_word_status(pool.clone(), WordStatus::ClassificationFailed).await?)
        .bind(fetch_word_status(pool.clone(), WordStatus::ClassifiedAsInvalid).await?)
        .bind(fetch_word_status(pool.clone(), WordStatus::WaitingForSearch).await?)
        .bind(fetch_word_status(pool.clone(), WordStatus::Searching).await?)
        .bind(fetch_word_status(pool.clone(), WordStatus::SearchFailed).await?)
        .bind(fetch_word_status(pool.clone(), WordStatus::SearchComplete).await?)
        .execute(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    query("INSERT INTO link_statistic (
timestamp, waiting_for_processing, processing, download_failed, extraction_failed, processed, total_content_size
) VALUES (?, ?, ?, ?, ?, ?, ?)")
        .bind(timestamp)
        .bind(fetch_table_count(pool.clone(), "waiting_link").await?)
        .bind(fetch_table_count(pool.clone(), "processing_link").await?)
        .bind(fetch_table_count(pool.clone(), "download_failed_link").await?)
        .bind(fetch_table_count(pool.clone(), "extraction_failed_link").await?)
        .bind(fetch_table_count(pool.clone(), "processed_link").await?)
        .bind(fetch_total_content_size(pool.clone()).await?)
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

pub async fn run(pool: Pool<MySql>) {
    info!("Started statistic updater");

    let mut interval = interval(Duration::from_secs(30));

    loop {
        interval.tick().await;

        if let Err(err) = update(pool.clone()).await {
            warn!("Error while updating statistics: {} (source: {:?})", err, err.source());
        }
    }
}

