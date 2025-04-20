use std::time::Duration;

use anyhow::Error;
use log::{info, warn};
use recipe_common::{link::{links_with_status, total_content_size, LinkStatus}, rawcipe::rawcipe_count};
use redis::aio::MultiplexedConnection;
use sqlx::{query, MySql, Pool};
use tokio::time::interval;

#[tracing::instrument(skip(redis_links, redis_recipes, mysql))]
#[must_use]
async fn update(
    redis_links: MultiplexedConnection, 
    redis_recipes: MultiplexedConnection, 
    mysql: Pool<MySql>,
) -> Result<(), Error> {
    let timestamp = chrono::offset::Utc::now();

    query("INSERT INTO link_statistic (
timestamp, waiting_for_processing, processing, download_failed, extraction_failed, parsing_failed, processed, total_content_size
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
        .bind(timestamp)
        .bind(links_with_status(redis_links.clone(), LinkStatus::Waiting).await? as i64)
        .bind(links_with_status(redis_links.clone(), LinkStatus::Processing).await? as i64)
        .bind(links_with_status(redis_links.clone(), LinkStatus::DownloadFailed).await? as i64)
        .bind(links_with_status(redis_links.clone(), LinkStatus::ExtractionFailed).await? as i64)
        .bind(links_with_status(redis_links.clone(), LinkStatus::ParsingFailed).await? as i64)
        .bind(links_with_status(redis_links.clone(), LinkStatus::Processed).await? as i64)
        .bind(total_content_size(redis_links.clone()).await? as u64)
        .execute(&mysql)
        .await?;

    query("INSERT INTO recipe_statistic (
timestamp, recipe_count
) VALUES (?, ?)")
        .bind(timestamp)
        .bind(rawcipe_count(redis_recipes.clone()).await? as i64)
        .execute(&mysql)
        .await?;

    info!("Statistics updated");

    Ok(())
}

pub async fn run(
    redis_links: MultiplexedConnection, 
    redis_recipes: MultiplexedConnection, 
    mysql: Pool<MySql>
) {
    info!("Started statistic updater");

    let mut interval = interval(Duration::from_secs(30));

    loop {
        interval.tick().await;

        if let Err(err) = update(redis_links.clone(), redis_recipes.clone(), mysql.clone()).await {
            warn!("Error while updating statistics: {} (source: {:?})", err, err.source());
        }
    }
}

