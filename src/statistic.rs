use std::time::Duration;

use log::{info, warn};
use redis::aio::MultiplexedConnection;
use sqlx::{query, MySql, Pool};
use tokio::time::interval;

use crate::{link::{self, LinkStatus}, recipe, BoxError};

#[tracing::instrument(skip(redis_links, redis_recipes, mysql))]
#[must_use]
async fn update(
    redis_links: MultiplexedConnection, 
    redis_recipes: MultiplexedConnection, 
    mysql: Pool<MySql>,
) -> Result<(), BoxError> {
    let timestamp = chrono::offset::Utc::now();

    query("INSERT INTO link_statistic (
timestamp, waiting_for_processing, processing, download_failed, extraction_failed, parsing_failed, processed, total_content_size
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
        .bind(timestamp)
        .bind(link::links_with_status(redis_links.clone(), LinkStatus::Waiting).await? as i64)
        .bind(link::links_with_status(redis_links.clone(), LinkStatus::Processing).await? as i64)
        .bind(link::links_with_status(redis_links.clone(), LinkStatus::DownloadFailed).await? as i64)
        .bind(link::links_with_status(redis_links.clone(), LinkStatus::ExtractionFailed).await? as i64)
        .bind(link::links_with_status(redis_links.clone(), LinkStatus::ParsingFailed).await? as i64)
        .bind(link::links_with_status(redis_links.clone(), LinkStatus::Processed).await? as i64)
        .bind(link::total_content_size(redis_links.clone()).await? as u64)
        .execute(&mysql)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    query("INSERT INTO recipe_statistic (
timestamp, recipe_count
) VALUES (?, ?)")
        .bind(timestamp)
        .bind(recipe::recipe_count(redis_recipes.clone()).await? as i64)
        .execute(&mysql)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

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

