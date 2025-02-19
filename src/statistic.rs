use std::time::{Duration, SystemTime, UNIX_EPOCH};

use log::{info, warn};
use redis::{aio::MultiplexedConnection, AsyncCommands};
use tokio::time::interval;

use crate::{word::{words_with_status, WordStatus}, BoxError};

#[tracing::instrument(skip(redis_words, redis_links, redis_recipes, redis_statistics))]
#[must_use]
async fn update(
    mut redis_words: MultiplexedConnection, 
    mut redis_links: MultiplexedConnection, 
    mut redis_recipes: MultiplexedConnection, 
    mut redis_statistics: MultiplexedConnection, 
) -> Result<(), BoxError> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    for status in WordStatus::ALL_WORD_STATUSES {
        let words_with_status = words_with_status(redis_words.clone(), status)
            .await?;
        let _: () = redis_statistics.zadd("word:words_with_status", words_with_status, timestamp)
            .await
            .map_err(|err| Box::new(err) as BoxError)?;
    }

    info!("Statistics updated");

    Ok(())
}

pub async fn run(
    redis_words: MultiplexedConnection, 
    redis_links: MultiplexedConnection, 
    redis_recipes: MultiplexedConnection, 
    redis_statistics: MultiplexedConnection, 
) {
    info!("Started statistic updater");

    let mut interval = interval(Duration::from_secs(5));

    loop {
        interval.tick().await;

        if let Err(err) = update(redis_words.clone(), redis_links.clone(), redis_recipes.clone(), redis_statistics.clone()).await {
            warn!("Error while updating statistics: {} (source: {:?})", err, err.source());
        }
    }
}

