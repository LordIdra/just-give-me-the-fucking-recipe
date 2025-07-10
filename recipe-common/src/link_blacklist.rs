use anyhow::Error;
use redis::{aio::MultiplexedConnection, AsyncCommands};

fn key_blacklist() -> String {
    "blacklist".to_string()
}

/// Returns true if added
/// Returns false if already existed
#[tracing::instrument(skip(pool))]
pub async fn add(mut pool: MultiplexedConnection, word: &str) -> Result<bool, Error> {
    if exists(pool.clone(), word).await? {
        return Ok(false);
    }

    let _: () = pool.sadd(key_blacklist(), word).await?;
   
    Ok(true)
}

#[tracing::instrument(skip(pool))]
async fn exists(mut pool: MultiplexedConnection, word: &str) -> Result<bool, Error> {
    Ok(pool.sismember(key_blacklist(), word).await?)
}

#[tracing::instrument(skip(pool))]
pub async fn is_allowed(mut pool: MultiplexedConnection, link: &str) -> Result<bool, Error> {
    let blacklist: Vec<String> = pool.smembers(key_blacklist()).await?;

    for word in blacklist {
        if link.contains(&word) {
            return Ok(false);
        }
    }

    Ok(true)
}

