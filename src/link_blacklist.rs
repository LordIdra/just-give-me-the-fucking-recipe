use redis::{aio::MultiplexedConnection, AsyncCommands};

use crate::BoxError;

fn key_blacklist() -> String {
    "blacklist".to_string()
}

/// Returns true if added
/// Returns false if already existed
#[tracing::instrument(skip(pool))]
#[must_use]
pub async fn add(mut pool: MultiplexedConnection, word: &str) -> Result<bool, BoxError> {
    if exists(pool.clone(), word).await? {
        return Ok(false);
    }

    let _: () = pool.sadd(key_blacklist(), word)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;
   
    Ok(true)
}

#[tracing::instrument(skip(pool))]
#[must_use]
async fn exists(mut pool: MultiplexedConnection, word: &str) -> Result<bool, BoxError> {
    let exists: bool = pool.sismember(key_blacklist(), word)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    Ok(exists)
}

#[tracing::instrument(skip(pool))]
#[must_use]
pub async fn is_allowed(mut pool: MultiplexedConnection, link: &str) -> Result<bool, BoxError> {
    let blacklist: Vec<String> = pool.smembers(key_blacklist())
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    for word in blacklist {
        if link.contains(&word) {
            return Ok(false);
        }
    }

    Ok(true)
}

