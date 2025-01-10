use sqlx::{prelude::FromRow, query, query_as, MySql, Pool};

use crate::BoxError;

#[derive(FromRow)]
struct BlacklistWord(String);

/// Returns true if added
/// Returns false if already existed
#[tracing::instrument(skip(pool))]
#[must_use]
pub async fn add(pool: Pool<MySql>, word: &str) -> Result<bool, BoxError> {
    if exists(&pool, word).await? {
        return Ok(false);
    }

    query("INSERT INTO link_blacklist (word) VALUES (?)")
        .bind(word.to_string())
        .execute(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;
   
    Ok(true)
}

#[tracing::instrument(skip(pool))]
#[must_use]
async fn exists(pool: &Pool<MySql>, word: &str) -> Result<bool, BoxError> {
    Ok(query("SELECT id FROM link_blacklist WHERE word = ? LIMIT 1")
        .bind(word)
        .fetch_optional(pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?
        .is_some())
}

#[tracing::instrument(skip(pool))]
#[must_use]
pub async fn is_allowed(pool: &Pool<MySql>, link: &str) -> Result<bool, BoxError> {
    let blacklist = query_as::<_, BlacklistWord>("SELECT word FROM link_blacklist")
        .fetch_all(pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    for word in blacklist {
        if link.contains(&word.0) {
            return Ok(false);
        }
    }

    Ok(true)
}

