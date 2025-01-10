use serde::{Deserialize, Serialize};
use sqlx::{Row, mysql::MySqlRow, prelude::FromRow, query, query_as, MySql, Pool};

use crate::BoxError;

pub mod classifier;
pub mod generator;
pub mod searcher;

#[derive(FromRow)]
struct CountRow(i32);

#[derive(Debug, Deserialize, Serialize, Clone, Copy)]
pub enum WordStatus {
    WaitingForGeneration,
    Generating,
    GenerationFailed,
    GenerationComplete,
    WaitingForClassification,
    Classifying,
    ClassificationFailed,
    ClassifiedAsInvalid,
    WaitingForSearch,
    Searching,
    SearchFailed,
    SearchComplete,
}

impl WordStatus {
    pub fn from_string(x: &str) -> Option<Self> {
        match x {
            "WAITING_FOR_GENERATION" => Some(WordStatus::WaitingForGeneration),
            "GENERATING" => Some(WordStatus::Generating),
            "GENERATION_FAILED" => Some(WordStatus::GenerationFailed),
            "GENERATION_COMPLETE" => Some(WordStatus::GenerationComplete),
            "WAITING_FOR_CLASSIFICATION" => Some(WordStatus::WaitingForClassification),
            "CLASSIFYING" => Some(WordStatus::Classifying),
            "CLASSIFICATION_FAILED" => Some(WordStatus::ClassificationFailed),
            "CLASSIFIED_AS_INVALID" => Some(WordStatus::ClassifiedAsInvalid),
            "WAITING_FOR_SEARCH" => Some(WordStatus::WaitingForSearch),
            "SEARCHING" => Some(WordStatus::Searching),
            "SEARCH_FAILED" => Some(WordStatus::SearchFailed),
            "SEARCH_COMPLETE" => Some(WordStatus::SearchComplete),
            _ => None,
        }
    }
    
    pub fn to_string(self) -> &'static str {
        match self {
            WordStatus::WaitingForGeneration => "WAITING_FOR_GENERATION",
            WordStatus::Generating => "GENERATING",
            WordStatus::GenerationFailed => "GENERATION_FAILED",
            WordStatus::GenerationComplete => "GENERATION_COMPLETE",
            WordStatus::WaitingForClassification => "WAITING_FOR_CLASSIFICATION",
            WordStatus::Classifying => "CLASSIFYING",
            WordStatus::ClassificationFailed => "CLASSIFICATION_FAILED",
            WordStatus::ClassifiedAsInvalid => "CLASSIFIED_AS_INVALID",
            WordStatus::WaitingForSearch => "WAITING_FOR_SEARCH",
            WordStatus::Searching => "SEARCHING",
            WordStatus::SearchFailed => "SEARCH_FAILED",
            WordStatus::SearchComplete => "SEARCH_COMPLETE",
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Word {
    pub id: i32,
    pub word: String,
    pub parent: Option<i32>,
    pub priority: i32,
    pub status: WordStatus,
}

impl FromRow<'_, MySqlRow> for Word {
    #[tracing::instrument(skip(row))]
    fn from_row(row: &'_ MySqlRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            id: row.try_get("id")?,
            word: row.try_get("word")?,
            parent: row.try_get("parent")?,
            priority: row.try_get("priority")?,
            status: WordStatus::from_string(row.try_get("status")?).expect("Invalid status"),
        })
    }
}

/// Returns true if word added
/// Returns false if word already existed
#[tracing::instrument(skip(pool))]
#[must_use]
pub async fn add(pool: Pool<MySql>, word: &str, parent: Option<i32>, priority: i32, status: WordStatus) -> Result<bool, BoxError> {
    if exists(&pool, word).await? {
        return Ok(false);
    }

    query("INSERT INTO word (word, parent, priority, status) VALUES (?, ?, ?, ?)")
        .bind(word.to_string())
        .bind(parent)
        .bind(priority)
        .bind(status.to_string())
        .execute(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;
   
    Ok(true)
}

#[tracing::instrument(skip(pool))]
#[must_use]
pub async fn words_with_status(pool: Pool<MySql>, status: WordStatus) -> Result<i32, BoxError> {
    Ok(query_as::<_, CountRow>("SELECT count(id) FROM word WHERE status = ?")
        .bind(status.to_string())
        .fetch_one(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?
        .0)
}

#[tracing::instrument(skip(pool))]
#[must_use]
pub async fn set_status(pool: Pool<MySql>, id: i32, status: WordStatus) -> Result<(), BoxError> {
    query("UPDATE word SET status = ? WHERE id = ?")
        .bind(status.to_string())
        .bind(id)
        .execute(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;
    
    Ok(())
}

#[tracing::instrument(skip(pool))]
#[must_use]
pub async fn reset_tasks(pool: Pool<MySql>) -> Result<(), BoxError> {
    query("UPDATE word SET status = 'WAITING_FOR_GENERATION' WHERE status = 'GENERATING'")
        .execute(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    query("UPDATE word SET status = 'WAITING_FOR_CLASSIFICATION' WHERE status = 'CLASSIFYING'")
        .execute(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    query("UPDATE word SET status = 'WAITING_FOR_SEARCH' WHERE status = 'SEARCHING'")
        .execute(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    Ok(())
}

#[tracing::instrument(skip(pool))]
#[must_use]
async fn exists(pool: &Pool<MySql>, word: &str) -> Result<bool, BoxError> {
    Ok(query("SELECT id FROM word WHERE word = ? LIMIT 1")
        .bind(word)
        .fetch_optional(pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?
        .is_some())
}

#[tracing::instrument(skip(pool))]
#[must_use]
pub async fn next_job(pool: Pool<MySql>, status_from: WordStatus, status_to: WordStatus) -> Result<Option<Word>, BoxError> {
    let tx = pool.begin()
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    let Some(result) = query_as::<_, Word>("SELECT id, word, parent, priority, status FROM word WHERE status = ? ORDER BY priority DESC LIMIT 1")
            .bind(status_from.to_string())
            .fetch_optional(&pool)
            .await
            .map_err(|err| Box::new(err) as BoxError)?
    else {
        return Ok(None);
    };

    query("UPDATE word SET status = ? WHERE id = ?")
        .bind(status_to.to_string())
        .bind(result.id)
        .execute(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    tx.commit()
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    Ok(Some(result))
}

