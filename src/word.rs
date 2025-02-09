use redis::{aio::MultiplexedConnection, AsyncCommands};
use serde::{Deserialize, Serialize};

use crate::BoxError;

pub mod classifier;
pub mod generator;
pub mod searcher;

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
    pub fn to_string(self) -> &'static str {
        match self {
            WordStatus::WaitingForGeneration => "waiting_for_generation",
            WordStatus::Generating => "generating",
            WordStatus::GenerationFailed => "generation_failed",
            WordStatus::GenerationComplete => "generation_complete",
            WordStatus::WaitingForClassification => "waiting_for_classification",
            WordStatus::Classifying => "classifying",
            WordStatus::ClassificationFailed => "classification_failed",
            WordStatus::ClassifiedAsInvalid => "classified_as_invalid",
            WordStatus::WaitingForSearch => "waiting_for_search",
            WordStatus::Searching => "searching",
            WordStatus::SearchFailed => "search_failed",
            WordStatus::SearchComplete => "search_complete",
        }
    }
}

fn key_status_words(status: WordStatus) -> String {
    format!("word:words_by_status:{}", status.to_string())
}

fn key_word_status(word: &str) -> String {
    format!("word:{}:status", word)
}

fn key_word_parent(word: &str) -> String {
    format!("word:{}:parent", word)
}

fn key_word_priority(word: &str) -> String {
    format!("word:{}:priority", word)
}

/// Returns true if word added
/// Returns false if word already existed
#[tracing::instrument(skip(pool))]
#[must_use]
pub async fn add(mut pool: MultiplexedConnection, word: &str, parent: Option<&str>, priority: i32, status: WordStatus) -> Result<bool, BoxError> {
    let mut pipe = redis::pipe();
    pipe.zadd(key_status_words(status), priority, word.to_string());
    pipe.set(key_word_status(word), status.to_string());
    pipe.set(key_word_priority(word), priority);

    if let Some(parent) = parent {
        pipe.set(key_word_parent(word), parent.to_string());
    }
        
    if exists(pool.clone(), word).await? {
        return Ok(false);
    }

    pipe.exec_async(&mut pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    Ok(true)
}

#[tracing::instrument(skip(pool))]
#[must_use]
pub async fn get_priority(mut pool: MultiplexedConnection, word: &str) -> Result<i32, BoxError> {
    let count: i32 = pool.get(key_word_priority(word))
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    Ok(count)
}

#[tracing::instrument(skip(pool))]
#[must_use]
pub async fn words_with_status(mut pool: MultiplexedConnection, status: WordStatus) -> Result<usize, BoxError> {
    let count: usize = pool.zcard(key_status_words(status))
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    Ok(count)
}

#[tracing::instrument(skip(pool))]
#[must_use]
pub async fn update_status(mut pool: MultiplexedConnection, word: &str, status: WordStatus) -> Result<(), BoxError> {
    let _: () = pool.set(key_word_status(word), status.to_string())
        .await
        .map_err(|err| Box::new(err) as BoxError)?;
    
    Ok(())
}

#[tracing::instrument(skip(pool))]
#[must_use]
pub async fn reset_tasks(mut pool: MultiplexedConnection) -> Result<(), BoxError> {
    let generating: Vec<String> = pool.zrange(key_status_words(WordStatus::Generating), 0, -1)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;
    for word in generating {
        update_status(pool.clone(), &word, WordStatus::WaitingForGeneration).await?;
    }

    let classifying: Vec<String> = pool.zrange(key_status_words(WordStatus::Classifying), 0, -1)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;
    for word in classifying {
        update_status(pool.clone(), &word, WordStatus::Classifying).await?;
    }

    let searching: Vec<String> = pool.zrange(key_status_words(WordStatus::Searching), 0, -1)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;
    for word in searching {
        update_status(pool.clone(), &word, WordStatus::WaitingForSearch).await?;
    }

    Ok(())
}

#[tracing::instrument(skip(pool))]
#[must_use]
async fn exists(mut pool: MultiplexedConnection, word: &str) -> Result<bool, BoxError> {
    let exists: bool = pool.exists(key_word_status(word))
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    Ok(exists)
}

#[tracing::instrument(skip(pool))]
#[must_use]
pub async fn next_job(mut pool: MultiplexedConnection, status_from: WordStatus, status_to: WordStatus) -> Result<Option<String>, BoxError> {
    let words: Vec<String> = pool.zpopmax(key_status_words(status_from), 1)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    let Some(word) = words.first() else {
        return Ok(None);
    };

    let _: () = pool.set(key_word_status(word), status_to.to_string())
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    Ok(Some(word.to_string()))
}

