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
    pub const ALL_WORD_STATUSES: [WordStatus; 12] = [
        WordStatus::WaitingForGeneration,
        WordStatus::Generating,
        WordStatus::GenerationFailed,
        WordStatus::GenerationComplete,
        WordStatus::WaitingForClassification,
        WordStatus::Classifying,
        WordStatus::ClassificationFailed,
        WordStatus::ClassifiedAsInvalid,
        WordStatus::WaitingForSearch,
        WordStatus::Searching,
        WordStatus::SearchFailed,
        WordStatus::SearchComplete,
    ];

    pub fn from_string(x: &str) -> Option<Self> {
        match x {
            "waiting_for_generation" => Some(WordStatus::WaitingForGeneration),
            "generating" => Some(WordStatus::Generating),
            "generation_failed" => Some(WordStatus::GenerationFailed),
            "generation_complete" => Some(WordStatus::GenerationComplete),
            "waiting_for_classification" => Some(WordStatus::WaitingForClassification),
            "classifying" => Some(WordStatus::Classifying),
            "classification_failed" => Some(WordStatus::ClassificationFailed),
            "classified_as_invalid" => Some(WordStatus::ClassifiedAsInvalid),
            "waiting_for_search" => Some(WordStatus::WaitingForSearch),
            "searching" => Some(WordStatus::Searching),
            "search_failed" => Some(WordStatus::SearchFailed),
            "search_complete" => Some(WordStatus::SearchComplete),
            _ => None,
        }
    }

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

fn key_status_to_words(status: WordStatus) -> String {
    format!("word:words_by_status:{}", status.to_string())
}

fn key_word_to_status() -> String {
    "word:status".to_string()
}

fn key_word_to_parent() -> String {
    "word:parent".to_string()
}

fn key_word_to_priority() -> String {
    "word:priority".to_string()
}

/// Returns true if word added
/// Returns false if word already existed
#[tracing::instrument(skip(pool))]
#[must_use]
pub async fn add(mut pool: MultiplexedConnection, word: &str, parent: Option<&str>, priority: f32, status: WordStatus) -> Result<bool, BoxError> {
    let mut pipe = redis::pipe();
    pipe.zadd(key_status_to_words(status), word.to_string(), priority);
    pipe.hset(key_word_to_status(), word, status.to_string());
    pipe.hset(key_word_to_priority(), word, priority);

    if let Some(parent) = parent {
        pipe.hset(key_word_to_parent(), word, parent.to_string());
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
pub async fn get_priority(mut pool: MultiplexedConnection, word: &str) -> Result<f32, BoxError> {
    let priority: f32 = pool.hget(key_word_to_priority(), word)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    Ok(priority)
}

#[tracing::instrument(skip(pool))]
#[must_use]
pub async fn get_status(mut pool: MultiplexedConnection, word: &str) -> Result<WordStatus, BoxError> {
    let status: String = pool.hget(key_word_to_status(), word)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    Ok(WordStatus::from_string(&status).unwrap())
}

#[tracing::instrument(skip(pool))]
#[must_use]
pub async fn words_with_status(mut pool: MultiplexedConnection, status: WordStatus) -> Result<usize, BoxError> {
    let count: usize = pool.zcard(key_status_to_words(status))
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    Ok(count)
}

#[tracing::instrument(skip(pool))]
#[must_use]
pub async fn update_status(mut pool: MultiplexedConnection, word: &str, status: WordStatus) -> Result<(), BoxError> {
    let previous_status = get_status(pool.clone(), word)
        .await?;
    let priority = get_priority(pool.clone(), word)
        .await?;

    redis::pipe()
        .hset(key_word_to_status(), word, status.to_string())
        .zrem(key_status_to_words(previous_status), word)
        .zadd(key_status_to_words(status), word, priority)
        .exec_async(&mut pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;
    
    Ok(())
}

#[tracing::instrument(skip(pool))]
#[must_use]
pub async fn reset_tasks(mut pool: MultiplexedConnection) -> Result<(), BoxError> {
    let generating: Vec<String> = pool.zrange(key_status_to_words(WordStatus::Generating), 0, -1)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;
    for word in generating {
        update_status(pool.clone(), &word, WordStatus::WaitingForGeneration).await?;
    }

    let classifying: Vec<String> = pool.zrange(key_status_to_words(WordStatus::Classifying), 0, -1)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;
    for word in classifying {
        update_status(pool.clone(), &word, WordStatus::Classifying).await?;
    }

    let searching: Vec<String> = pool.zrange(key_status_to_words(WordStatus::Searching), 0, -1)
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
    let exists: bool = pool.hexists(key_word_to_status(), word)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    Ok(exists)
}

#[tracing::instrument(skip(pool))]
#[must_use]
pub async fn next_job(mut pool: MultiplexedConnection, status_from: WordStatus, status_to: WordStatus) -> Result<Option<String>, BoxError> {
    let words: Vec<String> = pool.zpopmax(key_status_to_words(status_from), 1)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    let Some(word) = words.first() else {
        return Ok(None);
    };

    update_status(pool.clone(), word, status_to)
        .await?;

    Ok(Some(word.to_string()))
}

