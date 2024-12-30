use log::warn;
use serde::{Deserialize, Serialize};
use tokio_rusqlite::{params, Connection};

pub mod classifier;
pub mod generator;

#[derive(Debug, Deserialize, Serialize, Clone, Copy)]
pub enum WordStatus {
    WaitingForGeneration,
    Generating,
    GenerationComplete,
    WaitingForClassification,
    Classifying,
    ClassifiedAsInvalid,
    WaitingForSearch,
    Searching,
    SearchComplete,
}

impl WordStatus {
    pub fn from_string(x: &str) -> Option<Self> {
        match x {
            "WAITING_FOR_GENERATION" => Some(WordStatus::WaitingForGeneration),
            "GENERATING" => Some(WordStatus::Generating),
            "GENERATION_COMPLETE" => Some(WordStatus::GenerationComplete),
            "WAITING_FOR_CLASSIFICATION" => Some(WordStatus::WaitingForClassification),
            "CLASSIFYING" => Some(WordStatus::Classifying),
            "CLASSIFIED_AS_INVALID" => Some(WordStatus::ClassifiedAsInvalid),
            "WAITING_FOR_SEARCH" => Some(WordStatus::WaitingForSearch),
            "SEARCHING" => Some(WordStatus::Searching),
            "SEARCH_COMPLETE" => Some(WordStatus::SearchComplete),
            _ => None,
        }
    }
    
    pub fn to_string(self) -> &'static str {
        match self {
            WordStatus::WaitingForGeneration => "WAITING_FOR_GENERATION",
            WordStatus::Generating => "GENERATING",
            WordStatus::GenerationComplete => "GENERATION_COMPLETE",
            WordStatus::WaitingForClassification => "WAITING_FOR_CLASSIFICATION",
            WordStatus::Classifying => "CLASSIFYING",
            WordStatus::ClassifiedAsInvalid => "CLASSIFIED_AS_INVALID",
            WordStatus::WaitingForSearch => "WAITING_FOR_SEARCH",
            WordStatus::Searching => "SEARCHING",
            WordStatus::SearchComplete => "SEARCH_COMPLETE",
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Word {
    pub id: u64,
    pub word: String,
    pub parent: Option<u64>,
    pub priority: i32,
    pub status: WordStatus,
}

/// Returns true if word added
/// Returns false if word already existed
pub async fn add(connection: Connection, word: &str, parent: Option<u64>, priority: i32, status: WordStatus) -> bool {
    if exists(connection.clone(), word).await {
        return false;
    }

    let word = word.to_string();
    let parent = parent.map(|str| str.to_string());
    let status = status.to_string();
    let sql = "INSERT INTO word (word, parent, priority, status) VALUES (?1, ?2, ?3, ?4)";

    let result = connection.call(move |c| {
        c.execute(sql, params![word, parent, priority, status])?;
        Ok(())
    }).await;

    if let Err(err) = result {
        warn!("Error while adding word: {}", err);
    }

    true
}

pub async fn set_status(connection: Connection, id: u64, status: WordStatus) {
    let status = status.to_string();
    let sql = "UPDATE word SET status = ?1 WHERE id = ?2";

    let result = connection.call(move |c| {
        c.execute(sql, params![status, id])?;
        Ok(())
    }).await;

    if let Err(err) = result {
        warn!("Error while updating word status: {}", err);
    }
}

pub async fn reset_tasks(connection: Connection) {
    let sql1 = "UPDATE word SET status = 'WAITING_FOR_GENERATION' WHERE status = 'GENERATING'"; 
    let sql2 = "UPDATE word SET status = 'WAITING_FOR_CLASSIFICATION' WHERE status = 'CLASSIFYING'"; 
    let sql3 = "UPDATE word SET status = 'WAITING_FOR_SEARCH' WHERE status = 'SEARCHING'"; 

    let result = connection.call(move |c| {
        c.execute(sql1, params![])?;
        c.execute(sql2, params![])?;
        c.execute(sql3, params![])?;
        Ok(())
    }).await;

    if let Err(err) = result {
        warn!("Error while purging processing: {}", err);
    }
}

async fn exists(connection: Connection, word: &str) -> bool {
    let word = word.to_string();
    let sql = "SELECT id FROM word WHERE word = ?1 LIMIT 1";

    let result = connection.call(move |c| {
        let mut statement = c.prepare(sql)?;
        let mut result = statement.query([word])?;
        Ok(result.next()?.is_some())
    }).await;

    if let Err(err) = &result {
        warn!("Error while checking word exists: {}", err);
        return false;
    }

    result.unwrap()
}

pub async fn next_job(connection: Connection, status_from: WordStatus, status_to: WordStatus) -> Option<Word> {
    let sql1 = "SELECT id, word, parent, priority, status FROM word WHERE status = ?1 ORDER BY priority ASC LIMIT 1";
    let sql2 = "UPDATE word SET status = ?1 WHERE id = ?2";

    let result = connection.call(move |c| {
        let mut statement = c.prepare(sql1)?;
        let mut result = statement.query([status_from.to_string()])?;

        let Some(row) = result.next()? else {
            return Ok(None);
        };
        
        let word = Word {
            id: row.get(0)?,
            word: row.get(1)?,
            parent: row.get(2)?,
            priority: row.get(3)?,
            status: WordStatus::from_string(&row.get::<_, String>(4)?).unwrap(),
        };

        c.execute(sql2, params![status_to.to_string(), word.id])?;

        Ok(Some(word))
    }).await;

    if let Err(err) = &result {
        warn!("Error while getting next job: {}", err);
        return None;
    }

    result.unwrap()
}

