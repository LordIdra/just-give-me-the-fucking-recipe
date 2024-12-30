use log::warn;
use serde::{Deserialize, Serialize};
use tokio_rusqlite::{params, Connection};

pub mod searcher;

#[derive(Debug, Deserialize, Serialize, Clone, Copy)]
pub enum LinkStatus {
    WaitingForDownload,
    DownloadComplete,
}

impl LinkStatus {
    pub fn from_string(x: &str) -> Option<Self> {
        match x {
            "WAITING_FOR_DOWNLOAD" => Some(LinkStatus::WaitingForDownload),
            "DOWNLOAD_COMPLETE" => Some(LinkStatus::DownloadComplete),
            _ => None,
        }
    }
    
    pub fn to_string(self) -> &'static str {
        match self {
            LinkStatus::WaitingForDownload => "WAITING_FOR_DOWNLOAD",
            LinkStatus::DownloadComplete => "DOWNLOAD_COMPLETE",
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct Link {
    pub id: u64,
    pub word: i64,
    pub title: String,
    pub link: String,
    pub priority: i32,
    pub status: LinkStatus,
}

/// Returns true if link added
/// Returns false if link already existed
pub async fn add(connection: Connection, word: u64, title: &str, link: &str, priority: i32, status: LinkStatus) -> bool {
    if exists(connection.clone(), title).await {
        return false;
    }

    let word = word.to_string();
    let title = title .to_string();
    let link = link.to_string();
    let status = status.to_string();
    let sql = "INSERT INTO link (word, title, link, priority, status) VALUES (?1, ?2, ?3, ?4, ?5)";

    let result = connection.call(move |c| {
        c.execute(sql, params![word, title, link, priority, status])?;
        Ok(())
    }).await;

    if let Err(err) = result {
        warn!("Error while adding link: {}", err);
    }

    true
}

pub async fn set_status(connection: Connection, id: u64, status: LinkStatus) {
    let status = status.to_string();
    let sql = "UPDATE link SET status = ?1 WHERE id = ?2";

    let result = connection.call(move |c| {
        c.execute(sql, params![status, id])?;
        Ok(())
    }).await;

    if let Err(err) = result {
        warn!("Error while updating link status: {}", err);
    }
}

pub async fn reset_tasks(connection: Connection) {
    let sql = "UPDATE link SET status = 'WAITING_FOR_DOWNLOAD' WHERE status = 'DOWNLOADING'"; 

    let result = connection.call(move |c| {
        c.execute(sql, params![])?;
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

async fn next_job(connection: Connection) -> Option<Link> {
    let sql1 = "SELECT id, word, title, link, priority, status FROM word WHERE status = 'WAITING_FOR_DOWNLOAD' ORDER BY priority ASC LIMIT 1";
    let sql2 = "UPDATE word SET status = 'DOWNLOADING' WHERE id = ?1";

    let result = connection.call(move |c| {
        let mut statement = c.prepare(sql1)?;
        let mut result = statement.query([])?;

        let Some(row) = result.next()? else {
            return Ok(None);
        };
        
        let word = Link {
            id: row.get(0)?,
            word: row.get(1)?,
            title: row.get(2)?,
            link: row.get(3)?,
            priority: row.get(4)?,
            status: LinkStatus::from_string(&row.get::<_, String>(5)?).unwrap(),
        };

        c.execute(sql2, params![word.id])?;

        Ok(Some(word))
    }).await;

    if let Err(err) = &result {
        warn!("Error while getting next job: {}", err);
        return None;
    }

    result.unwrap()
}


