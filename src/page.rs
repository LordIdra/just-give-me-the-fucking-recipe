use serde::{Deserialize, Serialize};
use sqlx::{mysql::MySqlRow, prelude::FromRow, query, query_as, MySql, Pool, Row};

use crate::{link_blacklist, BoxError};

pub mod downloader;
pub mod extractor;
pub mod follower;
pub mod parser;

#[derive(FromRow)]
struct CountRow(i32);

#[derive(Debug, Deserialize, Serialize, Clone, Copy)]
pub enum PageStatus {
    WaitingForDownload,
    Downloading,
    DownloadFailed,
    WaitingForExtraction,
    Extracting,
    ExtractionFailed,
    WaitingForParsing,
    Parsing,
    ParsingIncompleteRecipe,
    WaitingForFollowing,
    Following,
    FollowingComplete,
    FollowingFailed,
}

impl PageStatus {
    pub fn from_string(x: &str) -> Option<Self> {
        match x {
            "WAITING_FOR_DOWNLOAD" => Some(PageStatus::WaitingForDownload),
            "DOWNLOADING" => Some(PageStatus::Downloading),
            "DOWNLOAD_FAILED" => Some(PageStatus::DownloadFailed),
            "WAITING_FOR_EXTRACTION" => Some(PageStatus::WaitingForExtraction),
            "EXTRACTING" => Some(PageStatus::Extracting),
            "EXTRACTION_FAILED" => Some(PageStatus::ExtractionFailed),
            "WAITING_FOR_PARSING" => Some(PageStatus::WaitingForParsing),
            "PARSING" => Some(PageStatus::Parsing),
            "PARSING_INCOMPLETE_RECIPE" => Some(PageStatus::ParsingIncompleteRecipe),
            "WAITING_FOR_FOLLOWING" => Some(PageStatus::WaitingForFollowing),
            "FOLLOWING" => Some(PageStatus::Following),
            "FOLLOWING_COMPLETE" => Some(PageStatus::FollowingComplete),
            "FOLLOWING_FAILED" => Some(PageStatus::FollowingFailed),
            _ => None,
        }
    }
    
    pub fn to_string(self) -> &'static str {
        match self {
            PageStatus::WaitingForDownload => "WAITING_FOR_DOWNLOAD",
            PageStatus::Downloading => "DOWNLOADING",
            PageStatus::DownloadFailed => "DOWNLOAD_FAILED",
            PageStatus::WaitingForExtraction => "WAITING_FOR_EXTRACTION",
            PageStatus::Extracting => "EXTRACTING",
            PageStatus::ExtractionFailed => "EXTRACTION_FAILED",
            PageStatus::WaitingForParsing => "WAITING_FOR_PARSING",
            PageStatus::Parsing => "PARSING",
            PageStatus::ParsingIncompleteRecipe => "PARSING_INCOMPLETE_RECIPE",
            PageStatus::WaitingForFollowing => "WAITING_FOR_FOLLOWING",
            PageStatus::Following=> "FOLLOWING",
            PageStatus::FollowingComplete => "FOLLOWING_COMPLETE",
            PageStatus::FollowingFailed => "FOLLOWING_FAILED",
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Page {
    pub id: i32,
    pub link: String,
    pub domain: String,
    pub content: Option<String>,
    pub schema: Option<String>,
    pub priority: i32,
    pub status: PageStatus,
}

impl FromRow<'_, MySqlRow> for Page {
    #[tracing::instrument(skip_all)]
    fn from_row(row: &'_ MySqlRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            id: row.try_get("id")?,
            link: row.try_get("link")?,
            domain: row.try_get("domain")?,
            content: row.try_get("content")?,
            schema: row.try_get("schema")?,
            priority: row.try_get("priority")?,
            status: PageStatus::from_string(row.try_get("status")?).expect("Invalid status"),
        })
    }
}

/// Returns true if added
/// Returns false if already existed or matches the blacklist
#[tracing::instrument(skip(pool))]
#[must_use]
pub async fn add(
    pool: Pool<MySql>, 
    link: &str, 
    domain: &str, 
    word_source: Option<i32>, 
    follower_source: Option<i32>, 
    priority: i32, 
    status: PageStatus
) -> Result<bool, BoxError> {
    if exists(pool.clone(), link).await? || !link_blacklist::is_allowed(&pool, link).await? {
        return Ok(false);
    }

    query("INSERT INTO page (link, domain, priority, status) VALUES (?, ?, ?, ?)")
        .bind(link)
        .bind(domain)
        .bind(priority)
        .bind(status.to_string())
        .execute(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    Ok(true)
}

#[tracing::instrument(skip(pool))]
#[must_use]
pub async fn get(pool: Pool<MySql>, id: i32) -> Result<Page, BoxError> {
    query_as::<_, Page>("SELECT id, link, domain, content, schema, priority, status FROM page WHERE id = ?")
        .bind(id)
        .fetch_one(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)
}

#[tracing::instrument(skip(pool))]
#[must_use]
pub async fn pages_with_status(pool: Pool<MySql>, status: PageStatus) -> Result<i32, BoxError> {
    Ok(query_as::<_, CountRow>("SELECT count(id) FROM page WHERE status = ?")
        .bind(status.to_string())
        .fetch_one(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?
        .0)
}

#[tracing::instrument(skip(pool))]
#[must_use]
pub async fn set_status(pool: Pool<MySql>, id: i32, status: PageStatus) -> Result<(), BoxError> {
    query("UPDATE page SET status = ? WHERE id = ?")
        .bind(status.to_string())
        .bind(id)
        .execute(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    Ok(())
}

#[tracing::instrument(skip(pool, content))]
#[must_use]
pub async fn set_content(pool: Pool<MySql>, id: i32, content: Option<&str>) -> Result<(), BoxError> {
    query("UPDATE page SET content = ? WHERE id = ?")
        .bind(content)
        .bind(id)
        .execute(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    Ok(())
}

#[tracing::instrument(skip(pool))]
#[must_use]
pub async fn set_content_size(pool: Pool<MySql>, id: i32, content_size: i32) -> Result<(), BoxError> {
    query("UPDATE page SET content_size = ? WHERE id = ?")
        .bind(content_size)
        .bind(id)
        .execute(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    Ok(())
}

#[tracing::instrument(skip(pool, schema))]
#[must_use]
pub async fn set_schema(pool: Pool<MySql>, id: i32, schema: Option<&str>) -> Result<(), BoxError> {
    query("UPDATE page SET schema = ? WHERE id = ?")
        .bind(schema)
        .bind(id)
        .execute(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    Ok(())
}

#[tracing::instrument(skip(pool))]
#[must_use]
pub async fn reset_tasks(pool: Pool<MySql>) -> Result<(), BoxError> {
    query("UPDATE page SET status = 'WAITING_FOR_DOWNLOAD' WHERE status = 'DOWNLOADING'")
        .execute(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    query("UPDATE page SET status = 'WAITING_FOR_EXTRACTION' WHERE status = 'EXTRACTING'")
        .execute(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    query("UPDATE page SET status = 'WAITING_FOR_PARSING' WHERE status = 'PARSING'")
        .execute(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    query("UPDATE page SET status = 'WAITING_FOR_FOLLOWING' WHERE status = 'FOLLOWING'")
        .execute(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    Ok(())
}

#[tracing::instrument(skip(pool))]
#[must_use]
async fn exists(pool: Pool<MySql>, link: &str) -> Result<bool, BoxError> {
    Ok(query("SELECT id FROM page WHERE link = ? LIMIT 1")
        .bind(link)
        .fetch_optional(&pool)
        .await
        .map_err(|err| dbg!(Box::new(err) as BoxError))?
        .is_some())
}

#[tracing::instrument(skip(pool))]
#[must_use]
async fn next_jobs(pool: Pool<MySql>, status_from: PageStatus, status_to: PageStatus, limit: usize) -> Result<Vec<Page>, BoxError> {
    assert!(limit > 0);
    let tx = pool.begin()
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    let result = query_as::<_, Page>(&format!("SELECT id, link, domain, content, schema, priority, status FROM page WHERE status = ? GROUP BY domain ORDER BY priority DESC LIMIT {limit}"))
        .bind(status_from.to_string())
        .fetch_all(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    if !result.is_empty() {
        let mut statement = "UPDATE page SET status = ? WHERE id IN (".to_string();
        for (i, page) in result.iter().enumerate() {
            if i != 0 {
                statement += ", ";
            }
            statement += &page.id.to_string();
        }
        statement += ")";

            let q = query(&statement)
                .bind(status_to.to_string());


        q.execute(&pool)
            .await
            .map_err(|err| Box::new(err) as BoxError)?;
    }

    tx.commit()
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    Ok(result)
}

