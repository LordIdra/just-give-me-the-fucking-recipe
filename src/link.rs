use std::{sync::Arc, time::Duration};

use log::{debug, info, trace, warn};
use reqwest::{Certificate, Client, ClientBuilder, Proxy};
use serde::{Deserialize, Serialize};
use sqlx::{mysql::MySqlRow, prelude::FromRow, query, query_as, MySql, Pool, Row};
use tokio::{sync::Semaphore, time::interval};
use url::Url;

use crate::{link, link_blacklist, recipe, BoxError};

pub mod downloader;
pub mod extractor;
pub mod follower;
pub mod parser;

#[derive(FromRow)]
struct CountRow(i32);

#[derive(Debug, Deserialize, Serialize, Clone, Copy)]
pub enum LinkStatus {
    WaitingForProcessing,
    Processing,
    DownloadFailed,
    ExtractionFailed,
    ParsingIncompleteRecipe,
    FollowingFailed,
    Processed,
}

impl LinkStatus {
    #[allow(unused)]
    pub fn from_string(x: &str) -> Option<Self> {
        match x {
            "WAITING_FOR_PROCESSING" => Some(LinkStatus::WaitingForProcessing),
            "PROCESSING" => Some(LinkStatus::Processing),
            "DOWNLOAD_FAILED" => Some(LinkStatus::DownloadFailed),
            "EXTRACTION_FAILED" => Some(LinkStatus::ExtractionFailed),
            "PARSING_INCOMPLETE_RECIPE" => Some(LinkStatus::ParsingIncompleteRecipe),
            "FOLLOWING_FAILED" => Some(LinkStatus::FollowingFailed),
            "PROCESSED" => Some(LinkStatus::Processed),
            _ => None,
        }
    }
    
    pub fn to_string(self) -> &'static str {
        match self {
            LinkStatus::WaitingForProcessing => "WAITING_FOR_PROCESSING",
            LinkStatus::Processing => "PROCESSING",
            LinkStatus::DownloadFailed => "DOWNLOAD_FAILED",
            LinkStatus::ExtractionFailed => "EXTRACTION_FAILED",
            LinkStatus::ParsingIncompleteRecipe => "PARSING_INCOMPLETE_RECIPE",
            LinkStatus::FollowingFailed => "FOLLOWING_FAILED",
            LinkStatus::Processed => "PROCESSED",
        }
    }
}

#[derive(Debug, Clone, FromRow)]
pub struct Domain {
    domain: String,
}

#[derive(Debug, Clone)]
pub struct Link {
    pub id: i32,
    pub link: String,
    pub domain: String,
}

impl FromRow<'_, MySqlRow> for Link {
    #[tracing::instrument(skip_all)]
    fn from_row(row: &'_ MySqlRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            id: row.try_get("id")?,
            link: row.try_get("link")?,
            domain: row.try_get("domain")?,
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
    status: LinkStatus
) -> Result<bool, BoxError> {
    if exists(pool.clone(), link).await? || !link_blacklist::is_allowed(&pool, link).await? {
        return Ok(false);
    }

    query("INSERT INTO link (link, domain, word_source, follower_source, priority, status) VALUES (?, ?, ?, ?, ?, ?)")
        .bind(link)
        .bind(domain)
        .bind(word_source)
        .bind(follower_source)
        .bind(priority)
        .bind(status.to_string())
        .execute(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    Ok(true)
}

#[tracing::instrument(skip(pool))]
#[must_use]
pub async fn links_with_status_by_domain(pool: Pool<MySql>, status: LinkStatus) -> Result<i32, BoxError> {
    Ok(query_as::<_, CountRow>("SELECT count(id) FROM link WHERE status = ? GROUP BY domain")
        .bind(status.to_string())
        .fetch_one(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?
        .0)
}

#[tracing::instrument(skip(pool))]
#[must_use]
pub async fn set_status(pool: Pool<MySql>, id: i32, status: LinkStatus) -> Result<(), BoxError> {
    query("UPDATE link SET status = ? WHERE id = ?")
        .bind(status.to_string())
        .bind(id)
        .execute(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    Ok(())
}

#[tracing::instrument(skip(pool))]
#[must_use]
pub async fn set_content_size(pool: Pool<MySql>, id: i32, content_size: i32) -> Result<(), BoxError> {
    query("UPDATE link SET content_size = ? WHERE id = ?")
        .bind(content_size)
        .bind(id)
        .execute(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    Ok(())
}

#[tracing::instrument(skip(pool))]
#[must_use]
pub async fn reset_tasks(pool: Pool<MySql>) -> Result<(), BoxError> {
    query("UPDATE link SET status = 'WAITING_FOR_PROCESSING' WHERE status = 'PROCESSING'")
        .execute(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    Ok(())
}

#[tracing::instrument(skip(pool))]
#[must_use]
async fn exists(pool: Pool<MySql>, link: &str) -> Result<bool, BoxError> {
    Ok(query("SELECT id FROM link WHERE link = ? LIMIT 1")
        .bind(link)
        .fetch_optional(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?
        .is_some())
}

#[tracing::instrument(skip(pool))]
#[must_use]
async fn next_jobs(pool: Pool<MySql>, limit: usize) -> Result<Vec<Link>, BoxError> {
    assert!(limit > 0);

    let tx = pool.begin()
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    let domains_processing: Vec<String> = query_as::<_, Domain>(r#"SELECT domain FROM link WHERE status = "PROCESSING" GROUP BY domain"#)
        .fetch_all(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?
        .iter()
        .map(|v| v.domain.clone())
        .collect();

    let mut statement = r#"SELECT id, link, domain FROM link WHERE status = "WAITING_FOR_PROCESSING""#.to_string();

    if !domains_processing.is_empty() {
        statement += " AND domain NOT IN (";
        for (i, domain) in domains_processing.iter().enumerate() {
            if i != 0 {
                statement += ", ";
            }
            statement += &domain.to_string();
        }
        statement += r#")"#;
    }
    statement += &format!("ORDER BY priority DESC LIMIT {limit}");

    let links: Vec<Link> = query_as::<_, Link>(&statement)
        .fetch_all(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?
        .into_iter()
        .filter(|link| !domains_processing.contains(&link.domain))
        .collect();

    if !links.is_empty() {
        let mut statement = r#"UPDATE link SET status = "PROCESSING" WHERE id IN ("#.to_string();
        for (i, link) in links.iter().enumerate() {
            if i != 0 {
                statement += ", ";
            }
            statement += &link.id.to_string();
        }
        statement += ")";

        query(&statement)
            .execute(&pool)
            .await
            .map_err(|err| Box::new(err) as BoxError)?;
    }

    tx.commit()
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    Ok(links)
}

#[tracing::instrument(skip(pool, client, semaphore))]
pub async fn process(pool: Pool<MySql>, client: Client, semaphore: Arc<Semaphore>, link: Link) {
    let _permit = semaphore.acquire().await.unwrap();

    // Download
    let download_result = downloader::download(client, link.clone()).await;
    if let Err(err) = download_result {
        debug!("Error while downloading {}: {} (source: {:?})", link.link, err, err.source());

        if let Err(err) = link::set_status(pool.clone(), link.id, LinkStatus::DownloadFailed).await {
            warn!("Error while setting status to DOWNLOAD_FAILED for {}: {} (source: {:?})", link.link, err, err.source());
        }

        return;
    }

    let contents = download_result.unwrap();

    if let Err(err) = link::set_content_size(pool.clone(), link.id, contents.len() as i32).await {
        warn!("Error while setting content content size for {}: {} (source: {:?})", link.link, err, err.source());
        return;
    }

    trace!("Downloaded {} ({} characters)", link.link, &contents.len());

    // Extract
    let extract_result = extractor::extract(&contents).await;
    if let Err(err) = extract_result {
        debug!("Error while extracting schema from {}: {} (source: {:?})", link.link, err, err.source());

        if let Err(err) = link::set_status(pool.clone(), link.id, LinkStatus::ExtractionFailed).await {
            warn!("Error while setting status to EXTRACTION_FAILED for {}: {} (source: {:?})", link.link, err, err.source());
        }

        return;
    }

    let schema = extract_result.unwrap();


    trace!("Extracted schema from {}", link.link);

    // Parse
    let recipe = parser::parse(link.id, link.link.clone(), schema).await;

    let is_complete = recipe.is_complete();

    if let Err(err) = recipe::add(pool.clone(), recipe).await {
        warn!("Error while adding recipe from {}: {} (source: {:?})", link.link, err, err.source());
        return;
    }

    if !is_complete {
        if let Err(err) = link::set_status(pool.clone(), link.id, LinkStatus::ParsingIncompleteRecipe).await {
            warn!("Error while setting status to PARSING_INCOMPLETE_RECIPE for {}: {} (source: {:?})", link.link, err, err.source());
        }
        trace!("Parsed incomplete recipe from {}", link.link);
        return;
    }

    // Set to processing now rather than after following to avoid duplicate recipes appearing
    if let Err(err) = link::set_status(pool.clone(), link.id, LinkStatus::Processed).await {
        warn!("Error while setting status to PROCESSED for {}: {} (source: {:?})", link.link, err, err.source());
    }

    trace!("Parsed complete recipe from {}", link.link);

    // Follow
    let new_links = follower::follow(contents, link.link.clone()).await;

    let mut added_links = vec![];
    for new_link in &new_links {
        let maybe_domain = Url::parse(new_link)
            .ok()
            .and_then(|url| url.domain().map(|domain| domain.to_owned()));
        if let Some(domain) = maybe_domain {
            match link::add(pool.clone(), new_link, &domain, None, Some(link.id), -1000, LinkStatus::WaitingForProcessing).await {
                Ok(added) => if added { 
                    added_links.push(new_link) 
                }
                Err(err) => warn!("Error while adding new followed link {}: {} (source: {:?})", new_link, err, err.source()),
            }
        }
    }

    trace!("Followed {}/{} links from {}: {:?}", added_links.len(), new_links.len(), link.link, &added_links);
}

pub async fn run(pool: Pool<MySql>, proxy: String, certificates: Vec<Certificate>) {
    info!("Started processor");

    let mut builder = ClientBuilder::new()
        .proxy(Proxy::all(proxy).unwrap());
    
    for certificate in certificates {
        builder = builder.add_root_certificate(certificate);
    }
    
    let client = builder.build().unwrap();
    let semaphore = Arc::new(Semaphore::new(512));
    let mut interval = interval(Duration::from_millis(2000));

    loop {
        interval.tick().await;

        if semaphore.available_permits() == 0 {
            continue;
        }

        let next_jobs = link::next_jobs(pool.clone(), semaphore.available_permits()).await;
        if let Err(err) = next_jobs {
            warn!("Error while getting next job: {} (source: {:?})", err, err.source());
            continue;
        }

        for link in next_jobs.unwrap() {
            let pool = pool.clone();
            let client = client.clone();
            let semaphore = semaphore.clone();

            tokio::spawn(process(pool, client, semaphore, link));
        }
    }
}

