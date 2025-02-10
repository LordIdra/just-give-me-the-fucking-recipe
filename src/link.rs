use std::{error::Error, fmt, sync::Arc, time::Duration};

use log::{debug, info, trace, warn};
use redis::{aio::MultiplexedConnection, AsyncCommands};
use reqwest::{Certificate, Client, ClientBuilder, Proxy};
use serde::{Deserialize, Serialize};
use sqlx::{MySql, Pool};
use tokio::{sync::Semaphore, time::interval};
use url::Url;

use crate::{link, link_blacklist, recipe, BoxError};

pub mod downloader;
pub mod extractor;
pub mod follower;
pub mod parser;

#[derive(Debug)]
pub struct ProcessingLinkNotFound;

impl fmt::Display for ProcessingLinkNotFound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Processing link not found")
    }
}

impl Error for ProcessingLinkNotFound {}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
pub enum LinkStatus {
    Waiting,
    Processing,
    DownloadFailed,
    ExtractionFailed,
    Processed,
}

impl LinkStatus {
    pub fn from_string(x: &str) -> Option<Self> {
        match x {
            "waiting" => Some(LinkStatus::Waiting),
            "processing" => Some(LinkStatus::Processing),
            "download_failed" => Some(LinkStatus::DownloadFailed),
            "extraction_failed" => Some(LinkStatus::ExtractionFailed),
            "processed" => Some(LinkStatus::Processed),
            _ => None,
        }
    }

    pub fn to_string(self) -> &'static str {
        match self {
            LinkStatus::Waiting => "waiting",
            LinkStatus::Processing => "processing",
            LinkStatus::DownloadFailed => "download_failed",
            LinkStatus::ExtractionFailed => "extraction_failed",
            LinkStatus::Processed => "processed",
        }
    }
}

fn key_status_to_links(status: LinkStatus) -> String {
    format!("link:links_by_status:{}", status.to_string())
}

fn key_domain_to_waiting_links(domain: &str) -> String {
    format!("link:waiting_links_by_domain:{}", domain)
}

fn key_processing_domains() -> String {
    "link:processing_domains".to_string()
}


fn key_not_processing_domains() -> String {
    "link:not_processing_domains".to_string()
}

fn key_link_to_status() -> String {
    "link:status".to_string()
}

fn key_link_to_priority() -> String {
    "link:priority".to_string()
}

fn key_link_to_domain() -> String {
    "link:domain".to_string()
}

fn key_link_to_content_size() -> String {
    "link:content_size".to_string()
}

#[tracing::instrument(skip(pool))]
#[must_use]
pub async fn reset_tasks(mut pool: MultiplexedConnection) -> Result<(), BoxError> {
    let processing: Vec<String> = pool.zrange(key_status_to_links(LinkStatus::Processing), 0, -1)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;
    for word in processing {
        update_status(pool.clone(), &word, LinkStatus::Waiting).await?;
    }

    Ok(())
}

/// Returns true if added
/// Returns false if already existed or matches the blacklist
#[tracing::instrument(skip(pool))]
#[must_use]
pub async fn add(mut pool: MultiplexedConnection, link: &str, domain: &str, priority: f32) -> Result<bool, BoxError> {
    if !link_blacklist::is_allowed(pool.clone(), link).await? || exists(pool.clone(), link).await? {
        return Ok(false);
    }

    let _: () = redis::pipe()
        .zadd(key_status_to_links(LinkStatus::Waiting), link, priority)
        .hset(key_link_to_status(), link, LinkStatus::Waiting.to_string())
        .hset(key_link_to_priority(), link, priority)
        .hset(key_link_to_domain(), link, domain)
        .sadd(key_domain_to_waiting_links(domain), link)
        .exec_async(&mut pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    Ok(true)
}

#[tracing::instrument(skip(pool))]
#[must_use]
pub async fn get_status(mut pool: MultiplexedConnection, link: &str) -> Result<LinkStatus, BoxError> {
    let status: String = pool.hget(key_link_to_status(), link)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    Ok(LinkStatus::from_string(&status).unwrap())
}

#[tracing::instrument(skip(redis_pool))]
#[must_use]
pub async fn get_priority(mut redis_pool: MultiplexedConnection, link: &str) -> Result<f32, BoxError> {
    let priority: f32 = redis_pool.hget(key_link_to_priority(), link)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    Ok(priority)
}

#[tracing::instrument(skip(redis_pool))]
#[must_use]
pub async fn get_domain(mut redis_pool: MultiplexedConnection, link: &str) -> Result<String, BoxError> {
    let domain: String = redis_pool.hget(key_link_to_domain(), link)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    Ok(domain)
}

#[tracing::instrument(skip(redis_pool))]
#[must_use]
pub async fn set_content_size(mut redis_pool: MultiplexedConnection, link: &str, content_size: usize) -> Result<(), BoxError> {
    let _: () = redis_pool.hset(key_link_to_content_size(), link, content_size)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    Ok(())
}

#[tracing::instrument(skip(redis_pool))]
#[must_use]
pub async fn links_with_status(mut redis_pool: MultiplexedConnection, status: LinkStatus) -> Result<usize, BoxError> {
    let count: usize = redis_pool.zcard(key_status_to_links(status))
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    Ok(count)
}

#[tracing::instrument(skip(redis_pool))]
#[must_use]
pub async fn total_content_size(mut redis_pool: MultiplexedConnection) -> Result<usize, BoxError> {
    let sizes: Vec<String> = redis_pool.hvals(key_link_to_content_size())
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    Ok(sizes.iter().filter_map(|v| v.parse::<usize>().ok()).sum())
}

#[tracing::instrument(skip(redis_pool))]
#[must_use]
pub async fn poll_next_job(mut redis_pool: MultiplexedConnection) -> Result<Option<String>, BoxError> {
    let next_domain: Option<String> = redis_pool.spop(key_not_processing_domains())
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    let Some(next_domain) = next_domain else {
        return Ok(None);
    };

    let links: Vec<String> = redis_pool.zpopmax(key_domain_to_waiting_links(&next_domain), 1)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    let Some(link) = links.first() else {
        return Ok(None) // should not generally happen
    };

    update_status(redis_pool.clone(), link, LinkStatus::Processing)
        .await?;

    Ok(Some(link.to_string()))
}

#[tracing::instrument(skip(redis_pool))]
#[must_use]
pub async fn is_domain_waiting(mut redis_pool: MultiplexedConnection, domain: &str) -> Result<bool, BoxError> {
    let exists: bool = redis_pool.exists(key_domain_to_waiting_links(domain))
        .await
        .map_err(|err| Box::new(err) as BoxError)?;
    Ok(exists)
}

#[tracing::instrument(skip(redis_pool))]
#[must_use]
pub async fn update_status(mut redis_pool: MultiplexedConnection, link: &str, status: LinkStatus) -> Result<(), BoxError> {
    let previous_status = get_status(redis_pool.clone(), link)
        .await?;
    let priority = get_priority(redis_pool.clone(), link)
        .await?;
    let domain = get_domain(redis_pool.clone(), link)
        .await?;

    let mut pipe = redis::pipe();
    pipe.zrem(key_status_to_links(previous_status), link);
    pipe.zadd(key_status_to_links(status), link, priority);
    pipe.hset(key_link_to_status(), link, status.to_string());

    if status == LinkStatus::Waiting {
        pipe.sadd(key_domain_to_waiting_links(&domain), link);
    }

    if previous_status == LinkStatus::Waiting {
        pipe.srem(key_domain_to_waiting_links(&domain), link);
    }

    pipe.exec_async(&mut redis_pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    // Results of previous computation are required here so use a new pipe
    let mut pipe = redis::pipe();
    if status == LinkStatus::Processing {
        pipe.srem(key_not_processing_domains(), domain.clone());
        pipe.sadd(key_processing_domains(), domain.clone());
    }

    if previous_status == LinkStatus::Processing {
        pipe.srem(key_processing_domains(), domain.clone());
        if is_domain_waiting(redis_pool.clone(), &domain).await? {
            pipe.sadd(key_processing_domains(), domain.clone());
        }
    }

    pipe.exec_async(&mut redis_pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    Ok(())
}

#[tracing::instrument(skip(redis_pool))]
#[must_use]
async fn exists(mut redis_pool: MultiplexedConnection, link: &str) -> Result<bool, BoxError> {
    let exists: bool = redis_pool.hexists(key_link_to_status(), link)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    Ok(exists)
}

#[tracing::instrument(skip(sql_pool, redis_pool, client, semaphore))]
pub async fn process(sql_pool: Pool<MySql>, redis_pool: MultiplexedConnection, client: Client, semaphore: Arc<Semaphore>, link: String) {
    let _permit = semaphore.acquire().await.unwrap();

    // Download
    let download_result = downloader::download(redis_pool.clone(), client, link.clone()).await;
    if let Err(err) = download_result {
        debug!("Error while downloading {}: {} (source: {:?})", link, err, err.source());

        if let Err(err) = update_status(redis_pool.clone(), &link, LinkStatus::DownloadFailed).await {
            warn!("Error while setting download failed for {}: {} (source: {:?})", link, err, err.source());
        }

        return;
    }

    let contents = download_result.unwrap();

    trace!("Downloaded {} ({} characters)", link, &contents.len());

    // Extract
    let extract_result = extractor::extract(&contents).await;
    if let Err(err) = extract_result {
        debug!("Error while extracting schema from {}: {} (source: {:?})", link, err, err.source());

        if let Err(err) = update_status(redis_pool.clone(), &link, LinkStatus::ExtractionFailed).await {
            warn!("Error while setting extraction failed for {}: {} (source: {:?})", link, err, err.source());
        }

        if let Err(err) = set_content_size(redis_pool.clone(), &link, contents.len()).await {
            warn!("Error while setting content size for {}: {} (source: {:?})", link, err, err.source());
        }

        return;
    }

    let schema = extract_result.unwrap();

    trace!("Extracted schema from {}", link);

    // Parse
    // Set to processed now rather than after following to avoid duplicate recipes appearing
    let processed_result = update_status(redis_pool.clone(), &link, LinkStatus::Processed).await;
    if let Err(err) = processed_result {
        warn!("Error while setting status processed for {}: {} (source: {:?})", link, err, err.source());
        return;
    }
    
    if let Err(err) = set_content_size(redis_pool.clone(), &link, contents.len()).await {
        warn!("Error while setting content size failed for {}: {} (source: {:?})", link, err, err.source());
    }

    let recipe = parser::parse(link.to_string(), schema).await;
    let is_complete = recipe.is_complete();

    if recipe.should_add() {
        if let Err(err) = recipe::add(sql_pool.clone(), recipe).await {
            warn!("Error while adding recipe from {}: {} (source: {:?})", link, err, err.source());
            return;
        }
    }

    if !is_complete {
        trace!("Parsed incomplete recipe from {}", link);
        return;

    } else {
        trace!("Parsed complete recipe from {}", link);
    }

    // Follow
    let new_links = follower::follow(contents, link.to_string()).await;

    let mut added_links = vec![];
    for new_link in &new_links {
        let maybe_domain = Url::parse(new_link)
            .ok()
            .and_then(|url| url.domain().map(|domain| domain.to_owned()));
        if let Some(domain) = maybe_domain {
            match link::add(redis_pool.clone(), new_link, &domain, -1000.0).await {
                Ok(added) => if added { 
                    added_links.push(new_link) 
                }
                Err(err) => warn!("Error while adding new followed link {}: {} (source: {:?})", new_link, err, err.source()),
            }
        }
    }

    trace!("Followed {}/{} links from {}: {:?}", added_links.len(), new_links.len(), link, &added_links);
}

pub async fn run(sql_pool: Pool<MySql>, redis_pool: MultiplexedConnection, proxy: String, certificates: Vec<Certificate>) {
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
        
        loop {
            if semaphore.available_permits() == 0 {
                break;
            }

            let link_result = poll_next_job(redis_pool.clone()).await;
            if let Err(err) = link_result {
                warn!("Error while getting next job: {} (source: {:?})", err, err.source());
                continue;
            }

            let Some(link) = link_result.unwrap() else {
                break;
            };

            tokio::spawn(process(sql_pool.clone(), redis_pool.clone(), client.clone(), semaphore.clone(), link));
        }
    }
}

