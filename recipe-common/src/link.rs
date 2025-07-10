use std::fmt;

use anyhow::Error;
use redis::{aio::MultiplexedConnection, AsyncCommands};
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;
use url::Url;

use crate::link_blacklist;

#[derive(Debug)]
pub struct ProcessingLinkNotFoundError;

impl fmt::Display for ProcessingLinkNotFoundError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Processing link not found")
    }
}

impl std::error::Error for ProcessingLinkNotFoundError {}

#[derive(Debug)]
pub struct LinkMissingDomainError {
    link: String
}

impl fmt::Display for LinkMissingDomainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Link did not have a domain: {}", self.link)
    }
}

impl std::error::Error for LinkMissingDomainError  {}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
pub enum LinkStatus {
    Waiting,
    Processing,
    DownloadFailed,
    ExtractionFailed,
    ParsingFailed,
    Processed,
}

impl LinkStatus {
    pub fn from_string(x: &str) -> Option<Self> {
        match x {
            "waiting" => Some(LinkStatus::Waiting),
            "processing" => Some(LinkStatus::Processing),
            "download_failed" => Some(LinkStatus::DownloadFailed),
            "extraction_failed" => Some(LinkStatus::ExtractionFailed),
            "parsing_failed" => Some(LinkStatus::ExtractionFailed),
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
            LinkStatus::ParsingFailed => "parsing_failed",
            LinkStatus::Processed => "processed",
        }
    }
}

fn key_status_to_links(status: LinkStatus) -> String {
    format!("link:links_by_status:{}", status.to_string())
}

fn key_domain_to_waiting_links(domain: &str) -> String {
    format!("link:waiting_links_by_domain:{domain}")
}

fn key_processing_domains() -> String {
    "link:processing_domains".to_string()
}

fn key_waiting_domains() -> String {
    "link:waiting_domains".to_string()
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

fn key_link_to_parent() -> String {
    "link:parent".to_string()
}

fn key_link_to_remaining_follows() -> String {
    "link:remaining_follows".to_string()
}

fn key_link_to_content_size() -> String {
    "link:content_size".to_string()
}

#[tracing::instrument(skip(pool))]
pub async fn reset_tasks(mut pool: MultiplexedConnection) -> Result<(), Error> {
    let processing: Vec<String> = pool.zrange(key_status_to_links(LinkStatus::Processing), 0, -1).await?;
    for link in processing {
        update_status(pool.clone(), &link, LinkStatus::Waiting).await?;
    }

    Ok(())
}

/// Returns true if added
/// Returns false if already existed or matches the blacklist
#[tracing::instrument(skip(pool))]
pub async fn add(
    mut pool: MultiplexedConnection,
    link: &str,
    parent: Option<&str>,
    priority: f32,
    remaining_follows: i32,
) -> Result<bool, Error> {
    if !link_blacklist::is_allowed(pool.clone(), link).await? || exists(pool.clone(), link).await? {
        return Ok(false);
    }

    let url = Url::parse(link)?;
    let Some(domain) = url.domain().map(|domain| domain.to_owned()) else {
        return Err(LinkMissingDomainError { link: link.to_owned() }.into())
    };

    let mut pipe = redis::pipe();
    pipe.zadd(key_status_to_links(LinkStatus::Waiting), link, priority)
        .hset(key_link_to_status(), link, LinkStatus::Waiting.to_string())
        .hset(key_link_to_priority(), link, priority)
        .hset(key_link_to_domain(), link, &domain)
        .hset(key_link_to_remaining_follows(), link, remaining_follows)
        .sadd(key_waiting_domains(), &domain);

    if let Some(parent) = parent {
        pipe.hset(key_link_to_parent(), link, parent);
    }

    pipe.exec_async(&mut pool).await?;
    let is_domain_processing: bool = pool.sismember(key_processing_domains(), &domain).await?;
    if !is_domain_processing {
        let _: () = pool.zadd(key_domain_to_waiting_links(&domain), link, priority).await?;
    }

    Ok(true)
}

#[tracing::instrument(skip(redis_links))]
pub async fn get_status(mut redis_links: MultiplexedConnection, link: &str) -> Result<LinkStatus, Error> {
    Ok(LinkStatus::from_string(&redis_links.hget::<_, _, String>(key_link_to_status(), link).await?).unwrap())
}

#[tracing::instrument(skip(redis_links))]
pub async fn get_priority(mut redis_links: MultiplexedConnection, link: &str) -> Result<f32, Error> {
    Ok(redis_links.hget(key_link_to_priority(), link).await?)
}

#[tracing::instrument(skip(redis_links))]
pub async fn get_parent(mut redis_links: MultiplexedConnection, link: &str) -> Result<Option<String>, Error> {
    Ok(redis_links.hget(key_link_to_parent(), link).await?)
}

#[tracing::instrument(skip(redis_links))]
pub async fn get_domain(mut redis_links: MultiplexedConnection, link: &str) -> Result<String, Error> {
    Ok(redis_links.hget(key_link_to_domain(), link).await?)
}

#[tracing::instrument(skip(redis_links))]
pub async fn get_remaining_follows(mut redis_links: MultiplexedConnection, link: &str) -> Result<i32, Error> {
    Ok(redis_links.hget(key_link_to_remaining_follows(), link).await?)
}

#[tracing::instrument(skip(redis_links))]
pub async fn set_content_size(mut redis_links: MultiplexedConnection, link: &str, content_size: usize) -> Result<(), Error> {
    Ok(redis_links.hset(key_link_to_content_size(), link, content_size).await?)
}

#[tracing::instrument(skip(redis_links))]
pub async fn links_with_status(mut redis_links: MultiplexedConnection, status: LinkStatus) -> Result<usize, Error> {
    Ok(redis_links.zcard(key_status_to_links(status)).await?)
}

#[tracing::instrument(skip(redis_links))]
pub async fn total_content_size(mut redis_links: MultiplexedConnection) -> Result<u64, Error> {
    let sizes: Vec<String> = redis_links.hvals(key_link_to_content_size()).await?;
    Ok(sizes.iter().filter_map(|v| v.parse::<u64>().ok()).sum())
}

#[tracing::instrument(skip(redis_links))]
pub async fn is_domain_waiting(mut redis_links: MultiplexedConnection, domain: &str) -> Result<bool, Error> {
    Ok(redis_links.exists(key_domain_to_waiting_links(domain)).await?)
}

#[tracing::instrument(skip(redis_links))]
pub async fn domains_in_system(mut redis_links: MultiplexedConnection) -> Result<usize, Error> {
    let processing: usize = redis_links.scard(key_processing_domains()).await?;
    let waiting: usize = redis_links.scard(key_waiting_domains()).await?;
    Ok(processing + waiting)
}

#[tracing::instrument(skip(redis_links))]
pub async fn get_links_by_status(mut redis_links: MultiplexedConnection, status: LinkStatus) -> Result<Vec<String>, Error> {
    Ok(redis::cmd("zrange").arg(key_status_to_links(status)).arg("0").arg("-1").query_async(&mut redis_links).await?)
}

#[tracing::instrument(skip(redis_links))]
pub async fn update_status(mut redis_links: MultiplexedConnection, link: &str, status: LinkStatus) -> Result<(), Error> {
    let previous_status = get_status(redis_links.clone(), link).await?;
    let priority = get_priority(redis_links.clone(), link).await?;
    let domain = get_domain(redis_links.clone(), link).await?;

    let mut pipe = redis::pipe();
    pipe.zrem(key_status_to_links(previous_status), link);
    pipe.zadd(key_status_to_links(status), link, priority);
    pipe.hset(key_link_to_status(), link, status.to_string());

    if status == LinkStatus::Waiting {
        pipe.zadd(key_domain_to_waiting_links(&domain), link, priority);
    }

    if previous_status == LinkStatus::Waiting {
        pipe.zrem(key_domain_to_waiting_links(&domain), link);
    }

    pipe.exec_async(&mut redis_links).await?;

    // Results of previous computation are required here so use a new pipe
    let mut pipe = redis::pipe();
    if status == LinkStatus::Processing {
        pipe.srem(key_waiting_domains(), domain.clone());
        pipe.sadd(key_processing_domains(), domain.clone());
    }

    if previous_status == LinkStatus::Processing {
        pipe.srem(key_processing_domains(), domain.clone());
        if is_domain_waiting(redis_links.clone(), &domain).await? {
            pipe.sadd(key_waiting_domains(), domain.clone());
        }
    }

    pipe.exec_async(&mut redis_links).await?;

    Ok(())
}

#[tracing::instrument(skip(redis_links))]
async fn exists(mut redis_links: MultiplexedConnection, link: &str) -> Result<bool, Error> {
    Ok(redis_links.hexists(key_link_to_status(), link).await?)
}

#[tracing::instrument(skip(redis_links))]
pub async fn poll_next_jobs(mut redis_links: MultiplexedConnection, count: usize) -> Result<Vec<String>, Error> {
    let next_domains: Vec<String> = redis::cmd("SPOP").arg(key_waiting_domains()).arg(count).query_async(&mut redis_links).await?;

    let mut futures = JoinSet::new();
    for domain in next_domains {
        let redis_links = redis_links.clone();
        futures.spawn(async move {
            redis_links.clone().zpopmax::<_, Vec<String>>(key_domain_to_waiting_links(&domain), 1).await
        });
    }
    let next_links: Vec<String> = futures.join_all()
        .await
        .into_iter()
        .collect::<Result<Vec<Vec<String>>, _>>()?
        .into_iter()
        .filter_map(|entry| entry.first().cloned())
        .collect();

    let mut futures = JoinSet::new();
    for link in next_links.clone() {
        let redis_links = redis_links.clone();
        futures.spawn(async move {
            update_status(redis_links.clone(), &link, LinkStatus::Processing).await
        });
    }
    futures.join_all()
        .await
        .into_iter()
        .collect::<Result<(), _>>()?;

    Ok(next_links)
}

