use std::{error::Error, fmt, sync::Arc, time::Duration};

use log::{debug, info, trace, warn};
use redis::{aio::MultiplexedConnection, AsyncCommands};
use reqwest::{Certificate, Client, ClientBuilder, Proxy};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{MySql, Pool};
use tokio::{sync::Semaphore, task::JoinSet, time::interval};
use url::Url;

use crate::{link, link_blacklist, recipe::{self, Recipe}, BoxError};

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

fn key_link_to_remaining_follows() -> String {
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
pub async fn add(
    mut pool: MultiplexedConnection,
    link: &str,
    domain: &str,
    priority: f32,
    remaining_follows: i32,
) -> Result<bool, BoxError> {
    if !link_blacklist::is_allowed(pool.clone(), link).await? || exists(pool.clone(), link).await? {
        return Ok(false);
    }

    let _: () = redis::pipe()
        .zadd(key_status_to_links(LinkStatus::Waiting), link, priority)
        .hset(key_link_to_status(), link, LinkStatus::Waiting.to_string())
        .hset(key_link_to_priority(), link, priority)
        .hset(key_link_to_domain(), link, domain)
        .hset(key_link_to_remaining_follows(), link, remaining_follows)
        .sadd(key_waiting_domains(), domain)
        .exec_async(&mut pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    let is_domain_processing: bool = pool.sismember(key_processing_domains(), domain)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    if !is_domain_processing {
        let _: () = pool.zadd(key_domain_to_waiting_links(domain), link, priority)
            .await
            .map_err(|err| Box::new(err) as BoxError)?;
    }

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
pub async fn get_remaining_follows(mut redis_pool: MultiplexedConnection, link: &str) -> Result<i32, BoxError> {
    let remaining_follows: i32 = redis_pool.hget(key_link_to_remaining_follows(), link)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    Ok(remaining_follows)
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
pub async fn total_content_size(mut redis_pool: MultiplexedConnection) -> Result<u64, BoxError> {
    let sizes: Vec<String> = redis_pool.hvals(key_link_to_content_size())
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    Ok(sizes.iter().filter_map(|v| v.parse::<u64>().ok()).sum())
}

#[tracing::instrument(skip(redis_pool))]
#[must_use]
pub async fn poll_next_jobs(mut redis_pool: MultiplexedConnection, count: usize) -> Result<Vec<String>, BoxError> {
    let next_domains: Vec<String> = redis::cmd("SPOP").arg(key_waiting_domains()).arg(count).query_async(&mut redis_pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    let mut futures = JoinSet::new();
    for domain in next_domains {
        let redis_pool = redis_pool.clone();
        futures.spawn(async move {
            redis_pool.clone().zpopmax::<_, Vec<String>>(key_domain_to_waiting_links(&domain), 1).await
        });
    }
    let next_links: Vec<String> = futures.join_all()
        .await
        .into_iter()
        .collect::<Result<Vec<Vec<String>>, _>>()
        .map_err(|err| Box::new(err) as BoxError)?
        .into_iter()
        .filter_map(|entry| entry.first().cloned())
        .collect();

    let mut futures = JoinSet::new();
    for link in next_links.clone() {
        let redis_pool = redis_pool.clone();
        futures.spawn(async move {
            update_status(redis_pool.clone(), &link, LinkStatus::Processing).await
        });
    }
    futures.join_all()
        .await
        .into_iter()
        .collect::<Result<(), _>>()?;

    Ok(next_links)
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
pub async fn domains_in_system(mut redis_pool: MultiplexedConnection) -> Result<usize, BoxError> {
    let processing: usize = redis_pool.scard(key_processing_domains())
        .await
        .map_err(|err| Box::new(err) as BoxError)?;
    let waiting: usize = redis_pool.scard(key_waiting_domains())
        .await
        .map_err(|err| Box::new(err) as BoxError)?;
    Ok(processing + waiting)
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
        pipe.zadd(key_domain_to_waiting_links(&domain), link, priority);
    }

    if previous_status == LinkStatus::Waiting {
        pipe.zrem(key_domain_to_waiting_links(&domain), link);
    }

    pipe.exec_async(&mut redis_pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    // Results of previous computation are required here so use a new pipe
    let mut pipe = redis::pipe();
    if status == LinkStatus::Processing {
        pipe.srem(key_waiting_domains(), domain.clone());
        pipe.sadd(key_processing_domains(), domain.clone());
    }

    if previous_status == LinkStatus::Processing {
        pipe.srem(key_processing_domains(), domain.clone());
        if is_domain_waiting(redis_pool.clone(), &domain).await? {
            pipe.sadd(key_waiting_domains(), domain.clone());
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

#[tracing::instrument(skip(redis_pool, client))]
pub async fn process_download(
    redis_pool: MultiplexedConnection, 
    client: Client, 
    link: String
) -> Result<String, BoxError> {
    let downloaded = downloader::download(redis_pool.clone(), client, link.clone())
        .await;

    if let Err(err) = downloaded {
        update_status(redis_pool.clone(), &link, LinkStatus::DownloadFailed)
            .await?;
        return Err(err)
    }

    Ok(downloaded.unwrap())
}

#[tracing::instrument(skip(redis_pool, contents))]
pub async fn process_extract(
    redis_pool: MultiplexedConnection, 
    contents: String,
    link: String
) -> Result<Option<Value>, BoxError> {
    let extracted = extractor::extract(&contents)
        .await;

    if let Err(err) = extracted {
        update_status(redis_pool.clone(), &link, LinkStatus::ExtractionFailed)
            .await?;
        set_content_size(redis_pool.clone(), &link, contents.len())
            .await?;
        return Err(err);
    }

    let extracted = extracted.unwrap();

    if extracted.is_none() {
        update_status(redis_pool.clone(), &link, LinkStatus::ExtractionFailed)
            .await?;
        set_content_size(redis_pool.clone(), &link, contents.len())
            .await?;
        return Ok(None);
    }

    Ok(extracted)
}

#[tracing::instrument(skip(sql_pool, redis_pool, schema))]
pub async fn process_parse(
    sql_pool: Pool<MySql>,
    redis_pool: MultiplexedConnection, 
    schema: Value,
    link: String
) -> Result<Recipe, BoxError> {
    // Set to processed now rather than after following to avoid duplicate recipes appearing
    update_status(redis_pool.clone(), &link, LinkStatus::Processed)
        .await?;

    let parsed = parser::parse(link.to_string(), schema)
        .await;

    if parsed.should_add() {
        recipe::add(sql_pool.clone(), parsed.clone())
            .await?;
    }

    if !parsed.is_complete() {
        trace!("Parsed incomplete recipe from {}", link);
    } else {
        trace!("Parsed complete recipe from {}", link);
    }

    Ok(parsed)
}

#[tracing::instrument(skip(redis_pool, contents, recipe))]
pub async fn process_follow(
    redis_pool: MultiplexedConnection, 
    contents: String,
    recipe: Option<Recipe>,
    link: String
) -> Result<(), BoxError> {
    let recipe_should_add = recipe.as_ref().is_some_and(|recipe| recipe.should_add());
    let recipe_is_complete = recipe.as_ref().is_some_and(|recipe| recipe.is_complete());

    // Remaining follows
    let remaining_follows = get_remaining_follows(redis_pool.clone(), &link)
        .await?;
    if remaining_follows <= 0 && !recipe_is_complete {
        trace!("Terminated follow for {}", link);
        return Ok(())
    }

    let new_remaining_follows = if recipe_is_complete {
        2
    } else if recipe_should_add {
        1
    } else {
        remaining_follows - 1
    };

    // Priority
    let new_priority = if recipe_is_complete {
        0.0
    } else if recipe_should_add {
        -1.0
    } else {
        -2.0
    };

    // Following logic finally
    let new_links = follower::follow(contents, link.to_string()).await;

    let mut added_links = vec![];
    for new_link in &new_links {
        let maybe_domain = Url::parse(new_link)
            .ok()
            .and_then(|url| url.domain().map(|domain| domain.to_owned()));

        if let Some(domain) = maybe_domain {
            link::add(redis_pool.clone(), new_link, &domain, new_priority, new_remaining_follows)
                .await?;
            added_links.push(new_link) 
        }
    }

    trace!("Followed {}/{} links from {}: {:?}", added_links.len(), new_links.len(), link, &added_links);

    Ok(())
}

#[tracing::instrument(skip(sql_pool, redis_pool, client, semaphore))]
pub async fn process(
    sql_pool: Pool<MySql>, 
    redis_pool: MultiplexedConnection, 
    client: Client, 
    semaphore: Arc<Semaphore>, 
    link: String
) {
    let _permit = semaphore.acquire().await.unwrap();

    // Download
    let downloaded = process_download(redis_pool.clone(), client, link.clone())
        .await;
    if let Err(err) = downloaded {
        debug!("Error downloading {}: {} (source: {:?})", &link, err, err.source());
        return;
    }
    let downloaded = downloaded.unwrap();

    // Extract
    let extracted = process_extract(redis_pool.clone(), downloaded.clone(), link.clone())
        .await;
    if let Err(err) = extracted  {
        warn!("Error extracting {}: {} (source: {:?})", &link, err, err.source());
        return;
    }
    let extracted = extracted.unwrap();

    // Parse
    // (can't use map due to async closures being unstable)
    let parsed = match extracted {
        Some(extracted) => {
            let parsed = process_parse(sql_pool.clone(), redis_pool.clone(), extracted, link.clone())
                .await;
            if let Err(err) = parsed  {
                warn!("Error parsing {}: {} (source: {:?})", &link, err, err.source());
                return;
            }
            Some(parsed.unwrap())
        }
        None => None
    };

    // Follow
    let followed = process_follow(redis_pool.clone(), downloaded, parsed, link.clone())
        .await;
    if let Err(err) = followed  {
        warn!("Error following {}: {} (source: {:?})", &link, err, err.source());
        return;
    }
}

pub async fn run(sql_pool: Pool<MySql>, redis_pool: MultiplexedConnection, proxy: String, certificates: Vec<Certificate>) {
    info!("Started processor");

    let mut builder = ClientBuilder::new()
        .proxy(Proxy::all(proxy).unwrap());
    
    for certificate in certificates {
        builder = builder.add_root_certificate(certificate);
    }
    
    let client = builder.build().unwrap();
    let semaphore = Arc::new(Semaphore::new(2048));
    let mut interval = interval(Duration::from_millis(500));

    loop {
        interval.tick().await;
        
        if semaphore.available_permits() == 0 {
            continue;
        }

        let links_result = poll_next_jobs(redis_pool.clone(), semaphore.available_permits()).await;
        if let Err(err) = links_result {
            warn!("Error while getting next job: {} (source: {:?})", err, err.source());
            continue;
        }

        for link in links_result.unwrap() {
            tokio::spawn(process(sql_pool.clone(), redis_pool.clone(), client.clone(), semaphore.clone(), link));
        }
    }
}

