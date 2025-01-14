use std::{error::Error, fmt, sync::Arc, time::Duration};

use log::{debug, info, trace, warn};
use reqwest::{Certificate, Client, ClientBuilder, Proxy};
use sqlx::{mysql::MySqlRow, prelude::FromRow, query, query_as, MySql, Pool, Row};
use tokio::{sync::Semaphore, time::interval};
use url::Url;

use crate::{link, link_blacklist, recipe, BoxError};

pub mod downloader;
pub mod extractor;
pub mod follower;
pub mod parser;

#[derive(Debug, Clone, FromRow)]
pub struct CountRow(i32);

#[derive(Debug, Clone)]
pub struct ProcessingLink {
    pub id: i32,
    pub link: String,
    pub domain: String,
    pub priority: i32,
}

impl FromRow<'_, MySqlRow> for ProcessingLink  {
    #[tracing::instrument(skip_all)]
    fn from_row(row: &'_ MySqlRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            id: row.try_get("id")?,
            link: row.try_get("link")?,
            domain: row.try_get("domain")?,
            priority: row.try_get("priority")?,
        })
    }
}

#[derive(Debug)]
pub struct ProcessingLinkNotFound;

impl fmt::Display for ProcessingLinkNotFound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Processing link not found")
    }
}

impl Error for ProcessingLinkNotFound {}

#[tracing::instrument(skip(pool))]
#[must_use]
pub async fn reset_tasks(pool: Pool<MySql>) -> Result<(), BoxError> {
    let tx = pool.begin()
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    let links = query_as::<_, ProcessingLink>("SELECT id, link, domain, priority FROM processing_link")
        .fetch_all(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    for link in links {
        set_waiting(pool.clone(), link.id).await?;
    }

    tx.commit()
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    Ok(())
}

/// Returns true if added
/// Returns false if already existed or matches the blacklist
#[tracing::instrument(skip(pool))]
#[must_use]
pub async fn add_waiting(pool: Pool<MySql>, link: &str, domain: &str, priority: i32) -> Result<bool, BoxError> {
    if !link_blacklist::is_allowed(&pool, link).await? || exists(pool.clone(), link).await? {
        return Ok(false);
    }

    query("INSERT INTO waiting_link (link, domain, priority) VALUES (?, ?, ?)")
        .bind(link)
        .bind(domain)
        .bind(priority)
        .execute(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    Ok(true)
}

#[tracing::instrument(skip(pool))]
#[must_use]
pub async fn poll_next_job(pool: Pool<MySql>) -> Result<Option<ProcessingLink>, BoxError> {
    let tx = pool.begin()
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    let waiting_link = query_as::<_, ProcessingLink>("SELECT waiting_link.id, waiting_link.link, waiting_link.domain, waiting_link.priority FROM waiting_link LEFT JOIN processing_link ON waiting_link.domain = processing_link.domain WHERE processing_link.domain IS NULL LIMIT 1")
        .fetch_optional(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    let link = if let Some(waiting_link) = &waiting_link {
        let processing_id = query("INSERT INTO processing_link (link, domain, priority) VALUES (?, ?, ?)")
            .bind(waiting_link.link.clone())
            .bind(waiting_link.domain.clone())
            .bind(waiting_link.priority)
            .execute(&pool)
            .await
            .map_err(|err| Box::new(err) as BoxError)?
            .last_insert_id();

        let processing_link = query_as::<_, ProcessingLink>("SELECT id, link, domain, priority FROM processing_link WHERE id = ?")
            .bind(processing_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| Box::new(err) as BoxError)?;

        query("DELETE FROM waiting_link WHERE id = ?")
            .bind(waiting_link.id)
            .execute(&pool)
            .await
            .map_err(|err| Box::new(err) as BoxError)?;

        Some(processing_link)
    } else {
        None
    };

    tx.commit()
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    Ok(link)
}

#[tracing::instrument(skip(pool))]
#[must_use]
pub async fn set_waiting(pool: Pool<MySql>, id: i32) -> Result<ProcessingLink, BoxError> {
    let tx = pool.begin()
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    let processing_link = query_as::<_, ProcessingLink>("SELECT id, link, domain, priority FROM processing_link WHERE id = ?")
        .bind(id)
        .fetch_optional(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    let Some(processing_link) = processing_link else {
        return Err(Box::new(ProcessingLinkNotFound));
    };

    query("INSERT INTO waiting_link (link, domain, priority) VALUES (?, ?, ?)")
        .bind(processing_link.link.clone())
        .bind(processing_link.domain.clone())
        .bind(processing_link.priority)
        .execute(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    query("DELETE FROM processing_link WHERE id = ?")
        .bind(processing_link.id)
        .execute(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    tx.commit()
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    Ok(processing_link)
}

#[tracing::instrument(skip(pool))]
#[must_use]
pub async fn set_download_failed(pool: Pool<MySql>, id: i32) -> Result<(), BoxError> {
    let tx = pool.begin()
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    let processing_link = query_as::<_, ProcessingLink>("SELECT id, link, domain, priority FROM processing_link WHERE id = ?")
        .bind(id)
        .fetch_optional(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    let Some(processing_link) = processing_link else {
        return Err(Box::new(ProcessingLinkNotFound));
    };

    query("INSERT INTO download_failed_link (link) VALUES (?)")
        .bind(processing_link.link.clone())
        .execute(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    query("DELETE FROM processing_link WHERE id = ?")
        .bind(processing_link.id)
        .execute(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    tx.commit()
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    Ok(())
}

#[tracing::instrument(skip(pool))]
#[must_use]
pub async fn set_extraction_failed(pool: Pool<MySql>, id: i32, content_size: i32) -> Result<(), BoxError> {
    let tx = pool.begin()
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    let processing_link = query_as::<_, ProcessingLink>("SELECT id, link, domain, priority FROM processing_link WHERE id = ?")
        .bind(id)
        .fetch_optional(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    let Some(processing_link) = processing_link else {
        return Err(Box::new(ProcessingLinkNotFound));
    };

    query("INSERT INTO extraction_failed_link (link, content_size) VALUES (?, ?)")
        .bind(processing_link.link.clone())
        .bind(content_size)
        .execute(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    query("DELETE FROM processing_link WHERE id = ?")
        .bind(processing_link.id)
        .execute(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    tx.commit()
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    Ok(())
}

#[tracing::instrument(skip(pool))]
#[must_use]
pub async fn set_processed(pool: Pool<MySql>, processing_id: i32, content_size: i32) -> Result<i32, BoxError> {
    let tx = pool.begin()
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    let processing_link = query_as::<_, ProcessingLink>("SELECT id, link, domain, priority FROM processing_link WHERE id = ?")
        .bind(processing_id)
        .fetch_optional(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    let Some(processing_link) = processing_link else {
        return Err(Box::new(ProcessingLinkNotFound));
    };

    let processed_id = query("INSERT INTO processed_link (link, content_size) VALUES (?, ?)")
        .bind(processing_link.link)
        .bind(content_size)
        .execute(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?
        .last_insert_id() as i32;

    query("DELETE FROM processing_link WHERE id = ?")
        .bind(processing_id)
       .execute(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    tx.commit()
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    Ok(processed_id)
}

#[tracing::instrument(skip(pool))]
#[must_use]
async fn exists(pool: Pool<MySql>, link: &str) -> Result<bool, BoxError> {
    let mut count = query_as::<_, CountRow>("SELECT count(waiting_link.id) FROM waiting_link WHERE link = ? LIMIT 1")
        .bind(link)
        .fetch_one(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?
        .0;

    count += query_as::<_, CountRow>("SELECT count(processing_link.id) FROM processing_link WHERE link = ? LIMIT 1")
        .bind(link)
        .fetch_one(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?
        .0;

    count += query_as::<_, CountRow>("SELECT count(download_failed_link.id) FROM download_failed_link WHERE link = ? LIMIT 1")
        .bind(link)
        .fetch_one(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?
        .0;

    count += query_as::<_, CountRow>("SELECT count(extraction_failed_link.id) FROM extraction_failed_link WHERE link = ? LIMIT 1")
        .bind(link)
        .fetch_one(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?
        .0;

    count += query_as::<_, CountRow>("SELECT count(processed_link.id) FROM processed_link WHERE link = ? LIMIT 1")
        .bind(link)
        .fetch_one(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?
        .0;

    Ok(count > 0)
}

#[tracing::instrument(skip(pool, client, semaphore))]
pub async fn process(pool: Pool<MySql>, client: Client, semaphore: Arc<Semaphore>, link: ProcessingLink) {
    let _permit = semaphore.acquire().await.unwrap();

    // Download
    let download_result = downloader::download(client, link.clone()).await;
    if let Err(err) = download_result {
        debug!("Error while downloading {}: {} (source: {:?})", link.link, err, err.source());

        if let Err(err) = set_download_failed(pool.clone(), link.id).await {
            warn!("Error while setting link to download failed {}: {} (source: {:?})", link.link, err, err.source());
        }

        return;
    }

    let contents = download_result.unwrap();

    trace!("Downloaded {} ({} characters)", link.link, &contents.len());

    // Extract
    let extract_result = extractor::extract(&contents).await;
    if let Err(err) = extract_result {
        debug!("Error while extracting schema from {}: {} (source: {:?})", link.link, err, err.source());

        if let Err(err) = set_extraction_failed(pool.clone(), link.id, contents.len() as i32).await {
            warn!("Error while setting link to extraction failed for {}: {} (source: {:?})", link.link, err, err.source());
        }

        return;
    }

    let schema = extract_result.unwrap();

    trace!("Extracted schema from {}", link.link);

    // Parse
    // Set to processing now rather than after following to avoid duplicate recipes appearing
    let processed_result = set_processed(pool.clone(), link.id, contents.len() as i32).await;
    if let Err(err) = processed_result {
        warn!("Error while setting status to PROCESSED for {}: {} (source: {:?})", link.link, err, err.source());

        return;
    }
    let id = processed_result.unwrap();

    let recipe = parser::parse(id, link.link.clone(), schema).await;
    let is_complete = recipe.is_complete();

    if recipe.should_add() {
        if let Err(err) = recipe::add(pool.clone(), recipe).await {
            warn!("Error while adding recipe from {}: {} (source: {:?})", link.link, err, err.source());

            return;
        }
    }

    if !is_complete {
        trace!("Parsed incomplete recipe from {}", link.link);
        return;

    } else {
        trace!("Parsed complete recipe from {}", link.link);
    }

    // Follow
    let new_links = follower::follow(contents, link.link.clone()).await;

    let mut added_links = vec![];
    for new_link in &new_links {
        let maybe_domain = Url::parse(new_link)
            .ok()
            .and_then(|url| url.domain().map(|domain| domain.to_owned()));
        if let Some(domain) = maybe_domain {
            match link::add_waiting(pool.clone(), new_link, &domain, -1000).await {
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
    let semaphore = Arc::new(Semaphore::new(8));
    let mut interval = interval(Duration::from_millis(2000));

    loop {
        interval.tick().await;

        if semaphore.available_permits() == 0 {
            continue;
        }

        loop {
            let link_result = poll_next_job(pool.clone()).await;
            if let Err(err) = link_result {
                warn!("Error while getting next job: {} (source: {:?})", err, err.source());
                continue;
            }

            let Some(link) = link_result.unwrap() else {
                break;
            };

            tokio::spawn(process(pool.clone(), client.clone(), semaphore.clone(), link));
        }
    }
}

