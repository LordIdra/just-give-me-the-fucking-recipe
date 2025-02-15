use std::{sync::Arc, time::Duration};

use axum::http::HeaderMap;
use log::{info, trace, warn};
use redis::aio::MultiplexedConnection;
use reqwest::{Client, Method};
use serde::{Deserialize, Serialize};
use tokio::{sync::Semaphore, time::interval};
use url::Url;

use crate::{link::{self}, word::{self, WordStatus}, BoxError, UnexpectedStatusCodeErr};

const SERPER_API_URL: &str = "https://google.serper.dev/search";

const MIN_DOMAINS_IN_SYSTEM: usize = 1000;

#[derive(Debug, Serialize)]
struct SerperRequest {
    q: String,
    num: usize,
}

#[derive(Debug, Deserialize, Clone)]
struct SerperResponse {
    organic: Vec<SearchResult>,
}

#[derive(Debug, Deserialize, Clone)]
struct SearchResult {
    title: String,
    link: String,
    sitelinks: Option<Vec<SiteLink>>,
}

#[derive(Debug, Deserialize, Clone)]
struct SiteLink {
    title: String,
    link: String,
}

#[tracing::instrument(skip(pool, client, serper_key))]
async fn search(pool: MultiplexedConnection, client: Client, serper_key: String, job: &str) -> Result<(), BoxError> {
    let query = job.to_string() + " recipe";
    let mut headers = HeaderMap::new();
    headers.insert("X-API-KEY", serper_key.parse().unwrap());
    headers.insert("Content-Type", "application/json".parse().unwrap());

    let response = client.request(Method::POST, SERPER_API_URL)
        .headers(headers.clone())
        .json(&SerperRequest { q: query, num: 100 })
        .send()
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    if !response.status().is_success() {
        return Err(Box::new(UnexpectedStatusCodeErr(response.status())));
    }

    let response = response.json::<SerperResponse>()
        .await
        .map_err(|err| Box::new(err) as BoxError)?;
    
    let mut links = vec![];

    for result in &response.organic.clone() {
        links.push(SiteLink { 
            title: result.title.clone(), 
            link: result.link.clone(),
        });

        if let Some(sitelinks) = &result.sitelinks {
            for link in sitelinks {
                if !link.link.starts_with(&result.link) {
                    links.push(SiteLink { 
                        title: link.title.clone(), 
                        link: link.link.clone(), 
                    });
                }
            }
        }
    }

    let link_names: Vec<String> = links.iter()
        .map(|link| link.link.clone())
        .collect();

    for link in links {
        let parsed_url = Url::parse(&link.link)
            .map_err(|err| Box::new(err) as BoxError)?;

        let Some(domain) = parsed_url.domain() else {
            continue;
        };

        let priority = word::get_priority(pool.clone(), job).await?;
        link::add(pool.clone(), &link.link, domain, priority, 1).await?;
    }

    trace!("Searched keyword '{}' and found: {:?}", job, link_names);

    word::update_status(pool, job, word::WordStatus::SearchComplete).await?;

    Ok(())
}

pub async fn run(redis_pool: MultiplexedConnection, serper_key: String) {
    info!("Started searcher");

    let client = Client::new();

    let semaphore = Arc::new(Semaphore::new(1));

    let mut interval = interval(Duration::from_millis(1000));

    loop {
        interval.tick().await;

        let current_domains_waiting_for_processing = link::domains_in_system(redis_pool.clone()).await;
        if let Err(err) = current_domains_waiting_for_processing {
            warn!("Error while getting words with status WAITING_FOR_PROCESSING: {} (source: {:?})", err, err.source());
            continue;
        }

        if current_domains_waiting_for_processing.unwrap() >= MIN_DOMAINS_IN_SYSTEM {
            continue;
        }

        loop {
            if semaphore.available_permits() == 0 {
                break
            }

            let next_job = word::next_job(redis_pool.clone(), WordStatus::WaitingForSearch, WordStatus::SearchComplete).await;
            if let Err(err) = next_job {
                warn!("Error while getting next job: {}", err);
                break;
            }

            let Some(next_job) = next_job.unwrap() else {
                break;
            };
            
            let sempahore = semaphore.clone();
            let client = client.clone();
            let serper_key = serper_key.clone();
            let redis_pool = redis_pool.clone();

            tokio::spawn(async move {
                let _permit = sempahore.acquire().await.unwrap();
                if let Err(err) = search(redis_pool.clone(), client, serper_key, &next_job).await {
                    warn!("Searcher encountered error on word '{}': {} (source: {:?})", next_job, err, err.source());
                    if let Err(err) = word::update_status(redis_pool, &next_job, WordStatus::SearchFailed).await {
                        warn!("Error while setting status to failed on word '{}': {} (source: {:?})", next_job, err, err.source());
                    }
                }
            });
        }
    }
}

