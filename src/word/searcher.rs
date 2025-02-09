use std::{sync::Arc, time::Duration};

use axum::http::HeaderMap;
use log::{info, trace, warn};
use redis::aio::MultiplexedConnection;
use reqwest::{Client, Method};
use serde::{Deserialize, Serialize};
use sqlx::{query_as, FromRow, MySql, Pool};
use tokio::{sync::Semaphore, time::interval};
use url::Url;

use crate::{link::{self}, word::{self, WordStatus}, BoxError, UnexpectedStatusCodeErr};

const SERPER_API_URL: &str = "https://google.serper.dev/search";

const MIN_WAITING_FOR_PROCESSING: i64 = 200;

#[derive(FromRow)]
struct OneBigInt(i64);

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
async fn search(sql_pool: Pool<MySql>, pool: MultiplexedConnection, client: Client, serper_key: String, job: &str) -> Result<(), BoxError> {
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
        link::add_waiting(sql_pool.clone(), &link.link, domain, priority as i32).await?;
    }

    trace!("Searched keyword '{}' and found: {:?}", job, link_names);

    word::update_status(pool, job, word::WordStatus::SearchComplete).await?;

    Ok(())
}

pub async fn waiting_domains_count(pool: Pool<MySql>) -> Result<i64, BoxError> {
    // REALLY SLOW QUERY
    Ok(query_as::<_, OneBigInt>("SELECT COUNT(DISTINCT domain) FROM waiting_link")
        .fetch_one(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?
        .0)
}

pub async fn run(sql_pool: Pool<MySql>, pool: MultiplexedConnection, serper_key: String) {
    info!("Started searcher");

    let client = Client::new();

    let semaphore = Arc::new(Semaphore::new(1));

    let mut interval = interval(Duration::from_millis(60000));

    loop {
        interval.tick().await;

        let current_domains_waiting_for_processing = waiting_domains_count(sql_pool.clone()).await;
        if let Err(err) = current_domains_waiting_for_processing {
            warn!("Error while getting words with status WAITING_FOR_PROCESSING: {} (source: {:?})", err, err.source());
            continue;
        }

        if current_domains_waiting_for_processing.unwrap() >= MIN_WAITING_FOR_PROCESSING {
            continue;
        }

        loop {
            if semaphore.available_permits() == 0 {
                break
            }

            let next_job = word::next_job(pool.clone(), WordStatus::WaitingForSearch, WordStatus::SearchComplete).await;
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
            let sql_pool = sql_pool.clone();
            let pool = pool.clone();

            tokio::spawn(async move {
                let _permit = sempahore.acquire().await.unwrap();
                if let Err(err) = search(sql_pool, pool.clone(), client, serper_key, &next_job).await {
                    warn!("Searcher encountered error on word '{}': {} (source: {:?})", next_job, err, err.source());
                    if let Err(err) = word::update_status(pool, &next_job, WordStatus::SearchFailed).await {
                        warn!("Error while setting status to failed on word '{}': {} (source: {:?})", next_job, err, err.source());
                    }
                }
            });
        }
    }
}

