use std::{sync::Arc, time::Duration};

use axum::http::HeaderMap;
use log::{info, warn};
use reqwest::{Client, Method};
use serde::{Deserialize, Serialize};
use sqlx::{MySql, Pool};
use tokio::{sync::Semaphore, time::interval};
use url::Url;

use crate::{link::{self, LinkStatus}, word::{self, Word, WordStatus}, BoxError, UnexpectedStatusCodeErr};

const SERPER_API_URL: &str = "https://google.serper.dev/search";

const MIN_WAITING_FOR_DOWNLOAD: i32 = 100;

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
async fn search(pool: Pool<MySql>, client: Client, serper_key: String, word: Word) -> Result<(), BoxError> {
    let query = word.word.clone() + "recipe";
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

        link::add(
            pool.clone(), 
            &link.link, 
            domain,
            Some(word.id),
            None,
            word.priority, 
            LinkStatus::WaitingForProcessing,
        ).await?;
    }

    info!("Searched keyword '{}' and found: {:?}", word.word, link_names);

    word::set_status(pool, word.id, word::WordStatus::SearchComplete).await?;

    Ok(())
}

pub async fn run(pool: Pool<MySql>, serper_key: String) {
    info!("Started searcher");

    let client = Client::new();

    let semaphore = Arc::new(Semaphore::new(1));

    let mut interval = interval(Duration::from_millis(500));

    loop {
        interval.tick().await;

        let current_waiting_for_download = link::links_with_status_by_domain(pool.clone(), LinkStatus::WaitingForProcessing).await;
        if let Err(err) = current_waiting_for_download {
            warn!("Error while getting words with status WAITING_FOR_DOWNLOAD: {} (source: {:?})", err, err.source());
            continue;
        }

        if current_waiting_for_download.unwrap() >= MIN_WAITING_FOR_DOWNLOAD {
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

            let Some(state) = next_job.unwrap() else {
                break;
            };
            
            let sempahore = semaphore.clone();
            let client = client.clone();
            let serper_key = serper_key.clone();
            let pool = pool.clone();

            tokio::spawn(async move {
                let _permit = sempahore.acquire().await.unwrap();
                if let Err(err) = search(pool.clone(), client, serper_key, state.clone()).await {
                    warn!("Searcher encountered error on word #{} ('{}'): {} (source: {:?})", state.id, state.word, err, err.source());
                    if let Err(err) = word::set_status(pool, state.id, WordStatus::SearchFailed).await {
                        warn!("Error while setting status to failed on word #{} ('{}')@ {} (source: {:?})", state.id, state.word, err, err.source());
                    }
                }
            });
        }
    }
}

