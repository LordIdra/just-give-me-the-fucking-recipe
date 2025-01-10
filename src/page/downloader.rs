use std::{collections::HashMap, error::Error, fmt, sync::{Arc, LazyLock}, time::{Duration, Instant}};

use axum::http::HeaderMap;
use log::{info, warn};
use reqwest::{Client, Method};
use sqlx::{MySql, Pool};
use tokio::{sync::{Mutex, Semaphore}, time::{interval, sleep}};
use url::Url;

use crate::{page::{self, PageStatus}, BoxError};

use super::Page;

const MIN_WAITING_FOR_EXTRACTION: i32 = 100;

const REQUEST_INTERVAL_FOR_ONE_SITE: Duration = Duration::from_millis(3000);

static SEMAPHORES: LazyLock<Mutex<HashMap<String, Arc<Semaphore>>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Debug)]
pub struct InvalidDomainErr;

impl fmt::Display for InvalidDomainErr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Failed to get domain from link")
    }
}

impl Error for InvalidDomainErr {}

pub fn headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8".parse().unwrap());
    headers.insert("Accept-Language", "en-GB,en;q=0.5".parse().unwrap());
    headers.insert("Cache-Control", "no-cache".parse().unwrap());
    headers.insert("Pragma", "no-cache".parse().unwrap());
    headers.insert("Priority", "u=0, i".parse().unwrap());
    headers.insert("Sec-Ch-Ua", "\"Brave\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"".parse().unwrap());
    headers.insert("Sec-Ch-Ua-Mobile", "?0".parse().unwrap());
    headers.insert("Sec-Ch-Ua-Platform", "Linux".parse().unwrap());
    headers.insert("Sec-Fetch-Dest", "document".parse().unwrap());
    headers.insert("Sec-Fetch-Mode", "navigate".parse().unwrap());
    headers.insert("Sec-Fetch-Site", "none".parse().unwrap());
    headers.insert("Sec-Fetch-User", "?1".parse().unwrap());
    headers.insert("Sec-Gpc", "1".parse().unwrap());
    headers.insert("Upgrade-Insecure-Requests", "1".parse().unwrap());
    headers.insert("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36".parse().unwrap());
    headers
}

async fn download_impl(pool: Pool<MySql>, client: Client, page: Page) -> Result<(), BoxError> {
    let response = client.request(Method::GET, page.link.clone())
        .headers(headers())
        .send()
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    if !response.status().is_success() {
        page::set_status(pool, page.id, PageStatus::DownloadFailed).await?;
        info!("Unsuccessful status code {:?} for {}", response.status(), page.link);
        return Ok(())
    }

    let content = response.text()
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    page::set_content(pool.clone(), page.id, Some(&content)).await?;
    page::set_status(pool, page.id, PageStatus::WaitingForExtraction).await?;

    info!("Downloaded {} ({} characters)", page.link, content.len());

    Ok(())
}

#[tracing::instrument(skip(pool, client, page), fields(id = page.id))]
async fn download(pool: Pool<MySql>, client: Client, page: Page) -> Result<(), BoxError> {
    let Some(domain) = Url::parse(&page.link)
            .map_err(|err| Box::new(err) as BoxError)?
            .domain() 
            .map(|v| v.to_owned())
    else {
        return Err(Box::new(InvalidDomainErr))
    };

    let semaphore = SEMAPHORES.lock()
        .await
        .entry(domain.to_owned())
        .or_insert(Arc::new(Semaphore::new(1)))
        .clone();

    let _permit = semaphore.acquire()
        .await
        .unwrap();

    let start_time = Instant::now();

    let result = download_impl(pool, client, page).await;

    let elapsed_time = Instant::now() - start_time;
    if elapsed_time < REQUEST_INTERVAL_FOR_ONE_SITE {
        sleep(REQUEST_INTERVAL_FOR_ONE_SITE - elapsed_time).await;
    }

    result
}

pub async fn run(pool: Pool<MySql>) {
    info!("Started downloader");

    let client = Client::new();

    let semaphore = Arc::new(Semaphore::new(256));

    let mut interval = interval(Duration::from_millis(500));

    loop {
        interval.tick().await;

        let current_waiting_for_extraction = page::pages_with_status(pool.clone(), PageStatus::WaitingForExtraction).await;
        if let Err(err) = current_waiting_for_extraction {
            warn!("Error while getting words with status WAITING_FOR_EXTRACTION: {} (source: {:?})", err, err.source());
            continue;
        }

        if current_waiting_for_extraction.unwrap() >= MIN_WAITING_FOR_EXTRACTION {
            continue;
        }

        loop {
            if semaphore.available_permits() == 0 {
                break
            }

            let next_job = page::next_job(pool.clone(), PageStatus::WaitingForDownload, PageStatus::Downloading).await;
            if let Err(err) = next_job {
                warn!("Error while getting next job: {} (source: {:?})", err, err.source());
                break;
            }

            let Some(state) = next_job.unwrap() else {
                break;
            };
            
            let sempahore = semaphore.clone();
            let client = client.clone();
            let pool = pool.clone();

            tokio::spawn(async move {
                let _permit = sempahore.acquire().await.unwrap();
                if let Err(err) = download(pool.clone(), client, state.clone()).await {
                    warn!("Downloader encountered error on page #{} ('{}'): {} (source: {:?})", state.id, state.link, err, err.source());
                    if let Err(err) = page::set_status(pool.clone(), state.id, PageStatus::DownloadFailed).await {
                        warn!("Error while setting status to failed on page #{} ('{}')@ {} (source: {:?})", state.id, state.link, err, err.source());
                    }
                }
            });
        }
    }
}

