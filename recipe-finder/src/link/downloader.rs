use std::{collections::HashMap, sync::{Arc, LazyLock}, time::{Duration, Instant}};

use recipe_common::{link, BoxError};
use redis::aio::MultiplexedConnection;
use reqwest::{header::HeaderMap, Client, Method};
use tokio::{sync::{Mutex, Semaphore}, time::sleep};

use crate::UnexpectedStatusCodeErr;

const REQUEST_INTERVAL_FOR_ONE_SITE: Duration = Duration::from_millis(4000);
const ADDITIONAL_REQUEST_INTERVAL_MAX_MILLIS: i32 = 4000;

static SEMAPHORES: LazyLock<Mutex<HashMap<String, Arc<Semaphore>>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

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
    headers.insert("User-Agent", "Prototype recipe search engine indexer".parse().unwrap());
    headers
}

#[tracing::instrument(skip(redis_links, client))]
pub async fn download(redis_links: MultiplexedConnection, client: Client, job: String) -> Result<String, BoxError> {
    let domain = link::get_domain(redis_links.clone(), &job)
        .await?;

    let semaphore = SEMAPHORES.lock()
        .await
        .entry(domain)
        .or_insert(Arc::new(Semaphore::new(1)))
        .clone();

    let _permit = semaphore.acquire()
        .await
        .unwrap();

    let start_time = Instant::now();

    let response = client.request(Method::GET, job)
        .headers(headers())
        .send()
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    if !response.status().is_success() {
        return Err(Box::new(UnexpectedStatusCodeErr(response.status())));
    }

    let content = response.text()
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    let elapsed_time = Instant::now() - start_time;
    let request_interval = REQUEST_INTERVAL_FOR_ONE_SITE + Duration::from_millis((rand::random::<f64>() * ADDITIONAL_REQUEST_INTERVAL_MAX_MILLIS as f64) as u64);
    if elapsed_time < request_interval {
        sleep(request_interval - elapsed_time).await;
    }

    Ok(content)
}

