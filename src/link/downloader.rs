use std::{collections::HashMap, sync::{Arc, LazyLock}, time::{Duration, Instant}};

use axum::http::HeaderMap;
use reqwest::{Client, Method};
use tokio::{sync::{Mutex, Semaphore}, time::sleep};

use crate::{BoxError, UnexpectedStatusCodeErr};

use super::Link;

const REQUEST_INTERVAL_FOR_ONE_SITE: Duration = Duration::from_millis(5000);

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

#[tracing::instrument(skip(client, link), fields(id = link.id))]
pub async fn download(client: Client, link: Link) -> Result<String, BoxError> {
    let semaphore = SEMAPHORES.lock()
        .await
        .entry(link.domain.to_owned())
        .or_insert(Arc::new(Semaphore::new(1)))
        .clone();

    let _permit = semaphore.acquire()
        .await
        .unwrap();

    let start_time = Instant::now();

    let response = client.request(Method::GET, link.link.clone())
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
    if elapsed_time < REQUEST_INTERVAL_FOR_ONE_SITE {
        sleep(REQUEST_INTERVAL_FOR_ONE_SITE - elapsed_time).await;
    }

    Ok(content)
}


