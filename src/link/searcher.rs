use std::time::{Duration, Instant};

use axum::http::HeaderMap;
use log::{info, warn};
use reqwest::{Client, Method};
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;
use tokio_rusqlite::Connection;

use crate::{link, word::{self, Word, WordStatus}};

use super::LinkStatus;

const POLL_INTERVAL_MILLIS: u128 = 100;

const SERPER_API_KEY: &str = "25a6cb43d17d54a402297c591f06f387a080bc2e";
const SERPER_API_URL: &str = "https://google.serper.dev/search";

static SERPER_SEMAPHORE: Semaphore = Semaphore::const_new(1);

#[derive(Debug, Serialize)]
struct SerperRequest {
    q: String,
}

#[derive(Debug, Deserialize, Clone)]
struct SerperResponse {
    results: Vec<SerperSingleResponse>,
}

#[derive(Debug, Deserialize, Clone)]
struct SerperSingleResponse {
    organic: Vec<SearchResult>,
}

#[derive(Debug, Deserialize, Clone)]
struct SearchResult {
    title: String,
    link: String,
}

async fn search(connection: Connection, word: Word) {
    let client = Client::new();

    let query = word.word + "recipe";
    let mut headers = HeaderMap::new();
    headers.insert("X-API-KEY", SERPER_API_KEY.parse().unwrap());
    headers.insert("Content-Type", "application/json".parse().unwrap());

    let response = client.request(Method::POST, SERPER_API_URL)
        .headers(headers.clone())
        .json(&SerperRequest { q: query })
        .send()
        .await;

    if let Err(err) = response {
        warn!("Serper request unsuccessful: {}", err);
        return;
    }
    let response = response.unwrap();

    if !response.status().is_success() {
        warn!("Serper request unsuccessful with status code {:?}", response.status());
        return;
    }

    let response2 = response.json::<SerperResponse>().await;
    if let Err(err) = response2 {
        warn!("Error while deserializing response from Serper: {:?}", err);
        return;
    }
    let response2 = response2.unwrap();

    for result in &response2.results[0].organic {
        link::add(connection.clone(), word.id, &result.title, &result.link, word.priority, LinkStatus::WaitingForDownload).await;
    }

    word::set_status(connection, word.id, word::WordStatus::SearchComplete).await;
}

pub async fn start(connection: Connection) {
    info!("Started searching task");

    loop {
        let start = Instant::now();

        while let Some(job) = word::next_job(connection.clone(), WordStatus::WaitingForSearch, WordStatus::Searching).await {
            let connection = connection.clone();
            tokio::spawn(async move {
                search(connection, job).await;
            });
        }

        let elapsed = (start - Instant::now()).as_millis();
        if elapsed < POLL_INTERVAL_MILLIS {
            let sleep_duration = Duration::from_millis((POLL_INTERVAL_MILLIS - elapsed) as u64);
            tokio::time::sleep(sleep_duration).await;
        }
    }
}

