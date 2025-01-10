use std::fs::File;
use std::io::Read;
use std::{error::Error, fmt};

use axum::{extract::State, response::IntoResponse, routing::post, Json, Router};
use clap::Parser;
use log::{info, warn};
use page::{downloader, extractor, follower, parser};
use reqwest::{Certificate, StatusCode};
use serde::Deserialize;
use sqlx::{mysql::MySqlPoolOptions, MySql, Pool};
use tokio::net::TcpListener;
use tracing_subscriber::Layer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use word::{classifier, generator, searcher, WordStatus};

mod gpt;
mod link_blacklist;
mod page;
mod recipe;
mod statistic;
mod word;

type BoxError = Box<dyn Error + Send>;

#[derive(Debug)]
pub struct UnexpectedStatusCodeErr(StatusCode);

impl fmt::Display for UnexpectedStatusCodeErr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Unexpected status code '{}'", self.0)
    }
}

impl Error for UnexpectedStatusCodeErr {}

#[derive(Debug, Deserialize)]
struct SubmitKeyword {
    keyword: String,
}

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    port: usize,
    #[arg(long)]
    openai_key: String,
    #[arg(long)]
    serper_key: String,
    #[arg(long)]
    proxy: String,
    #[arg(long)]
    crt_file: String,
    #[arg(long)]
    database_url: String,
}

#[derive(Debug, Clone)]
struct AppState {
    pool: Pool<MySql>,
}

#[tokio::main]
async fn main() {
    let registry = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer()
        .with_line_number(true)
        .with_filter(EnvFilter::new("just_give_me_the_fucking_recipe=trace")));

    #[cfg(feature = "profiling")]
    let registry = registry.with(tracing_tracy::TracyLayer::default());

    registry.init();

    info!("Starting...");

    let args = Args::parse();

    let mut buf = vec![];
    File::open(args.crt_file)
        .unwrap()
        .read_to_end(&mut buf)
        .unwrap();

    let certificates = Certificate::from_pem_bundle(&buf).unwrap();
    
    let pool = MySqlPoolOptions::new()
        .max_connections(8)
        .connect(&args.database_url)
        .await
        .expect("Failed to connect to database");

    word::reset_tasks(pool.clone()).await.expect("Failed to reset word tasks");
    page::reset_tasks(pool.clone()).await.expect("Failed to reset page tasks");

    tokio::spawn(classifier::run(pool.clone(), args.openai_key.clone()));
    tokio::spawn(generator::run(pool.clone(), args.openai_key));
    tokio::spawn(searcher::run(pool.clone(), args.serper_key));
    tokio::spawn(downloader::run(pool.clone(), args.proxy, certificates));
    tokio::spawn(extractor::run(pool.clone()));
    tokio::spawn(parser::run(pool.clone()));
    tokio::spawn(follower::run(pool.clone()));
    tokio::spawn(statistic::run(pool.clone()));

    let state = AppState {
        pool,
    };
    
    let app = Router::new()
        .route("/", post(submit_keyword))
        .with_state(state);

    let listener = TcpListener::bind(format!("127.0.0.1:{}", args.port)).await.unwrap();

    axum::serve(listener, app).await.unwrap();
}

#[tracing::instrument]
async fn submit_keyword(State(state): State<AppState>, Json(request): Json<SubmitKeyword>) -> impl IntoResponse {
    match word::add(state.pool, &request.keyword, None, 0, WordStatus::WaitingForClassification).await {
        Ok(was_added) => {
            if was_added {
                info!("Added new input {}", request.keyword);
                StatusCode::OK
            } else {
                info!("Rejected duplicate new input {}", request.keyword);
                StatusCode::CONFLICT
            }
        },
        Err(err) => {
            warn!("Error while submitting keyword: {} (source: {:?})", err, err.source());
            StatusCode::INTERNAL_SERVER_ERROR
        },
    }
    
}

