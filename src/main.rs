use std::fmt;
use std::time::Duration;
use std::{error::Error, fs::File};
use std::io::Read;

use axum::{extract::State, response::IntoResponse, routing::post, Json, Router};
use clap::Parser;
use log::info;
use redis::aio::MultiplexedConnection;
use reqwest::{Certificate, StatusCode};
use serde::Deserialize;
use sqlx::mysql::MySqlPoolOptions;
use tokio::net::TcpListener;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;
use tracing_subscriber::{layer::SubscriberExt, EnvFilter};

mod link;
mod link_blacklist;
mod recipe;
mod statistic;

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
pub struct AppState {
    #[allow(unused)]
    redis_links: MultiplexedConnection,
    #[allow(unused)]
    redis_recipes: MultiplexedConnection,
}

#[tokio::main]
async fn main() {
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_line_number(true)
        .with_filter(EnvFilter::new("just_give_me_the_fucking_recipe=trace"));

    let subscriber = tracing_subscriber::registry()
        .with(fmt_layer);
    #[cfg(feature = "profiling")]
    let subscriber = subscriber.with(tracing_tracy::TracyLayer::default());
    subscriber.init();

    info!("Starting...");

    let args = Args::parse();

    let mut buf = vec![];
    File::open(args.crt_file)
        .unwrap()
        .read_to_end(&mut buf)
        .unwrap();

    let certificates = Certificate::from_pem_bundle(&buf).unwrap();

    let mysql = MySqlPoolOptions::new()
        .test_before_acquire(true)
        .max_connections(200)
        .acquire_timeout(Duration::from_secs(2))
        .connect(&args.database_url)
        .await
        .expect("Failed to connect to database");

    let redis_links = redis::Client::open("redis://127.0.0.1:6381")
        .unwrap()
        .get_multiplexed_tokio_connection()
        .await
        .unwrap();

    let redis_recipes = redis::Client::open("redis://127.0.0.1:6382")
        .unwrap()
        .get_multiplexed_tokio_connection()
        .await
        .unwrap();

    link::reset_tasks(redis_links.clone()).await.expect("Failed to reset link tasks");

    tokio::spawn(link::run(redis_links.clone(), redis_recipes.clone(), args.proxy, certificates));
    tokio::spawn(statistic::run(redis_links.clone(), redis_recipes.clone(), mysql));

    let state = AppState {
        redis_links,
        redis_recipes,
    };
    
    let app = Router::new()
        .route("/", post(submit_keyword))
        .with_state(state);

    let listener = TcpListener::bind(format!("127.0.0.1:{}", args.port)).await.unwrap();

    axum::serve(listener, app).await.unwrap();
}

#[tracing::instrument(skip(state))]
async fn submit_keyword(State(state): State<AppState>, Json(request): Json<SubmitKeyword>) -> impl IntoResponse {
    todo!();
    //match word::add(state.redis_words, &request.keyword, None, 0.0, WordStatus::WaitingForClassification).await {
    //    Ok(was_added) => {
    //        if was_added {
    //            trace!("Added new input {}", request.keyword);
    //            StatusCode::OK
    //        } else {
    //            trace!("Rejected duplicate new input {}", request.keyword);
    //            StatusCode::CONFLICT
    //        }
    //    },
    //    Err(err) => {
    //        warn!("Error while submitting keyword: {} (source: {:?})", err, err.source());
    //        StatusCode::INTERNAL_SERVER_ERROR
    //    },
    //}
}

