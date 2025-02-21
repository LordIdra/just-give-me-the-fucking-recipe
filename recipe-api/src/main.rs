use axum::{routing::post, Router};
use clap::Parser;
use endpoints::submit_link::submit_link;
use log::info;
use redis::aio::MultiplexedConnection;
use serde::Deserialize;
use tokio::net::TcpListener;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

mod endpoints;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    port: usize,
    #[arg(long)]
    redis_links_url: String,
    #[arg(long)]
    redis_recipes_url: String,
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
        .with_filter(EnvFilter::new("recipe-api=trace"));

    tracing_subscriber::registry()
        .with(fmt_layer)
        .init();

    info!("Starting...");

    let args = Args::parse();

    let redis_links = redis::Client::open(args.redis_links_url)
        .unwrap()
        .get_multiplexed_tokio_connection()
        .await
        .unwrap();

    let redis_recipes = redis::Client::open(args.redis_recipes_url)
        .unwrap()
        .get_multiplexed_tokio_connection()
        .await
        .unwrap();

    let state = AppState {
        redis_links,
        redis_recipes,
    };
    
    let app = Router::new()
        .route("/submit_link", post(submit_link))
        .with_state(state);

    let listener = TcpListener::bind(format!("127.0.0.1:{}", args.port)).await.unwrap();

    axum::serve(listener, app).await.unwrap();
}

