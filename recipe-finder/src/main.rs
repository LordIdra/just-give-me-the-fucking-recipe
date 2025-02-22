use std::fmt;
use std::time::Duration;
use std::{error::Error, fs::File};
use std::io::Read;

use clap::Parser;
use log::info;
use reqwest::{Certificate, StatusCode};
use sqlx::mysql::MySqlPoolOptions;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;
use tracing_subscriber::{layer::SubscriberExt, EnvFilter};

mod link;
mod statistic;

#[derive(Debug)]
pub struct UnexpectedStatusCodeErr(StatusCode);

impl fmt::Display for UnexpectedStatusCodeErr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Unexpected status code '{}'", self.0)
    }
}

impl Error for UnexpectedStatusCodeErr {}

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    proxy: String,
    #[arg(long)]
    crt_file: String,
    #[arg(long)]
    mysql_url: String,
    #[arg(long)]
    redis_links_url: String,
    #[arg(long)]
    redis_recipes_url: String,
}

#[tokio::main]
async fn main() {
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_line_number(true);
        //.with_filter(EnvFilter::new("recipe-finder=trace,recipe-common=trace"));

    tracing_subscriber::registry()
        .with(fmt_layer)
        .init();

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
        .connect(&args.mysql_url)
        .await
        .expect("Failed to connect to database");

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

    recipe_common::link::reset_tasks(redis_links.clone()).await.expect("Failed to reset link tasks");

    tokio::spawn(link::run(redis_links.clone(), redis_recipes.clone(), args.proxy, certificates));
    // await to prevent program from exiting
    let _ = tokio::spawn(statistic::run(redis_links.clone(), redis_recipes.clone(), mysql)).await;
}

