use clap::Parser;
use endpoints::get_links::get_links;
use endpoints::get_rawcipe::get_rawcipe;
use endpoints::refine::refine;
use endpoints::{parse_ingredients::parse_ingredients, submit_link::submit_link};
use log::info;
use redis::aio::MultiplexedConnection;
use tokio::net::TcpListener;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};
use utoipa::OpenApi;
use utoipa_axum::{router::OpenApiRouter, routes};
use utoipa_redoc::{Redoc, Servable};
use crate::endpoints::get_links::__path_get_links;
use crate::endpoints::get_rawcipe::__path_get_rawcipe;
use crate::endpoints::parse_ingredients::__path_parse_ingredients;
use crate::endpoints::refine::__path_refine;
use crate::endpoints::submit_link::__path_submit_link;

pub mod endpoints;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    port: usize,
    #[arg(long)]
    redis_links_url: String,
    #[arg(long)]
    redis_rawcipes_url: String,
}

#[derive(Debug, Clone)]
pub struct AppState {
    #[allow(unused)]
    redis_links: MultiplexedConnection,
    #[allow(unused)]
    redis_rawcipes: MultiplexedConnection,
}

#[tokio::main]
async fn main() {
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_line_number(true)
        .with_filter(EnvFilter::new("recipe_api=trace,recipe_common=trace,axum=trace"));

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

    let redis_rawcipes = redis::Client::open(args.redis_rawcipes_url)
        .unwrap()
        .get_multiplexed_tokio_connection()
        .await
        .unwrap();

    let state = AppState {
        redis_links,
        redis_rawcipes,
    };

    let api_router = OpenApiRouter::new()
        .routes(routes!(get_links))
        .routes(routes!(get_rawcipe))
        .routes(routes!(parse_ingredients))
        .routes(routes!(refine))
        .routes(routes!(submit_link))
        .with_state(state);


    #[derive(OpenApi)]
    pub struct ApiDocs;

    let (main_router, api) = OpenApiRouter::with_openapi(ApiDocs::openapi())
        .nest("/api/v1", api_router)
        .split_for_parts();
    let main_router = main_router.merge(Redoc::with_url("/docs", api.clone()));

    let listener = TcpListener::bind(format!("127.0.0.1:{}", args.port)).await.unwrap();

    axum::serve(listener, main_router.into_make_service()).await.unwrap();
}

