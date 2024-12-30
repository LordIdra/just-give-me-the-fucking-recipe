use axum::{extract::State, response::IntoResponse, routing::post, Json, Router};
use clap::Parser;
//use link::searcher;
use log::info;
use reqwest::StatusCode;
use serde::Deserialize;
use tokio::net::TcpListener;
use tokio_rusqlite::Connection;
use word::{classifier, generator, WordStatus};

mod gpt;
//mod link;
mod word;

#[derive(Debug, Deserialize)]
struct SubmitKeyword {
    keyword: String,
}

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    port: usize,
}

#[derive(Debug, Clone)]
struct AppState {
    connection: Connection,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    info!("Starting...");

    let args = Args::parse();

    let connection = Connection::open("database.db").await.unwrap();

    word::reset_tasks(connection.clone()).await;
    //link::reset_tasks(connection.clone()).await;

    {
        let connection = connection.clone();
        tokio::spawn(async move {
            classifier::start(connection).await;
        });
    }

    {
        let connection = connection.clone();
        tokio::spawn(async move {
            generator::start(connection).await;
        });
    }

    //{
    //    let connection = connection.clone();
    //    tokio::spawn(async move {
    //        searcher::start(connection).await;
    //    });
    //}

    let state = AppState {
        connection,
    };
    
    let app = Router::new()
        .route("/", post(submit_keyword))
        .with_state(state);

    let listener = TcpListener::bind(format!("127.0.0.1:{}", args.port)).await.unwrap();

    axum::serve(listener, app).await.unwrap();
}

async fn submit_keyword(State(state): State<AppState>, Json(request): Json<SubmitKeyword>) -> impl IntoResponse {
    if word::add(state.connection.clone(), &request.keyword, None, 0, WordStatus::WaitingForClassification).await {
        info!("Added new input {}", request.keyword);
        StatusCode::OK
    } else {
        info!("Rejected duplicate new input {}", request.keyword);
        StatusCode::CONFLICT
    }
}

