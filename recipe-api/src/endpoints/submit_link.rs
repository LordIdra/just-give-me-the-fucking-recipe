use axum::{extract::State, response::IntoResponse, Json};
use serde::Deserialize;

use crate::AppState;

#[derive(Debug, Deserialize)]
struct SubmitLink {
    keyword: String,
}

#[tracing::instrument(skip(state))]
pub async fn submit_link(State(state): State<AppState>, Json(request): Json<SubmitLink>) -> impl IntoResponse {
    state.redis_links

    match word::add(state.redis_words, &request.keyword, None, 0.0, WordStatus::WaitingForClassification).await {
        Ok(was_added) => {
            if was_added {
                trace!("Added new input {}", request.keyword);
                StatusCode::OK
            } else {
                trace!("Rejected duplicate new input {}", request.keyword);
                StatusCode::CONFLICT
            }
        },
        Err(err) => {
            warn!("Error while submitting keyword: {} (source: {:?})", err, err.source());
            StatusCode::INTERNAL_SERVER_ERROR
        },
    }
}
