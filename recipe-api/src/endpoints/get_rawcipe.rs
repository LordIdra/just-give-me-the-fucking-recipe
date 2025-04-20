use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use recipe_common::rawcipe::{self, Rawcipe};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::AppState;

#[derive(Debug, Deserialize, ToSchema)]
pub struct GetRawcipeRequest {
    #[schema(example = 54)]
    id: u64,
}

#[derive(Debug, Serialize, ToSchema)]
struct GetRawcipeErrorResponse {
    #[schema(example = "some error")]
    err: String,
}

#[utoipa::path(
    post,
    path = "/get_rawcipe",
    description = "Get a rawcipe by id.",
    responses(
        (status = OK, body = Rawcipe),
        (status = BAD_REQUEST, body = GetRawcipeErrorResponse)
    ),
)]
#[tracing::instrument(skip(state))]
pub async fn get_rawcipe(
    State(state): State<AppState>, 
    Json(request): Json<GetRawcipeRequest>
) -> impl IntoResponse {
    match rawcipe::get_rawcipe(state.redis_rawcipes, request.id).await {
        Err(err) => (
            StatusCode::BAD_REQUEST, 
            Json(GetRawcipeErrorResponse { err: err.to_string() }),
        ).into_response(),

        Ok(rawcipe) => (
            StatusCode::OK,
            Json(rawcipe),
        ).into_response()
    }
}

