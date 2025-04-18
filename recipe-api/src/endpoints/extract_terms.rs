use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use recipe_common::recipe::{self, Recipe};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::AppState;

#[derive(Debug, Deserialize, ToSchema)]
pub struct ExtractTermsRequest {
    #[schema(example = 54)]
    id: u64,
}

#[derive(Debug, Serialize, ToSchema)]
struct ExtractTermsErrorResponse {
    #[schema(example = "some error")]
    err: String,
}

#[utoipa::path(
    post,
    path = "/extract_terms",
    description = ".",
    responses(
        (status = OK, body = Recipe),
        (status = BAD_REQUEST, body = ExtractTermsErrorResponse)
    ),
)]
#[tracing::instrument(skip(state))]
pub async fn extract_terms(
    State(state): State<AppState>, 
    Json(request): Json<ExtractTermsRequest>
) -> impl IntoResponse {
    match recipe::get_recipe(state.redis_recipes, request.id).await {
        Err(err) => (
            StatusCode::BAD_REQUEST, 
            Json(ExtractTermsErrorResponse { err: err.to_string() }),
        ).into_response(),

        Ok(recipe) => (
            StatusCode::OK,
            Json(recipe::extract_terms(&recipe).await),
        ).into_response()
    }
}

