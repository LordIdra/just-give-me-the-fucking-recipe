use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use recipe_common::recipe::{self, Recipe};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::AppState;

#[derive(Debug, Deserialize, ToSchema)]
pub struct SearchRequest {
    #[schema(example = "parmesan")]
    term: String,
}

#[derive(Debug, Serialize, ToSchema)]
struct SearchErrorResponse {
    #[schema(example = "some error")]
    err: String,
}

#[utoipa::path(
    post,
    path = "/search",
    description = ".",
    responses(
        (status = OK, body = Recipe),
        (status = BAD_REQUEST, body = SearchErrorResponse)
    ),
)]
#[tracing::instrument(skip(state))]
pub async fn search(
    State(state): State<AppState>, 
    Json(request): Json<SearchRequest>
) -> impl IntoResponse {
    (
        StatusCode::OK,
        Json(recipe::get_recipes_by_term(state.redis_recipes, &request.term).await.into_iter().collect::<Vec<usize>>()),
    ).into_response()
}

