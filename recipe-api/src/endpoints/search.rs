use std::collections::HashSet;

use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use recipe_common::recipe::get_recipes_by_term;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::AppState;

#[derive(Debug, Deserialize, ToSchema)]
pub struct SearchRequest {
    #[schema(example = json!(["vegetarian", "aubergine", "cake"]))]
    terms: Vec<String>,
}

#[derive(Debug, Serialize, ToSchema)]
struct SearchErrorResponse {
    #[schema(example = "some error")]
    err: String,
}

#[derive(Debug, Serialize, ToSchema)]
struct SearchSuccessResponse {
    recipe_ids: Vec<usize>,
}

#[utoipa::path(
    post,
    path = "/search",
    description = "Search recipes.",
    responses(
        (status = OK, body = SearchSuccessResponse),
        (status = BAD_REQUEST, body = SearchErrorResponse)
    ),
)]
#[tracing::instrument(skip(state))]
pub async fn search(
    State(state): State<AppState>,
    Json(request): Json<SearchRequest>,
) -> impl IntoResponse {
    let mut recipes = HashSet::<usize>::new();
    for term in request.terms {
        recipes.extend(&get_recipes_by_term(state.redis_recipes.clone(), &term).await)
    }
    (StatusCode::OK, Json(recipes)).into_response()
}

