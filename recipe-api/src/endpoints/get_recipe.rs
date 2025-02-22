use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use recipe_common::recipe::{self, Recipe};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::AppState;

#[derive(Debug, Deserialize, ToSchema)]
pub struct GetRecipeRequest {
    #[schema(example = 54)]
    id: u64,
}

#[derive(Debug, Serialize, ToSchema)]
struct GetRecipeErrorResponse {
    #[schema(example = "some error")]
    err: String,
}

#[utoipa::path(
    post,
    path = "/get_recipe",
    description = "Get a recipe by id.",
    responses(
        (status = OK, body = Recipe),
        (status = BAD_REQUEST, body = GetRecipeErrorResponse)
    ),
)]
#[tracing::instrument(skip(state))]
pub async fn get_recipe(
    State(state): State<AppState>, 
    Json(request): Json<GetRecipeRequest>
) -> impl IntoResponse {
    match recipe::get_recipe(state.redis_recipes, request.id).await {
        Err(err) => (
            StatusCode::BAD_REQUEST, 
            Json(GetRecipeErrorResponse { err: err.to_string() }),
        ).into_response(),

        Ok(recipe) => (
            StatusCode::OK,
            Json(recipe),
        ).into_response()
    }
}

