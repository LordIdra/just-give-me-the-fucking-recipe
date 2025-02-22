use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use ingredient::Ingredient;
use recipe_common::recipe::{self, Recipe};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::AppState;

#[derive(Debug, Deserialize, ToSchema)]
pub struct ParseIngredientRequest {
    #[schema(example = 54)]
    id: u64,
}

#[derive(Debug, Serialize, ToSchema)]
struct ParseIngredientSuccessResponse {
    id: u64,
    ingredients: Vec<String>,
}

#[derive(Debug, Serialize, ToSchema)]
struct ParseIngredientErrorResponse {
    #[schema(example = "some error")]
    err: String,
}

#[utoipa::path(
    post,
    path = "/parse_ingredients",
    description = "Parse a link's ingredient list. The link must have already been indexed.",
    responses(
        (status = OK, body = ParseIngredientSuccessResponse),
        (status = BAD_REQUEST, body = ParseIngredientErrorResponse)
    ),
)]
#[tracing::instrument(skip(state))]
pub async fn get_recipe(
    State(state): State<AppState>, 
    Json(request): Json<ParseIngredientRequest>
) -> impl IntoResponse {
    match recipe::get_recipe(state.redis_recipes, request.id).await {
        Err(err) => (
            StatusCode::BAD_REQUEST, 
            Json(ParseIngredientErrorResponse { err: err.to_string() }),
        ).into_response(),

        Ok(recipe) => (
            StatusCode::OK,
            Json(recipe),
        ).into_response()
    }
}

