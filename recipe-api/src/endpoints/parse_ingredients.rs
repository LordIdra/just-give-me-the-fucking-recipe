use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use recipe_common::recipe;
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
    description = "Parse a recipe's ingredient list.",
    responses(
        (status = OK, body = ParseIngredientSuccessResponse),
        (status = BAD_REQUEST, body = ParseIngredientErrorResponse)
    ),
)]
#[tracing::instrument(skip(state))]
pub async fn parse_ingredients(
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
            Json(ParseIngredientSuccessResponse { ingredients: recipe.ingredients }),
        ).into_response()
    }
}

