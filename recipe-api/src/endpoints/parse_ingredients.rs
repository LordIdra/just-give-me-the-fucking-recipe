use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use ingredient::Ingredient;
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

        Ok(recipe) => {
            let ingredients: Result<Vec<Ingredient>, _> = recipe.ingredients.iter()
                .map(|v| Ingredient::try_from(v.as_str()))
                .collect();

            match ingredients {
                Err(err) => (
                    StatusCode::BAD_REQUEST, 
                    Json(ParseIngredientErrorResponse { err: err.to_string() }),
                ).into_response(),
                Ok(ingredients) => {
                    let ingredients = ingredients.into_iter()
                        .map(|v| v.to_string())
                        .collect();

                    (
                        StatusCode::OK,
                        Json(ParseIngredientSuccessResponse { ingredients }),
                    ).into_response()
                }
            }
        }
    }
}

