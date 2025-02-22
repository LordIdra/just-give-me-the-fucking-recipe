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
    description = "Parse a link's ingredient list. The link must have already been indexed.",
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
    match recipe::ingredients(state.redis_recipes, request.id).await {
        Err(err) => (
            StatusCode::BAD_REQUEST, 
            Json(ParseIngredientErrorResponse { err: err.to_string() }),
        ).into_response(),

        Ok(ingredients) => {
            let mut parsed = vec![];
            for ingredient in ingredients {
                match Ingredient::try_from(ingredient.as_str()) {
                    Err(err) => return (
                        StatusCode::BAD_REQUEST, 
                        Json(ParseIngredientErrorResponse { err: err.to_string() }),
                    ).into_response(),

                    Ok(ok) => parsed.push(ok.to_string()),
                }
            }

            (
                StatusCode::OK,
                Json(ParseIngredientSuccessResponse { ingredients: parsed })
            ).into_response()
        }
    }
}

