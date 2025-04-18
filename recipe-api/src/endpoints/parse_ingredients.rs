use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use ingredient::Ingredient;
use recipe_common::recipe;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::AppState;

#[derive(Debug, Deserialize, ToSchema)]
pub struct ParseIngredientsRequest {
    #[schema(example = 54)]
    id: u64,
}

#[derive(Debug, Serialize, ToSchema)]
struct ParseIngredientsAmount {
    raw: String,
    value: String,
    upper_value: Option<String>,
    unit: String,
}

#[derive(Debug, Serialize, ToSchema)]
struct ParseIngredientsIngredient {
    original: String,
    raw: String,
    name: String,
    amounts: Vec<ParseIngredientsAmount>,
    modifier: Option<String>
}

#[derive(Debug, Serialize, ToSchema)]
struct ParseIngredientsSuccessResponse {
    ingredients: Vec<ParseIngredientsIngredient>,
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
        (status = OK, body = ParseIngredientsSuccessResponse),
        (status = BAD_REQUEST, body = ParseIngredientErrorResponse)
    ),
)]
#[tracing::instrument(skip(state))]
pub async fn parse_ingredients(
    State(state): State<AppState>, 
    Json(request): Json<ParseIngredientsRequest>
) -> impl IntoResponse {
    match recipe::get_recipe(state.redis_recipes, request.id).await {
        Err(err) => (
            StatusCode::BAD_REQUEST, 
            Json(ParseIngredientErrorResponse { err: err.to_string() }),
        ).into_response(),

        Ok(recipe) => {
            let parsed_ingredients: Result<Vec<Ingredient>, _> = recipe.ingredients.iter()
                .map(|v| Ingredient::try_from(v.as_str()))
                .collect();

            match parsed_ingredients {
                Err(err) => (
                    StatusCode::BAD_REQUEST, 
                    Json(ParseIngredientErrorResponse { err: err.to_string() }),
                ).into_response(),

                Ok(parsed_ingredients) => {

                    let mut formatted_ingredients = vec![];
                    for (parsed_ingredient, original_ingredient) in parsed_ingredients.iter().zip(recipe.ingredients) {

                        let mut formatted_amounts = vec![];
                        for amount in &parsed_ingredient.amounts {
                            let amount = amount.normalize();
                            let (value, upper_value) = amount.values();
                            formatted_amounts.push(ParseIngredientsAmount { 
                                raw: amount.to_string(),
                                value: value.to_string(),
                                upper_value: upper_value.map(|v| v.to_string()),
                                unit: amount.unit().to_str(),
                            });
                        }

                        formatted_ingredients.push(ParseIngredientsIngredient {
                            original: original_ingredient,
                            raw: parsed_ingredient.to_string(),
                            name: parsed_ingredient.name.clone(),
                            amounts: formatted_amounts,
                            modifier: parsed_ingredient.modifier.clone(),
                        });
                    }

                    (
                        StatusCode::OK,
                        Json(ParseIngredientsSuccessResponse { ingredients: formatted_ingredients }),
                    ).into_response()
                }
            }
        }
    }
}

