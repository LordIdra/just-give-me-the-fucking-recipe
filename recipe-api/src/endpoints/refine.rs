use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use recipe_common::{rawcipe::{self}, recipe::Recipe};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::AppState;

#[derive(Debug, Deserialize, ToSchema)]
pub struct RefineRecipeRequest {
    #[schema(example = 54)]
    id: u64,
}

#[derive(Debug, Serialize, ToSchema)]
struct RefineRecipeErrorResponse {
    #[schema(example = "some error")]
    err: String,
}

#[utoipa::path(
    post,
    path = "/refine",
    description = "Refine a rawcipe into a recipe.",
    responses(
        (status = OK, body = Recipe),
        (status = BAD_REQUEST, body = RefineRecipeErrorResponse)
    ),
)]
#[tracing::instrument(skip(state))]
pub async fn refine(
    State(state): State<AppState>, 
    Json(request): Json<RefineRecipeRequest>
) -> impl IntoResponse {
    let rawcipe = rawcipe::get_rawcipe(state.redis_rawcipes, request.id).await;
    if let Err(err) = rawcipe {
        return (StatusCode::BAD_REQUEST, Json(RefineRecipeErrorResponse { err: err.to_string() })).into_response()
    }
    let rawcipe = rawcipe.unwrap();

    let recipe = Recipe::from_rawcipe(rawcipe);

    (StatusCode::OK, Json(recipe)).into_response()
}

