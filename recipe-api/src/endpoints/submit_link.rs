use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use recipe_common::link::{self};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::AppState;

fn priority_default() -> f32 {
    0.0
}

fn remaining_follows_default() -> i32 {
    2
}

#[derive(Debug, Deserialize, ToSchema, IntoParams)]
pub struct SubmitLinkRequest {
    #[schema(example = "https://www.indianhealthyrecipes.com/cauliflower-curry-recipe/")]
    link: String,
    #[serde(default = "priority_default")]
    #[schema(default = 0.0)]
    priority: f32,
    #[serde(default = "remaining_follows_default")]
    #[schema(default = 2)]
    remaining_follows: i32,
}

#[derive(Debug, Serialize, ToSchema)]
struct SubmitLinkSuccessResponse {
    added: bool,
}

#[derive(Debug, Serialize, ToSchema)]
struct SubmitLinkErrorResponse {
    #[schema(example = "some error")]
    err: String,
}

#[utoipa::path(
    post,
    path = "/submit_link",
    description = "Add a link to the waiting queue, with an optional priority and optional remaining follows (how deep we should follow any links on the page)",
    params(SubmitLinkRequest),
    responses(
        (status = OK, body = SubmitLinkSuccessResponse),
        (status = BAD_REQUEST, body = SubmitLinkErrorResponse)
    ),
)]
#[tracing::instrument(skip(state))]
pub async fn submit_link(
    State(state): State<AppState>, 
    Json(request): Json<SubmitLinkRequest>
) -> impl IntoResponse {
    match link::add(state.redis_links, &request.link, None, request.priority, request.remaining_follows).await {
        Err(err) => (
            StatusCode::BAD_REQUEST, 
            Json(SubmitLinkErrorResponse { err: err.to_string() }),
        ).into_response(),

        Ok(added) => (
            StatusCode::OK,
            Json(SubmitLinkSuccessResponse { added }),
        ).into_response(),
    }
}

