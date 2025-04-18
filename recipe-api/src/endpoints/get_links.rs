use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use recipe_common::link::{self, LinkStatus};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::AppState;

#[derive(Debug, Deserialize, ToSchema)]
pub struct GetLinksRequest {
    #[schema(example = "waiting")]
    status: String,
}

#[derive(Debug, Serialize, ToSchema)]
struct GetLinksErrorResponse {
    #[schema(example = "some error")]
    err: String,
}

#[derive(Debug, Serialize, ToSchema)]
struct Link {
    link: String,
    priority: f32,
    parent: Option<String>,
}

#[utoipa::path(
    post,
    path = "/get_links",
    description = "Get links by status.",
    responses(
        (status = OK, body = Vec<Link>),
        (status = BAD_REQUEST, body = GetLinksErrorResponse)
    ),
)]
#[tracing::instrument(skip(state))]
pub async fn get_links(
    State(state): State<AppState>,
    Json(request): Json<GetLinksRequest>,
) -> impl IntoResponse {
    let Some(status) = LinkStatus::from_string(&request.status) else {
        return (StatusCode::BAD_REQUEST, Json(GetLinksErrorResponse { err: "".to_string() })).into_response()
    };

    let links = link::get_links_by_status(state.redis_links.clone(), status).await;
    if let Err(err) = links {
        return (StatusCode::BAD_REQUEST, Json(GetLinksErrorResponse { err: err.to_string() })).into_response()
    }
    let links = links.unwrap();

    let mut response_links = vec![];
    for link in links {

        let priority = link::get_priority(state.redis_links.clone(), &link).await;
        if let Err(err) = priority {
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(GetLinksErrorResponse { err: err.to_string() })).into_response()
        }
        let priority = priority.unwrap();

        let parent = link::get_parent(state.redis_links.clone(), &link).await;
        if let Err(err) = parent {
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(GetLinksErrorResponse { err: err.to_string() })).into_response()
        }
        let parent = parent.unwrap();

        response_links.push(Link { link, priority, parent });
    }

    (StatusCode::OK, Json(response_links)).into_response()
}

