use std::{error::Error, fmt, sync::{Arc, LazyLock}, time::Duration};

use log::{info, warn};
use regex::{Regex, RegexBuilder};
use sqlx::{MySql, Pool};
use tokio::{sync::Semaphore, time::interval};
use url::Url;

use crate::{page::{self, PageStatus}, BoxError};

use super::Page;

static LINK_ELEMENT_REGEX: LazyLock<Regex> = LazyLock::new(|| 
    RegexBuilder::new(r#"<a.{0,2000}>"#)
        .build()
        .unwrap()
);

static HREF_REGEX: LazyLock<Regex> = LazyLock::new(|| 
    RegexBuilder::new(r#"href\s?=\s?"([^"]{0,500})""#)
        .build()
        .unwrap()
);

#[derive(Debug)]
pub struct PageContentNullErr;

impl fmt::Display for PageContentNullErr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Page content null; constraint violated somewhere")
    }
}

impl Error for PageContentNullErr {}

#[tracing::instrument(skip(pool, page), fields(id = page.id))]
async fn follow(pool: Pool<MySql>, page: Page) -> Result<(), BoxError> {
    let Some(content) = page.content else {
        return Err(Box::new(PageContentNullErr));
    };

    let links: Vec<&str> = LINK_ELEMENT_REGEX.captures_iter(&content)
        .map(|captures| captures.get(0).unwrap().as_str())
        .filter_map(|element| HREF_REGEX.captures(element))
            .map(|captures| captures.get(1).unwrap().as_str())
        .filter(|link| Url::parse(link).is_ok())
        // eg, bruh.com/some-recipe might have links to bruh.com/some-recipe/comments#36
        .filter(|link| !link.starts_with(&page.link))
        .collect();

    let mut added_links = vec![];
    for link in &links {
        if page::add(pool.clone(), link, None, Some(page.id), -1000, PageStatus::WaitingForDownload).await? {
            added_links.push(link);
        }
    }

    page::set_content(pool.clone(), page.id, None).await?;
    page::set_status(pool, page.id, PageStatus::FollowingComplete).await?;

    info!("Followed {}/{} links from {} (page {}): {:?}", added_links.len(), links.len(), page.link, page.id, &added_links);

    Ok(())
}

pub async fn run(pool: Pool<MySql>) {
    info!("Started follower");

    let semaphore = Arc::new(Semaphore::new(256));

    let mut interval = interval(Duration::from_millis(500));

    loop {
        interval.tick().await;

        loop {
            if semaphore.available_permits() == 0 {
                break
            }

            let next_job = page::next_job(pool.clone(), PageStatus::WaitingForFollowing, PageStatus::Following).await;
            if let Err(err) = next_job {
                warn!("Error while getting next job: {} (source: {:?})", err, err.source());
                break;
            }

            let Some(state) = next_job.unwrap() else {
                break;
            };
            
            let sempahore = semaphore.clone();
            let pool = pool.clone();

            tokio::spawn(async move {
                let _permit = sempahore.acquire().await.unwrap();
                if let Err(err) = follow(pool.clone(), state.clone()).await {
                    warn!("Follower encountered error on page #{} ('{}'): {} (source: {:?})", state.id, state.link, err, err.source());
                    if let Err(err) = page::set_status(pool.clone(), state.id, PageStatus::FollowingFailed).await {
                        warn!("Error while setting status to failed on page #{} ('{}')@ {} (source: {:?})", state.id, state.link, err, err.source());
                    }
                    if let Err(err) = page::set_content(pool, state.id, None).await {
                        warn!("Error while deleting content on page #{} ('{}')@ {} (source: {:?})", state.id, state.link, err, err.source());
                    }
                }
            });
        }
    }
}

