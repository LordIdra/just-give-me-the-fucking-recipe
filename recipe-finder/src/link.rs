use std::{sync::Arc, time::Duration};

use anyhow::Error;
use log::{debug, info, trace, warn};
use recipe_common::{link::{self, LinkMissingDomainError, LinkStatus}, recipe::{self, Recipe}};
use redis::aio::MultiplexedConnection;
use reqwest::{Certificate, Client, ClientBuilder, Proxy};
use serde_json::Value;
use tokio::{sync::Semaphore, time::interval};

pub mod downloader;
pub mod extractor;
pub mod follower;
pub mod parser;

#[tracing::instrument(skip(redis_links, client))]
pub async fn process_download(
    redis_links: MultiplexedConnection, 
    client: Client, 
    link: String
) -> Result<String, Error> {
    match downloader::download(redis_links.clone(), client, link.clone()).await {
        Err(err) => {
            link::update_status(redis_links.clone(), &link, LinkStatus::DownloadFailed).await?;
            return Err(err)
        },
        Ok(downloaded) => Ok(downloaded),
    }
}

#[tracing::instrument(skip(redis_links, contents))]
pub async fn process_extract(
    redis_links: MultiplexedConnection, 
    contents: String,
    link: String
) -> Result<Option<Value>, Error> {
    let extracted = extractor::extract(&link, &contents).await;

    if let Err(err) = extracted {
        link::update_status(redis_links.clone(), &link, LinkStatus::ExtractionFailed).await?;
        link::set_content_size(redis_links.clone(), &link, contents.len()).await?;
        return Err(err);
    }

    let extracted = extracted.unwrap();

    if extracted.is_none() {
        link::update_status(redis_links.clone(), &link, LinkStatus::ExtractionFailed).await?;
        link::set_content_size(redis_links.clone(), &link, contents.len()).await?;
        return Ok(None);
    }

    Ok(extracted)
}

#[tracing::instrument(skip(redis_links, redis_recipes, schema))]
pub async fn process_parse(
    redis_links: MultiplexedConnection, 
    redis_recipes: MultiplexedConnection, 
    schema: Value,
    link: String
) -> Result<Option<Recipe>, Error> {

    let parsed = parser::parse(link.to_string(), schema).await;

    let Some(parsed) = parsed else {
        trace!("Failed to parse recipe from {}", link);
        link::update_status(redis_links.clone(), &link, LinkStatus::ParsingFailed).await?;
        return Ok(None);
    };


    link::update_status(redis_links.clone(), &link, LinkStatus::Processed).await?;
    recipe::add(redis_recipes, parsed.clone()).await?;

    trace!("Parsed recipe from {}", link);

    Ok(Some(parsed))
}

#[tracing::instrument(skip(redis_links, contents, recipe))]
pub async fn process_follow(
    redis_links: MultiplexedConnection, 
    contents: String,
    recipe: Option<Recipe>,
    link: String
) -> Result<(), Error> {
    let recipe_exists = recipe.as_ref().is_some_and(|recipe| !recipe.ingredients.is_empty());
    let recipe_is_complete = recipe.as_ref().is_some_and(|recipe| recipe.is_complete());

    // Remaining follows
    let remaining_follows = link::get_remaining_follows(redis_links.clone(), &link).await?;
    if remaining_follows <= 0 && !recipe_is_complete {
        trace!("Terminated follow for {}", link);
        return Ok(())
    }

    // Priority
    let new_priority = if recipe_is_complete {
        0.0
    } else if recipe_exists {
        -1.0
    } else {
        -2.0
    };

    // Following logic finally
    let new_links = follower::follow(contents, link.to_string()).await;

    let mut added_links = vec![];
    for new_link in &new_links {
        let new_remaining_follows = if recipe_exists {
            1
        } else {
            remaining_follows - 1
        };
        let added = match link::add(redis_links.clone(), new_link, Some(&link), new_priority, new_remaining_follows).await {
            Ok(ok) => ok,
            // don't return if the link is missing a domain
            Err(err) => match err.downcast_ref::<LinkMissingDomainError>() {
                Some(_) => false,
                None => return Err(err),
            }
        };
        if added {
            added_links.push(new_link) 
        }
    }

    trace!("Followed {}/{} links from {}: {:?}", added_links.len(), new_links.len(), link, &added_links);

    Ok(())
}

#[tracing::instrument(skip(redis_links, redis_recipes, client, semaphore))]
pub async fn process(
    redis_links: MultiplexedConnection, 
    redis_recipes: MultiplexedConnection, 
    client: Client, 
    semaphore: Arc<Semaphore>, 
    link: String
) {
    let _permit = semaphore.acquire().await.unwrap();

    // Download
    let downloaded = process_download(redis_links.clone(), client, link.clone()).await;
    if let Err(err) = downloaded {
        debug!("Error downloading {}: {} (source: {:?})", &link, err, err.source());
        return;
    }
    let downloaded = downloaded.unwrap();

    // Extract
    let extracted = process_extract(redis_links.clone(), downloaded.clone(), link.clone()).await;
    if let Err(err) = extracted  {
        warn!("Error extracting {}: {} (source: {:?})", &link, err, err.source());
        return;
    }
    let extracted = extracted.unwrap();

    // Parse
    // (can't use map due to async closures being unstable)
    let parsed = match extracted {
        Some(extracted) => {
            let parsed = process_parse(redis_links.clone(), redis_recipes, extracted, link.clone()).await;
            if let Err(err) = parsed  {
                warn!("Error parsing {}: {} (source: {:?})", &link, err, err.source());
                return;
            }
            parsed.unwrap()
        }
        None => None
    };

    // Follow
    let followed = process_follow(redis_links.clone(), downloaded, parsed, link.clone()).await;
    if let Err(err) = followed  {
        warn!("Error following {}: {} (source: {:?})", &link, err, err.source());
        return;
    }
}

pub async fn run(
    redis_links: MultiplexedConnection, 
    redis_recipes: MultiplexedConnection, 
    proxy: String, 
    certificates: Vec<Certificate>
) {
    info!("Started processor");

    let mut builder = ClientBuilder::new()
        .proxy(Proxy::https(proxy).unwrap());
    
    for certificate in certificates {
        builder = builder.add_root_certificate(certificate);
    }
    
    let client = builder.build().unwrap();
    let semaphore = Arc::new(Semaphore::new(4096));
    let mut interval = interval(Duration::from_millis(500));

    loop {
        interval.tick().await;
        
        if semaphore.available_permits() == 0 {
            continue;
        }

        let links_result = link::poll_next_jobs(redis_links.clone(), semaphore.available_permits()).await;
        if let Err(err) = links_result {
            warn!("Error while getting next job: {} (source: {:?})", err, err.source());
            continue;
        }

        for link in links_result.unwrap() {
            tokio::spawn(process(redis_links.clone(), redis_recipes.clone(), client.clone(), semaphore.clone(), link));
        }
    }
}

