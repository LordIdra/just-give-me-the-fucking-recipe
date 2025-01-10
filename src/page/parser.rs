use std::{error::Error, fmt, sync::Arc, time::Duration};

use chrono::{NaiveDate, NaiveDateTime};
use log::{info, warn};
use regex::Regex;
use serde_json::Value;
use sqlx::{MySql, Pool};
use tokio::{sync::Semaphore, time::interval};
use url::Url;

use crate::{page::{self, PageStatus}, recipe::{self, Nutrition, Recipe}, BoxError};

use super::Page;

#[derive(Debug)]
pub struct PageSchemaNullErr;

impl fmt::Display for PageSchemaNullErr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Page schema null; constraint violated somewhere")
    }
}

impl Error for PageSchemaNullErr {}

fn title(v: &Value) -> Option<String> {
    v.get("name")
        .and_then(|v| v.as_str())
        .map(|v| v.to_owned())
}

fn image(v: &Value) -> Vec<String> {
    let single_image = v.get("image")
        .and_then(|v| v.as_str())
        .map(|v| v.to_owned())
        .map(|v| vec![v]);

    let image_array = v.get("image")
        .and_then(|v| v.as_array())
        .map(|v| v.iter()
            .filter_map(|v| v.as_str())
            .map(|v| v.to_owned())
            .collect::<Vec<String>>()
        );

    let single_image_object = v.get("image")
        .and_then(|v| v.get("url"))
        .and_then(|v| v.as_str())
        .map(|v| vec![v.to_owned()]);

    let image_object_array = v.get("image")
        .and_then(|v| v.as_array())
        .map(|v| v.iter()
            .filter_map(|v| v.get("url"))
            .filter_map(|v| v.as_str())
            .map(|v| v.to_owned())
            .collect::<Vec<String>>()
        );

    single_image
        .or(image_array)
        .or(single_image_object)
        .or(image_object_array)
        .unwrap_or_default()
}

fn authors(v: &Value, link: String) -> Vec<String> {
    let Some(v) = v.get("author") else {
        return vec![]
    };

    let authors: Vec<String> = v.as_array()
        .cloned()
        .unwrap_or(vec![v.clone()])
        .iter()
        .filter_map(|v| v.get("name")
            .and_then(|v| v.as_str())
            .map(|v| v.to_owned())
        )
        .collect();

    if authors.is_empty() {
        let Ok(url) = Url::parse(&link) else {
            return vec![];
        };
        let Some(domain) = url.domain() else {
            return vec![];
        };
        return vec![domain.to_owned()];
    }

    authors
}

fn description(v: &Value) -> Option<String> {
    v.get("description")
        .and_then(|v| v.as_str())
        .map(|v| v.to_owned())
}

fn date(v: &Value) -> Option<NaiveDate> {
    let mut date_str = v.get("datePublished")
        .or(v.get("dateCreated"))?
        .as_str()?
        .to_owned();

    // converts eg 2009-09-06T20:07Z (not a valid ISO8601 apparently?)
    // into 2009-09-06T20:07:00Z
    // Yes this is ridiculous
    if Regex::new(r".{0,100}T\d\d:\d\dZ").unwrap().is_match(&date_str) {
        date_str = date_str.replace("Z", ":00Z");
    }

    // Covers almost all websites
    let format1 = dateparser::parse(&date_str)
        .ok()
        .map(|v| v.date_naive());

    // https://www.kingarthurbaking.com/recipes/muffaletta-recipe
    let format2 = NaiveDateTime::parse_from_str(&date_str, "%B %d, %Y at %I:%M%p")
        .ok()
        .map(|v| v.date());

    // https://www.food.com/recipe/authentic-german-black-bread-schwarzbrot-446730
    let format3 = NaiveDateTime::parse_from_str(&date_str, "%B %d, %Y")
        .ok()
        .map(|v| v.date());

    format1
        .or(format2)
        .or(format3)
}

fn servings(v: &Value) -> Option<String> {
    let v = v.get("recipeYield");

    // recipeYield: ["1 serving", ...]
    let array_text = v
        .and_then(|v| v.as_array())
        .and_then(|v| v.iter()
            .find_map(|v| v.as_str()
                .map(|v| v.to_owned())
                .take_if(|v| v.parse::<i32>().is_err())
            )
        );

    // recipeYield: ["1", ...]
    let array_wrapped_number = v
        .and_then(|v| v.as_array())
        .and_then(|v| v.iter()
            .find_map(|v| v.as_str()
                .map(|v| v.to_owned())
                .take_if(|v| v.parse::<i32>().is_ok())
            )
        );
    
    // recipeYield: [1, ...]
    let array_number = v
        .and_then(|v| v.as_array())
        .and_then(|v| v.iter()
            .find_map(|v| v.as_i64()
                .map(|v| v.to_string())
            )
        );  

    // recipeYield: "1 serving"
    let text = v
        .and_then(|v| v.as_str())
        .map(|v| v.to_owned())
        .take_if(|v| v.parse::<i32>().is_err());

    // recipeYield: "1"
    let text_wrapped_number = v
        .and_then(|v| v.as_str())
        .map(|v| v.to_owned())
        .take_if(|v| v.parse::<i32>().is_ok());

    // recipeYield: 1
    let text_number = v
        .and_then(|v| v.as_i64())
        .map(|v| v.to_string());

    array_text
        .or(text)
        .or(array_wrapped_number)
        .or(text_wrapped_number)
        .or(array_number)
        .or(text_number)
}

fn prep_time(v: &Value) -> Option<Duration> {
    v.get("prepTime")
        .and_then(|v| v.as_str())
        .and_then(|v| iso8601::duration(v).ok())
        .map(|v| v.into())
}

fn cook_time(v: &Value) -> Option<Duration> {
    v.get("cookTime")
        .and_then(|v| v.as_str())
        .and_then(|v| iso8601::duration(v).ok())
        .map(|v| v.into())
}

fn total_time(v: &Value) -> Option<Duration> {
    v.get("totalTime")
        .and_then(|v| v.as_str())
        .and_then(|v| iso8601::duration(v).ok())
        .map(|v| v.into())
}

fn ingredients(v: &Value) -> Vec<String> {
    v.get("recipeIngredient")
        .and_then(|v| v.as_array())
        .and_then(|v| v.iter()
            .map(|v| v.as_str().map(|v| v.to_owned()))
            .collect::<Option<Vec<String>>>()
        )
        .unwrap_or_default()
}

fn rating(v: &Value) -> Option<f32> {
    v.get("aggregateRating")
        .and_then(|v| {
            v.get("ratingValue")
        })
        .and_then(|v| v.as_str())
        .and_then(|v| v.parse::<f32>().ok())
}

fn rating_count(v: &Value) -> Option<i32> {
    let ratings = v.get("aggregateRating")
        .and_then(|v| v.get("ratingCount"))
        .and_then(|v| v.as_str())
        .and_then(|v| v.parse::<i32>().ok());

    let reviews = v.get("aggregateRating")
        .and_then(|v| v.get("reviewCount"))
        .and_then(|v| v.as_str())
        .and_then(|v| v.parse::<i32>().ok());

    match ratings {
        Some(ratings) => reviews
            .map(|v| v + ratings)
            .or(Some(ratings)),
        None => reviews,
    }
}

fn keywords(v: &Value) -> Vec<String> {
    let mut keywords = v.get("keywords")
        .and_then(|v| v.as_str())
        .map(|v| v.split(",")
            .map(|v| v.trim())
            .map(|v| v.to_owned())
            .collect::<Vec<String>>()
        )
        .unwrap_or_default();

    let category = v.get("recipeCategory")
        .and_then(|v| v.as_array())
        .and_then(|v| v.iter()
            .map(|v| v.as_str()
                .map(|v| v.trim())
                .map(|v| v.to_owned())
            )
            .collect::<Option<Vec<String>>>()
        )
        .unwrap_or_default();
    
    let cuisine = v.get("recipeCuisine")
        .and_then(|v| v.as_array())
        .and_then(|v| v.iter()
            .map(|v| v.as_str()
                .map(|v| v.trim())
                .map(|v| v.to_owned())
            )
            .collect::<Option<Vec<String>>>()
        )
        .unwrap_or_default();

    keywords.extend(category);
    keywords.extend(cuisine);

    keywords.sort();
    keywords.dedup();

    keywords
}

fn nutrition(v: &Value) -> Nutrition {
    let Some(v) = v.get("nutrition") else {
        return Nutrition::default()
    };

    Nutrition {
        calories: v.get("calories")
            .and_then(|v| v.as_str())
            .map(|v| v.replace("kcal", ""))
            .map(|v| v.replace("calories", ""))
            .map(|v| v.trim().to_owned())
            .and_then(|v| v.parse::<f32>().ok()),
        carbohydrates: v.get("carbohydrateContent")
            .and_then(|v| v.as_str())
            .map(|v| v.replace("g", ""))
            .map(|v| v.trim().to_owned())
            .and_then(|v| v.parse::<f32>().ok()),
        cholesterol: v.get("cholesterolContent")
            .and_then(|v| v.as_str())
            .map(|v| v.replace("mg", ""))
            .map(|v| v.trim().to_owned())
            .and_then(|v| v.parse::<f32>().ok()),
        fat: v.get("fatContent")
            .and_then(|v| v.as_str())
            .map(|v| v.replace("g", ""))
            .map(|v| v.trim().to_owned())
            .and_then(|v| v.parse::<f32>().ok()),
        fiber: v.get("fiberContent")
            .and_then(|v| v.as_str())
            .map(|v| v.replace("g", ""))
            .map(|v| v.trim().to_owned())
            .and_then(|v| v.parse::<f32>().ok()),
        protein: v.get("proteinContent")
            .and_then(|v| v.as_str())
            .map(|v| v.replace("g", ""))
            .map(|v| v.trim().to_owned())
            .and_then(|v| v.parse::<f32>().ok()),
        saturated_fat: v.get("saturatedFatContent")
            .and_then(|v| v.as_str())
            .map(|v| v.replace("g", ""))
            .map(|v| v.trim().to_owned())
            .and_then(|v| v.parse::<f32>().ok()),
        sodium: v.get("sodiumContent")
            .and_then(|v| v.as_str())
            .map(|v| v.replace("mg", ""))
            .map(|v| v.trim().to_owned())
            .and_then(|v| v.parse::<f32>().ok()),
        sugar: v.get("sugarContent")
            .and_then(|v| v.as_str())
            .map(|v| v.replace("g", ""))
            .map(|v| v.trim().to_owned())
            .and_then(|v| v.parse::<f32>().ok()),
    }
}

#[tracing::instrument(skip(pool, page), fields(id = page.id))]
async fn parse(pool: Pool<MySql>, page: Page) -> Result<(), BoxError> {
    let Some(schema) = page.schema else {
        return Err(Box::new(PageSchemaNullErr))
    };

    let v: Value = serde_json::from_str(&schema)
        .map_err(|err| Box::new(err) as BoxError)?;

    let recipe = Recipe {
        page: page.id,
        title: title(&v),
        images: image(&v),
        authors: authors(&v, page.link.to_owned()),
        description: description(&v),
        date: date(&v),
        servings: servings(&v),
        prep_time_seconds: prep_time(&v),
        cook_time_seconds: cook_time(&v),
        total_time_seconds: total_time(&v).or_else(|| Some(prep_time(&v)? + cook_time(&v)?)),
        ingredients: ingredients(&v),
        rating: rating(&v),
        rating_count: rating_count(&v),
        keywords: keywords(&v),
        nutrition: nutrition(&v),
    };

    let is_complete = recipe.is_complete();

    recipe::add(pool.clone(), recipe).await?;

    if is_complete {
        info!("Parsed complete recipe from {} (page {})", page.link, page.id);
        page::set_status(pool.clone(), page.id, PageStatus::WaitingForFollowing).await?;
        page::set_schema(pool, page.id, None).await?;
    } else {
        info!("Parsed incomplete recipe from {} (page {})", page.link, page.id);
        page::set_status(pool.clone(), page.id, PageStatus::ParsingIncompleteRecipe).await?;
        page::set_schema(pool, page.id, None).await?;
    }

    Ok(())
}

pub async fn run(pool: Pool<MySql>) {
    info!("Started parser");

    let semaphore = Arc::new(Semaphore::new(256));

    let mut interval = interval(std::time::Duration::from_millis(500));

    loop {
        loop {
            if semaphore.available_permits() == 0 {
                break
            }

            let next_job = page::next_job(pool.clone(), PageStatus::WaitingForParsing, PageStatus::Parsing).await;
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
                if let Err(err) = parse(pool.clone(), state.clone()).await {
                    warn!("Parser encountered error on page #{} ('{}'): {} (source: {:?})", state.id, state.link, err, err.source());
                    if let Err(err) = page::set_status(pool.clone(), state.id, PageStatus::ParsingIncompleteRecipe).await {
                        warn!("Error while setting status to failed on page #{} ('{}')@ {} (source: {:?})", state.id, state.link, err, err.source());
                    }
                    if let Err(err) = page::set_schema(pool, state.id, None).await {
                        warn!("Error while deleting content on page #{} ('{}')@ {} (source: {:?})", state.id, state.link, err, err.source());
                    }
                }
                
            });
        }

        interval.tick().await;
    }
}

