use std::slice::Iter;

use anyhow::Error;
use redis::{aio::MultiplexedConnection, AsyncCommands, ErrorKind, FromRedisValue, RedisError, RedisResult, Value};
use serde::Serialize;
use utoipa::ToSchema;

#[derive(Serialize, Debug, Clone, ToSchema)]
pub struct Rawcipe {
    pub link: String,
    pub title: String,
    pub description: String,
    pub ingredients: Vec<String>,
    pub instructions: Vec<String>,
    pub date: Option<String>,
    pub keywords: Vec<String>,
    pub authors: Vec<String>,
    pub images: Vec<String>,
    pub rating: Option<f32>,
    pub rating_count: Option<i32>,
    pub prep_time_seconds: Option<u64>,
    pub cook_time_seconds: Option<u64>,
    pub total_time_seconds: Option<u64>,
    pub servings: Option<String>,
    pub calories: Option<f32>,
    pub carbohydrates: Option<f32>,
    pub cholesterol: Option<f32>,
    pub fat: Option<f32>,
    pub fiber: Option<f32>,
    pub protein: Option<f32>,
    pub saturated_fat: Option<f32>,
    pub sodium: Option<f32>,
    pub sugar: Option<f32>,
}

pub fn get_redis_value<T: FromRedisValue>(iter: &mut Iter<Value>, field: &str) -> Result<T, RedisError> {
    match iter.next().map(T::from_redis_value) {
        Some(Ok(v)) => Ok(v),
        _ => Err(RedisError::from((ErrorKind::TypeError, "Failed to get field", field.to_owned()))),
    }
}

impl FromRedisValue for Rawcipe {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        let Value::Array(ref items) = *v else {
            return Err(RedisError::from((ErrorKind::TypeError, "Expected to get iterable")));
        };
        let mut iter = items.iter();

        Ok(Rawcipe {
            link: get_redis_value(&mut iter, "link")?,
            title: get_redis_value(&mut iter, "title")?,
            description: get_redis_value(&mut iter, "description")?,
            ingredients: get_redis_value(&mut iter, "ingredients")?,
            instructions: get_redis_value(&mut iter, "instructions")?,
            date: get_redis_value(&mut iter, "date")?,
            keywords: get_redis_value(&mut iter, "keywords")?,
            authors: get_redis_value(&mut iter, "authors")?,
            images: get_redis_value(&mut iter, "images")?,
            rating: get_redis_value(&mut iter, "rating")?,
            rating_count: get_redis_value(&mut iter, "rating-count")?,
            prep_time_seconds: get_redis_value(&mut iter, "prep_time_seconds")?,
            cook_time_seconds: get_redis_value(&mut iter, "cook_time_seconds")?,
            total_time_seconds: get_redis_value(&mut iter, "total_time_seconds")?,
            servings: get_redis_value(&mut iter, "servings")?,
            calories: get_redis_value(&mut iter, "calories")?,
            carbohydrates: get_redis_value(&mut iter, "carbohydrates")?,
            cholesterol: get_redis_value(&mut iter, "cholesterol")?,
            fat: get_redis_value(&mut iter, "fat")?,
            fiber: get_redis_value(&mut iter, "fiber")?,
            protein: get_redis_value(&mut iter, "protein")?,
            saturated_fat: get_redis_value(&mut iter, "saturated_fat")?,
            sodium: get_redis_value(&mut iter, "sodium")?,
            sugar: get_redis_value(&mut iter, "sugar")?,
        })
    }
}

impl Rawcipe {
    pub fn is_complete(&self) -> bool {
        !self.images.is_empty()
            && !self.authors.is_empty()
            && self.date.is_some()
            && self.servings.is_some()
            && self.total_time_seconds.is_some()
            && !self.ingredients.is_empty()
            && self.rating.is_some()
            && self.rating_count.is_some()
            && !self.keywords.is_empty()
            && self.calories.is_some()
            && self.carbohydrates.is_some()
            && self.cholesterol.is_some()
            && self.fat.is_some()
            && self.fiber.is_some()
            && self.protein.is_some()
            && self.saturated_fat.is_some()
            && self.sodium.is_some()
            && self.sugar.is_some()
    }
}

// STRING acting as counter for next rawcipe id
fn key_id() -> String {
    "static:id".to_string()
}

// SET of all rawcipe ids
fn key_rawcipes() -> String {
    "rawcipes".to_string()
}

// SET of all rawcipes associated with a term
fn key_term_rawcipes(term: &str) -> String {
    format!("term:{term}:rawcipes")
}

// SET of all rawcipes associated with a title
fn key_title_rawcipes(title: &str) -> String {
    format!("title:{title}:titles")
}

// SET of all rawcipes associated with a description
fn key_description_rawcipes(description: &str) -> String {
    format!("description:{description}:rawcipes")
}

// STRING
fn key_rawcipe_link(id: u64) -> String {
    format!("rawcipe:{id}:link")
}

// STRING
fn key_rawcipe_title(id: u64) -> String {
    format!("rawcipe:{id}:title")
}

// STRING
fn key_rawcipe_description(id: u64) -> String {
    format!("rawcipe:{id}:description")
}

// STRING
fn key_rawcipe_date(id: u64) -> String {
    format!("rawcipe:{id}:date")
}

// STRING
fn key_rawcipe_rating(id: u64) -> String {
    format!("rawcipe:{id}:rating")
}

// STRING
fn key_rawcipe_rating_count(id: u64) -> String {
    format!("rawcipe:{id}:rating_count")
}

// STRING
fn key_rawcipe_prep_time_seconds(id: u64) -> String {
    format!("rawcipe:{id}:prep_time_seconds")
}

// STRING
fn key_rawcipe_cook_time_seconds(id: u64) -> String {
    format!("rawcipe:{id}:cook_time_seconds")
}

// STRING
fn key_rawcipe_total_time_seconds(id: u64) -> String {
    format!("rawcipe:{id}:total_time_seconds")
}

// STRING
fn key_rawcipe_servings(id: u64) -> String {
    format!("rawcipe:{id}:key_servings")
}

// STRING
fn key_rawcipe_calories(id: u64) -> String {
    format!("rawcipe:{id}:calories")
}

// STRING
fn key_rawcipe_carbohydrates(id: u64) -> String {
    format!("rawcipe:{id}:carbohydrates")
}

// STRING
fn key_rawcipe_cholesterol(id: u64) -> String {
    format!("rawcipe:{id}:cholesterol")
}

// STRING
fn key_rawcipe_fat(id: u64) -> String {
    format!("rawcipe:{id}:fat")
}

// STRING
fn key_rawcipe_fiber(id: u64) -> String {
    format!("rawcipe:{id}:fiber")
}

// STRING
fn key_rawcipe_protein(id: u64) -> String {
    format!("rawcipe:{id}:protein")
}

// STRING
fn key_rawcipe_saturated_fat(id: u64) -> String {
    format!("rawcipe:{id}:saturated_fat")
}

// STRING
fn key_rawcipe_sodium(id: u64) -> String {
    format!("rawcipe:{id}:sodium")
}

// STRING
fn key_rawcipe_sugar(id: u64) -> String {
    format!("rawcipe:{id}:sugar")
}

// LIST
fn key_rawcipe_keywords(id: u64) -> String {
    format!("rawcipe:{id}:keywords")
}

// LIST
fn key_rawcipe_authors(id: u64) -> String {
    format!("rawcipe:{id}:authors")
}

// LIST
fn key_rawcipe_images(id: u64) -> String {
    format!("rawcipe:{id}:images")
}

// LIST
fn key_rawcipe_ingredients(id: u64) -> String {
    format!("rawcipe:{id}:ingredients")
}

// LIST
fn key_rawcipe_instructions(id: u64) -> String {
    format!("rawcipe:{id}:instructions")
}

/// Returns true if added
/// Returns false if already existed or matches the blacklist
#[tracing::instrument(skip(redis_rawcipes))]
#[must_use]
pub async fn add(mut redis_rawcipes: MultiplexedConnection, rawcipe: Rawcipe) -> Result<bool, Error> {
    if exists(redis_rawcipes.clone(), &rawcipe).await? {
        return Ok(false);
    }

    let id: u64 = redis_rawcipes.incr(key_id(), 1).await?;

    let mut pipe = redis::pipe();

    pipe.sadd(key_rawcipes(), id);

    pipe.set(key_rawcipe_link(id), &rawcipe.link);

    pipe.sadd(key_title_rawcipes(&rawcipe.title), id);
    pipe.set(key_rawcipe_title(id), &rawcipe.title);

    pipe.sadd(key_description_rawcipes(&rawcipe.description), id);
    pipe.set(key_rawcipe_description(id), &rawcipe.description);

    rawcipe.date.as_ref().map(|v| pipe.set(key_rawcipe_date(id), v.to_string()));
    rawcipe.rating.as_ref().map(|v| pipe.set(key_rawcipe_rating(id), v));
    rawcipe.rating_count.as_ref().map(|v| pipe.set(key_rawcipe_rating_count(id), v));
    rawcipe.prep_time_seconds.as_ref().map(|v| pipe.set(key_rawcipe_prep_time_seconds(id), v));
    rawcipe.cook_time_seconds.as_ref().map(|v| pipe.set(key_rawcipe_cook_time_seconds(id), v));
    rawcipe.total_time_seconds.as_ref().map(|v| pipe.set(key_rawcipe_total_time_seconds(id), v));
    rawcipe.servings.as_ref().map(|v| pipe.set(key_rawcipe_servings(id), v));
    rawcipe.calories.as_ref().map(|v| pipe.set(key_rawcipe_calories(id), v));
    rawcipe.carbohydrates.as_ref().map(|v| pipe.set(key_rawcipe_carbohydrates(id), v));
    rawcipe.cholesterol.as_ref().map(|v| pipe.set(key_rawcipe_cholesterol(id), v));
    rawcipe.fat.as_ref().map(|v| pipe.set(key_rawcipe_fat(id), v));
    rawcipe.fiber.as_ref().map(|v| pipe.set(key_rawcipe_fiber(id), v));
    rawcipe.protein.as_ref().map(|v| pipe.set(key_rawcipe_protein(id), v));
    rawcipe.saturated_fat.as_ref().map(|v| pipe.set(key_rawcipe_saturated_fat(id), v));
    rawcipe.sodium.as_ref().map(|v| pipe.set(key_rawcipe_sodium(id), v));
    rawcipe.sugar.as_ref().map(|v| pipe.set(key_rawcipe_sugar(id), v));

    if !rawcipe.keywords.is_empty() {
        pipe.cmd("lpush").arg(key_rawcipe_keywords(id));
        for keyword in rawcipe.keywords.iter().rev() {
            pipe.arg(keyword);
        }
    }

    if !rawcipe.authors.is_empty() {
        pipe.cmd("lpush").arg(key_rawcipe_authors(id));
        for author in rawcipe.authors.iter().rev() {
            pipe.arg(author);
        }
    }

    if !rawcipe.images.is_empty() {
        pipe.cmd("lpush").arg(key_rawcipe_images(id));
        for image in rawcipe.images.iter().rev() {
            pipe.arg(image);
        }
    }

    if !rawcipe.ingredients.is_empty() {
        pipe.cmd("lpush").arg(key_rawcipe_ingredients(id));
        for ingredient in rawcipe.ingredients.iter().rev() {
            pipe.arg(ingredient);
        }
    }

    if !rawcipe.instructions.is_empty() {
        pipe.cmd("lpush").arg(key_rawcipe_instructions(id));
        for instruction in rawcipe.instructions.iter().rev() {
            pipe.arg(instruction);
        }
    }
    
    for term in extract_terms(&rawcipe) {
        pipe.sadd(key_term_rawcipes(&term), id);
    }

    pipe.exec_async(&mut redis_rawcipes).await?;
    
    Ok(true)
}

#[tracing::instrument(skip(redis_rawcipes))]
#[must_use]
pub async fn rawcipe_count(mut redis_rawcipes: MultiplexedConnection) -> Result<usize, Error> {
    Ok(redis_rawcipes.scard(key_rawcipes()).await?)
}

#[tracing::instrument(skip(redis_rawcipes))]
#[must_use]
async fn exists(mut redis_rawcipes: MultiplexedConnection, rawcipe: &Rawcipe) -> Result<bool, Error> {
    let rawcipes_with_titles: Vec<usize> = redis_rawcipes.smembers(key_title_rawcipes(&rawcipe.title)).await?;
    let rawcipes_with_description: Vec<usize> = redis_rawcipes.smembers(key_description_rawcipes(&rawcipe.description)).await?;

    Ok(rawcipes_with_titles.iter().any(|x| rawcipes_with_description.contains(x)))
}

#[tracing::instrument(skip(redis_rawcipes))]
#[must_use]
pub async fn get_rawcipe(mut redis_rawcipes: MultiplexedConnection, id: u64) -> Result<Rawcipe, Error> {
    let mut pipe = redis::pipe();

    pipe.get(key_rawcipe_link(id));
    pipe.get(key_rawcipe_title(id));
    pipe.get(key_rawcipe_description(id));
    pipe.lrange(key_rawcipe_ingredients(id), 0, -1);
    pipe.lrange(key_rawcipe_instructions(id), 0, -1);
    pipe.get(key_rawcipe_date(id));
    pipe.lrange(key_rawcipe_keywords(id), 0, -1);
    pipe.lrange(key_rawcipe_authors(id), 0, -1);
    pipe.lrange(key_rawcipe_images(id), 0, -1);
    pipe.get(key_rawcipe_rating(id));
    pipe.get(key_rawcipe_rating_count(id));
    pipe.get(key_rawcipe_prep_time_seconds(id));
    pipe.get(key_rawcipe_cook_time_seconds(id));
    pipe.get(key_rawcipe_total_time_seconds(id));
    pipe.get(key_rawcipe_servings(id));
    pipe.get(key_rawcipe_calories(id));
    pipe.get(key_rawcipe_carbohydrates(id));
    pipe.get(key_rawcipe_cholesterol(id));
    pipe.get(key_rawcipe_fat(id));
    pipe.get(key_rawcipe_fiber(id));
    pipe.get(key_rawcipe_protein(id));
    pipe.get(key_rawcipe_saturated_fat(id));
    pipe.get(key_rawcipe_sodium(id));
    pipe.get(key_rawcipe_sugar(id));

    let rawcipe = pipe.query_async(&mut redis_rawcipes)
        .await?;

    Ok(rawcipe)
}

fn split_by_space(string: &str) -> Vec<String> {
    string.split(' ').map(|v| v.to_string()).collect()
}

pub fn extract_terms(rawcipe: &Rawcipe) -> Vec<String> {
    let mut terms = vec![];
    terms.append(&mut split_by_space(&rawcipe.title));
    terms.append(&mut split_by_space(&rawcipe.description));
    for keyword in &rawcipe.keywords {
        terms.append(&mut split_by_space(keyword));
    }
    for ingredient in &rawcipe.ingredients {
        terms.append(&mut split_by_space(ingredient));
    }
    for instruction in &rawcipe.instructions {
        terms.append(&mut split_by_space(instruction));
    }
    terms
}

//pub async fn get_rawcipes_by_term(mut redis_rawcipes: MultiplexedConnection, term: &str) -> HashSet<usize> {
//    redis_rawcipes.smembers(key_rawcipes_by_term(term)).await.unwrap_or(HashSet::new())
//}

