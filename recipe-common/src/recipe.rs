use std::collections::HashSet;

use anyhow::Error;
use redis::{aio::MultiplexedConnection, AsyncCommands as _, ErrorKind, FromRedisValue, RedisError, RedisResult, Value};
use serde::Serialize;
use utoipa::ToSchema;

use crate::rawcipe::{get_redis_value, Rawcipe};

#[derive(Serialize, Debug, Clone, ToSchema)]
pub struct Recipe {
    pub link: String,
    pub title: String,
    pub description: String,
    pub ingredients: Vec<String>,
    pub instructions: Vec<String>,
    pub date: String,
    pub keywords: Vec<String>,
    pub authors: Vec<String>,
    pub images: Vec<String>,
    pub rating: f32,
    pub rating_count: i32,
    pub prep_time_seconds: u64,
    pub cook_time_seconds: u64,
    pub total_time_seconds: u64,
    pub servings: String,
    pub calories: f32,
    pub carbohydrates: f32,
    pub cholesterol: f32,
    pub fat: f32,
    pub fiber: f32,
    pub protein: f32,
    pub saturated_fat: f32,
    pub sodium: f32,
    pub sugar: f32,
    pub tags: Vec<String>,
    pub terms: Vec<String>,
}

impl FromRedisValue for Recipe {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        let Value::Array(ref items) = *v else {
            return Err(RedisError::from((ErrorKind::TypeError, "Expected to get iterable")));
        };
        let mut iter = items.iter();

        Ok(Recipe {
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
            tags: get_redis_value(&mut iter, "tags")?,
            terms: get_redis_value(&mut iter, "terms")?,
        })
    }
}

impl Recipe {
    pub fn from_rawcipe(rawcipe: Rawcipe) -> Self {
        Self {
            link: rawcipe.link.clone(),
            title: rawcipe.title.clone(),
            description: rawcipe.description.clone(),
            ingredients: rawcipe.ingredients.clone(),
            instructions: rawcipe.instructions.clone(),
            date: rawcipe.date.clone().unwrap(),
            keywords: rawcipe.keywords.clone(),
            authors: rawcipe.authors.clone(),
            images: rawcipe.images.clone(),
            rating: rawcipe.rating.unwrap(),
            rating_count: rawcipe.rating_count.unwrap(),
            prep_time_seconds: rawcipe.prep_time_seconds.unwrap(),
            cook_time_seconds: rawcipe.cook_time_seconds.unwrap(),
            total_time_seconds: rawcipe.total_time_seconds.unwrap(),
            servings: rawcipe.servings.clone().unwrap(),
            calories: rawcipe.calories.unwrap(),
            carbohydrates: rawcipe.carbohydrates.unwrap(),
            cholesterol: rawcipe.cholesterol.unwrap(),
            fat: rawcipe.fat.unwrap(),
            fiber: rawcipe.fiber.unwrap(),
            protein: rawcipe.protein.unwrap(),
            saturated_fat: rawcipe.saturated_fat.unwrap(),
            sodium: rawcipe.sodium.unwrap(),
            sugar: rawcipe.sugar.unwrap(),
            tags: extract_tags(&rawcipe),
            terms: extract_terms(&rawcipe),
        }
    }
}

// STRING acting as counter for next recipe id
fn key_id() -> String {
    "static:id".to_string()
}

// SET of all recipe ids
fn key_recipes() -> String {
    "recipes".to_string()
}

// SET of all recipes associated with a term
fn key_term_recipes(term: &str) -> String {
    format!("term:{term}:recipes")
}

// STRING
fn key_recipe_link(id: u64) -> String {
    format!("recipe:{id}:link")
}

// STRING
fn key_recipe_title(id: u64) -> String {
    format!("recipe:{id}:title")
}

// STRING
fn key_recipe_description(id: u64) -> String {
    format!("recipe:{id}:description")
}

// STRING
fn key_recipe_date(id: u64) -> String {
    format!("recipe:{id}:date")
}

// LIST
fn key_recipe_keywords(id: u64) -> String {
    format!("recipe:{id}:keywords")
}

// LIST
fn key_recipe_authors(id: u64) -> String {
    format!("recipe:{id}:authors")
}

// LIST
fn key_recipe_images(id: u64) -> String {
    format!("recipe:{id}:images")
}

// STRING
fn key_recipe_rating(id: u64) -> String {
    format!("recipe:{id}:rating")
}

// STRING
fn key_recipe_rating_count(id: u64) -> String {
    format!("recipe:{id}:rating_count")
}

// STRING
fn key_recipe_prep_time_seconds(id: u64) -> String {
    format!("recipe:{id}:prep_time_seconds")
}

// STRING
fn key_recipe_cook_time_seconds(id: u64) -> String {
    format!("recipe:{id}:cook_time_seconds")
}

// STRING
fn key_recipe_total_time_seconds(id: u64) -> String {
    format!("recipe:{id}:total_time_seconds")
}

// STRING
fn key_recipe_servings(id: u64) -> String {
    format!("recipe:{id}:key_servings")
}

// STRING
fn key_recipe_calories(id: u64) -> String {
    format!("recipe:{id}:calories")
}

// STRING
fn key_recipe_carbohydrates(id: u64) -> String {
    format!("recipe:{id}:carbohydrates")
}

// STRING
fn key_recipe_cholesterol(id: u64) -> String {
    format!("recipe:{id}:cholesterol")
}

// STRING
fn key_recipe_fat(id: u64) -> String {
    format!("recipe:{id}:fat")
}

// STRING
fn key_recipe_fiber(id: u64) -> String {
    format!("recipe:{id}:fiber")
}

// STRING
fn key_recipe_protein(id: u64) -> String {
    format!("recipe:{id}:protein")
}

// STRING
fn key_recipe_saturated_fat(id: u64) -> String {
    format!("recipe:{id}:saturated_fat")
}

// STRING
fn key_recipe_sodium(id: u64) -> String {
    format!("recipe:{id}:sodium")
}

// STRING
fn key_recipe_sugar(id: u64) -> String {
    format!("recipe:{id}:sugar")
}

// LIST
fn key_recipe_ingredients(id: u64) -> String {
    format!("recipe:{id}:ingredients")
}

// LIST
fn key_recipe_instructions(id: u64) -> String {
    format!("recipe:{id}:instructions")
}

// LIST
fn key_recipe_tags(id: u64) -> String {
    format!("recipe:{id}:tags")
}

// LIST
fn key_recipe_terms(id: u64) -> String {
    format!("recipe:{id}:terms")
}

/// Returns true if added
/// Returns false if already existed or matches the blacklist
#[tracing::instrument(skip(redis_recipes))]
#[must_use]
pub async fn add(mut redis_recipes: MultiplexedConnection, recipe: Recipe) -> Result<bool, Error> {
    let id: u64 = redis_recipes.incr(key_id(), 1).await?;

    let mut pipe = redis::pipe();

    pipe.sadd(key_recipes(), id);
    pipe.set(key_recipe_link(id), &recipe.link);
    pipe.set(key_recipe_title(id), &recipe.title);
    pipe.set(key_recipe_description(id), &recipe.description);

    pipe.cmd("lpush").arg(key_recipe_ingredients(id));
    for ingredient in recipe.ingredients.iter().rev() {
        pipe.arg(ingredient);
    }

    pipe.cmd("lpush").arg(key_recipe_instructions(id));
    for instruction in recipe.instructions.iter().rev() {
        pipe.arg(instruction);
    }

    pipe.set(key_recipe_date(id), recipe.date.clone());

    pipe.cmd("lpush").arg(key_recipe_keywords(id));
    for keyword in recipe.keywords.iter().rev() {
        pipe.arg(keyword);
    }

    pipe.cmd("lpush").arg(key_recipe_authors(id));
    for author in recipe.authors.iter().rev() {
        pipe.arg(author);
    }

    pipe.cmd("lpush").arg(key_recipe_images(id));
    for image in recipe.images.iter().rev() {
        pipe.arg(image);
    }

    pipe.set(key_recipe_rating(id), recipe.rating);
    pipe.set(key_recipe_rating_count(id), recipe.rating_count);
    pipe.set(key_recipe_prep_time_seconds(id), recipe.prep_time_seconds);
    pipe.set(key_recipe_cook_time_seconds(id), recipe.cook_time_seconds);
    pipe.set(key_recipe_total_time_seconds(id), recipe.total_time_seconds);
    pipe.set(key_recipe_servings(id), recipe.servings.clone());
    pipe.set(key_recipe_calories(id), recipe.calories);
    pipe.set(key_recipe_carbohydrates(id), recipe.carbohydrates);
    pipe.set(key_recipe_cholesterol(id), recipe.cholesterol);
    pipe.set(key_recipe_fat(id), recipe.fat);
    pipe.set(key_recipe_fiber(id), recipe.fiber);
    pipe.set(key_recipe_protein(id), recipe.protein);
    pipe.set(key_recipe_saturated_fat(id), recipe.saturated_fat);
    pipe.set(key_recipe_sodium(id), recipe.sodium);
    pipe.set(key_recipe_sugar(id), recipe.sugar);

    for tag in recipe.tags {
        pipe.sadd(key_recipe_tags(id), tag);
    }

    for term in recipe.terms {
        pipe.sadd(key_term_recipes(&term), id);
        pipe.sadd(key_recipe_terms(id), term);
    }

    pipe.exec_async(&mut redis_recipes).await?;
    
    Ok(true)
}

pub fn extract_tags(rawcipe: &Rawcipe) -> Vec<String> {
    let lowercase_keywords: Vec<String> = rawcipe.keywords
        .clone()
        .iter()
        .map(|keyword| keyword.to_lowercase())
        .collect();
    let mut tags = vec![];

    if lowercase_keywords.contains(&"vegetarian".to_string()) {
        tags.push("vegetarian".to_string());
    }

    if lowercase_keywords.contains(&"vegan".to_string()) {
        tags.push("vegan".to_string());
    }

    if lowercase_keywords.contains(&"gluten free".to_string()) {
        tags.push("gluten free".to_string());
    }

    tags
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

fn split_by_space(string: &str) -> Vec<String> {
    string.split(' ').map(|v| v.to_string()).collect()
}

async fn get_recipes_by_term(mut redis_recipes: MultiplexedConnection, term: &str) -> HashSet<usize> {
    redis_recipes.smembers(key_term_recipes(term)).await.unwrap_or(HashSet::new())
}

