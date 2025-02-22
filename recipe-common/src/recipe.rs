use std::time::Duration;

use chrono::NaiveDate;
use redis::{aio::MultiplexedConnection, AsyncCommands};

use crate::BoxError;

#[derive(Debug, Clone)]
pub struct Recipe {
    pub link: String,
    pub title: String,
    pub description: String,
    pub ingredients: Vec<String>,
    pub instructions: Vec<String>,
    pub date: Option<NaiveDate>,
    pub keywords: Vec<String>,
    pub authors: Vec<String>,
    pub images: Vec<String>,
    pub rating: Option<f32>,
    pub rating_count: Option<i32>,
    pub prep_time_seconds: Option<Duration>,
    pub cook_time_seconds: Option<Duration>,
    pub total_time_seconds: Option<Duration>,
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

impl Recipe {
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

fn key_next_id() -> String {
    "recipe:id_counter".to_string()
}

fn key_recipes() -> String {
    "recipe:recipes".to_string()
}

fn key_recipe_link(id: u64) -> String {
    format!("recipe:{id}:link")
}

fn key_recipe_title(id: u64) -> String {
    format!("recipe:{id}:title")
}

fn key_title_recipes(title: &str) -> String {
    format!("title:{title}:titles")
}

fn key_recipe_description(id: u64) -> String {
    format!("recipe:{id}:description")
}

fn key_description_recipes(description: &str) -> String {
    format!("description:{description}:recipes")
}

fn key_recipe_date(id: u64) -> String {
    format!("recipe:{id}:date")
}

fn key_recipe_rating(id: u64) -> String {
    format!("recipe:{id}:rating")
}

fn key_recipe_rating_count(id: u64) -> String {
    format!("recipe:{id}:rating_count")
}

fn key_recipe_prep_time_seconds(id: u64) -> String {
    format!("recipe:{id}:prep_time_seconds")
}

fn key_recipe_cook_time_seconds(id: u64) -> String {
    format!("recipe:{id}:cook_time_seconds")
}

fn key_recipe_total_time_seconds(id: u64) -> String {
    format!("recipe:{id}:total_time_seconds")
}

fn key_recipe_servings(id: u64) -> String {
    format!("recipe:{id}:key_servings")
}

fn key_recipe_calories(id: u64) -> String {
    format!("recipe:{id}:calories")
}

fn key_recipe_carbohydrates(id: u64) -> String {
    format!("recipe:{id}:carbohydrates")
}

fn key_recipe_cholesterol(id: u64) -> String {
    format!("recipe:{id}:cholesterol")
}

fn key_recipe_fat(id: u64) -> String {
    format!("recipe:{id}:fat")
}

fn key_recipe_fiber(id: u64) -> String {
    format!("recipe:{id}:fiber")
}

fn key_recipe_protein(id: u64) -> String {
    format!("recipe:{id}:protein")
}

fn key_recipe_saturated_fat(id: u64) -> String {
    format!("recipe:{id}:saturated_fat")
}

fn key_recipe_sodium(id: u64) -> String {
    format!("recipe:{id}:sodium")
}

fn key_recipe_sugar(id: u64) -> String {
    format!("recipe:{id}:sugar")
}

fn key_recipe_keywords(id: u64) -> String {
    format!("recipe:{id}:keywords")
}

fn key_recipe_authors(id: u64) -> String {
    format!("recipe:{id}:authors")
}

fn key_recipe_images(id: u64) -> String {
    format!("recipe:{id}:images")
}

fn key_recipe_ingredients(id: u64) -> String {
    format!("recipe:{id}:ingredients")
}

fn key_recipe_instructions(id: u64) -> String {
    format!("recipe:{id}:ingredients")
}

/// Returns true if added
/// Returns false if already existed or matches the blacklist
#[tracing::instrument(skip(redis_recipes))]
#[must_use]
pub async fn add(mut redis_recipes: MultiplexedConnection, recipe: Recipe) -> Result<bool, BoxError> {
    if exists(redis_recipes.clone(), &recipe).await? {
        return Ok(false);
    }

    let id: u64 = redis_recipes.incr(key_next_id(), 1)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    let mut pipe = redis::pipe();

    pipe.sadd(key_recipes(), id);

    pipe.set(key_recipe_link(id), recipe.link);

    pipe.sadd(key_title_recipes(&recipe.title), id);
    pipe.set(key_recipe_title(id), recipe.title);

    pipe.sadd(key_description_recipes(&recipe.description), id);
    pipe.set(key_recipe_description(id), recipe.description);

    recipe.date.map(|v| pipe.set(key_recipe_date(id), v.to_string()));
    recipe.rating.map(|v| pipe.set(key_recipe_rating(id), v));
    recipe.rating_count.map(|v| pipe.set(key_recipe_rating_count(id), v));
    recipe.prep_time_seconds.map(|v| pipe.set(key_recipe_prep_time_seconds(id), v.as_secs().to_string()));
    recipe.cook_time_seconds.map(|v| pipe.set(key_recipe_cook_time_seconds(id), v.as_secs().to_string()));
    recipe.total_time_seconds.map(|v| pipe.set(key_recipe_total_time_seconds(id), v.as_secs().to_string()));
    recipe.servings.map(|v| pipe.set(key_recipe_servings(id), v));
    recipe.calories.map(|v| pipe.set(key_recipe_calories(id), v));
    recipe.carbohydrates.map(|v| pipe.set(key_recipe_carbohydrates(id), v));
    recipe.cholesterol.map(|v| pipe.set(key_recipe_cholesterol(id), v));
    recipe.fat.map(|v| pipe.set(key_recipe_fat(id), v));
    recipe.fiber.map(|v| pipe.set(key_recipe_fiber(id), v));
    recipe.protein.map(|v| pipe.set(key_recipe_protein(id), v));
    recipe.saturated_fat.map(|v| pipe.set(key_recipe_saturated_fat(id), v));
    recipe.sodium.map(|v| pipe.set(key_recipe_sodium(id), v));
    recipe.sugar.map(|v| pipe.set(key_recipe_sugar(id), v));

    if !recipe.keywords.is_empty() {
        pipe.cmd("lpush").arg(key_recipe_keywords(id));
        for keyword in recipe.keywords {
            pipe.arg(keyword);
        }
    }

    if !recipe.authors.is_empty() {
        pipe.cmd("lpush").arg(key_recipe_authors(id));
        for author in recipe.authors {
            pipe.arg(author);
        }
    }

    if !recipe.images.is_empty() {
        pipe.cmd("lpush").arg(key_recipe_images(id));
        for image in recipe.images {
            pipe.arg(image);
        }
    }

    if !recipe.ingredients.is_empty() {
        pipe.cmd("lpush").arg(key_recipe_ingredients(id));
        for ingredient in recipe.ingredients {
            pipe.arg(ingredient);
        }
    }

    if !recipe.instructions.is_empty() {
        pipe.cmd("lpush").arg(key_recipe_instructions(id));
        for instruction in recipe.instructions {
            pipe.arg(instruction);
        }
    }

    pipe.exec_async(&mut redis_recipes)
        .await
        .map_err(|err| dbg!(Box::new(err) as BoxError))?;
    
    Ok(true)
}

#[tracing::instrument(skip(redis_recipes))]
#[must_use]
pub async fn recipe_count(mut redis_recipes: MultiplexedConnection) -> Result<usize, BoxError> {
    let count: usize = redis_recipes.scard(key_recipes())
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    Ok(count)
}

#[tracing::instrument(skip(redis_recipes))]
#[must_use]
async fn exists(mut redis_recipes: MultiplexedConnection, recipe: &Recipe) -> Result<bool, BoxError> {
    let recipes_with_titles: Vec<usize> = redis_recipes.smembers(key_title_recipes(&recipe.title))
        .await
        .map_err(|err| dbg!(Box::new(err) as BoxError))?;

    let recipes_with_description: Vec<usize> = redis_recipes.smembers(key_description_recipes(&recipe.description))
        .await
        .map_err(|err| dbg!(Box::new(err) as BoxError))?;

    Ok(recipes_with_titles.iter().any(|x| recipes_with_description.contains(x)))
}

#[tracing::instrument(skip(redis_recipes))]
#[must_use]
pub async fn ingredients(mut redis_recipes: MultiplexedConnection, id: u64) -> Result<Vec<String>, BoxError> {
    let ingredients: Vec<String> = redis_recipes.lrange(key_recipe_ingredients(id), 0, -1)
        .await
        .map_err(|err| dbg!(Box::new(err) as BoxError))?;

    Ok(ingredients)
}

