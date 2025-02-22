use std::slice::Iter;

use redis::{aio::MultiplexedConnection, AsyncCommands, ErrorKind, FromRedisValue, RedisError, RedisResult, Value};
use serde::Serialize;
use utoipa::ToSchema;

use crate::BoxError;

#[derive(Serialize, Debug, Clone, ToSchema)]
pub struct Recipe {
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

fn get_redis_value<T: FromRedisValue>(iter: &mut Iter<Value>, field: &str) -> Result<T, RedisError> {
    match iter.next().map(T::from_redis_value) {
        Some(Ok(v)) => Ok(v),
        _ => Err(RedisError::from((ErrorKind::TypeError, "Failed to get field", field.to_owned()))),
    }
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
        })
    }
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
    format!("recipe:{id}:instructions")
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
    recipe.prep_time_seconds.map(|v| pipe.set(key_recipe_prep_time_seconds(id), v));
    recipe.cook_time_seconds.map(|v| pipe.set(key_recipe_cook_time_seconds(id), v));
    recipe.total_time_seconds.map(|v| pipe.set(key_recipe_total_time_seconds(id), v));
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
        for keyword in recipe.keywords.iter().rev() {
            pipe.arg(keyword);
        }
    }

    if !recipe.authors.is_empty() {
        pipe.cmd("lpush").arg(key_recipe_authors(id));
        for author in recipe.authors.iter().rev() {
            pipe.arg(author);
        }
    }

    if !recipe.images.is_empty() {
        pipe.cmd("lpush").arg(key_recipe_images(id));
        for image in recipe.images.iter().rev() {
            pipe.arg(image);
        }
    }

    if !recipe.ingredients.is_empty() {
        pipe.cmd("lpush").arg(key_recipe_ingredients(id));
        for ingredient in recipe.ingredients.iter().rev() {
            pipe.arg(ingredient);
        }
    }

    if !recipe.instructions.is_empty() {
        pipe.cmd("lpush").arg(key_recipe_instructions(id));
        for instruction in recipe.instructions.iter().rev() {
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
pub async fn get_recipe(mut redis_recipes: MultiplexedConnection, id: u64) -> Result<Recipe, BoxError> {
    let mut pipe = redis::pipe();

    pipe.get(key_recipe_link(id));
    pipe.get(key_recipe_title(id));
    pipe.get(key_recipe_description(id));
    pipe.lrange(key_recipe_ingredients(id), 0, -1);
    pipe.lrange(key_recipe_instructions(id), 0, -1);
    pipe.get(key_recipe_date(id));
    pipe.lrange(key_recipe_keywords(id), 0, -1);
    pipe.lrange(key_recipe_authors(id), 0, -1);
    pipe.lrange(key_recipe_images(id), 0, -1);
    pipe.get(key_recipe_rating(id));
    pipe.get(key_recipe_rating_count(id));
    pipe.get(key_recipe_prep_time_seconds(id));
    pipe.get(key_recipe_cook_time_seconds(id));
    pipe.get(key_recipe_total_time_seconds(id));
    pipe.get(key_recipe_servings(id));
    pipe.get(key_recipe_calories(id));
    pipe.get(key_recipe_carbohydrates(id));
    pipe.get(key_recipe_cholesterol(id));
    pipe.get(key_recipe_fat(id));
    pipe.get(key_recipe_fiber(id));
    pipe.get(key_recipe_protein(id));
    pipe.get(key_recipe_saturated_fat(id));
    pipe.get(key_recipe_sodium(id));
    pipe.get(key_recipe_sugar(id));

    let recipe = pipe.query_async(&mut redis_recipes)
        .await
        .unwrap();

    Ok(recipe)
}

