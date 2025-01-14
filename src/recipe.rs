use std::time::Duration;

use chrono::NaiveDate;
use sqlx::{prelude::FromRow, query, query_as, MySql, Pool};

use crate::BoxError;

#[derive(FromRow)]
struct Id(i32);

#[derive(Debug, Clone)]
pub struct Recipe {
    pub link: i32,
    pub title: Option<String>,
    pub description: Option<String>,
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
    pub ingredients: Vec<String>,
    pub instructions: Vec<String>,
    pub nutrition: Nutrition,
}

impl Recipe {
    pub fn is_complete(&self) -> bool {
        self.title.is_some()
            && !self.images.is_empty()
            && !self.authors.is_empty()
            && self.description.is_some()
            && self.date.is_some()
            && self.servings.is_some()
            && self.total_time_seconds.is_some()
            && !self.ingredients.is_empty()
            && self.rating.is_some()
            && self.rating_count.is_some()
            && !self.keywords.is_empty()
            && self.nutrition.is_complete()
    }

    pub fn should_add(&self) -> bool {
        !self.ingredients.is_empty()
    }
}

#[derive(Debug, Default, Clone)]
pub struct Nutrition {
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

impl Nutrition {
    pub fn is_complete(&self) -> bool {
        self.calories.is_some()
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

/// Returns true if added
/// Returns false if already existed or matches the blacklist
#[tracing::instrument(skip(pool))]
#[must_use]
pub async fn add(pool: Pool<MySql>, recipe: Recipe) -> Result<bool, BoxError> {
    if exists(pool.clone(), &recipe).await? {
        return Ok(false);
    }

    let recipe_id = query("INSERT INTO recipe (
link, title, description, date, servings, prep_time_seconds, cook_time_seconds, total_time_seconds, rating, rating_count, 
calories, carbohydrates, cholesterol, fat, fiber, protein, saturated_fat, sodium, sugar
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .bind(recipe.link)
        .bind(recipe.title)
        .bind(&recipe.description)
        .bind(recipe.date)
        .bind(recipe.servings)
        .bind(recipe.prep_time_seconds.map(|duration| duration.as_secs()))
        .bind(recipe.cook_time_seconds.map(|duration| duration.as_secs()))
        .bind(recipe.total_time_seconds.map(|duration| duration.as_secs()))
        .bind(recipe.rating)
        .bind(recipe.rating_count)
        .bind(recipe.nutrition.calories)
        .bind(recipe.nutrition.carbohydrates)
        .bind(recipe.nutrition.cholesterol)
        .bind(recipe.nutrition.fat)
        .bind(recipe.nutrition.fiber)
        .bind(recipe.nutrition.protein)
        .bind(recipe.nutrition.saturated_fat)
        .bind(recipe.nutrition.sodium)
        .bind(recipe.nutrition.fat)
        .execute(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?
        .last_insert_id();

    for keyword in &recipe.keywords {
        let tx = pool.begin()
            .await
            .map_err(|err| Box::new(err) as BoxError)?;
        
        let keyword_id = match get_keyword_id(pool.clone(), keyword).await? {
            Some(id) => id,
            None => query("INSERT INTO keyword (keyword) VALUES (?)")
                .bind(keyword)
                .execute(&pool)
                .await
                .map_err(|err| Box::new(err) as BoxError)?
                .last_insert_id() as i32,
        };

        tx.commit()
            .await
            .map_err(|err| Box::new(err) as BoxError)?;

        query("INSERT INTO recipe_keyword (recipe, keyword) VALUES (?, ?)")
            .bind(recipe_id)
            .bind(keyword_id)
            .execute(&pool)
            .await
            .map_err(|err| Box::new(err) as BoxError)?;
    }


    for author in &recipe.authors {
        let author_id = query("INSERT INTO author (name) VALUES (?)")
            .bind(author)
            .execute(&pool)
            .await
            .map_err(|err| Box::new(err) as BoxError)?
            .last_insert_id() as i32;

        query("INSERT INTO recipe_author (recipe, author) VALUES (?, ?)")
            .bind(recipe_id)
            .bind(author_id)
            .execute(&pool)
            .await
            .map_err(|err| Box::new(err) as BoxError)?;
    }

    for image in &recipe.images {
        let image_id = query("INSERT INTO image (image) VALUES (?)")
            .bind(image)
            .execute(&pool)
            .await
            .map_err(|err| Box::new(err) as BoxError)?
            .last_insert_id();

        query("INSERT INTO recipe_image (recipe, image) VALUES (?, ?)")
            .bind(recipe_id)
            .bind(image_id)
            .execute(&pool)
            .await
            .map_err(|err| Box::new(err) as BoxError)?;
    }

    for ingredient in &recipe.ingredients {
        let ingredient_id = query("INSERT INTO ingredient (ingredient) VALUES (?)")
            .bind(ingredient)
            .execute(&pool)
            .await
            .map_err(|err| Box::new(err) as BoxError)?
            .last_insert_id();

        query("INSERT INTO recipe_ingredient (recipe, ingredient) VALUES (?, ?)")
            .bind(recipe_id)
            .bind(ingredient_id)
            .execute(&pool)
            .await
            .map_err(|err| Box::new(err) as BoxError)?;
    }

    for instruction in &recipe.instructions {
        let instruction_id = query("INSERT INTO instruction (instruction) VALUES (?)")
            .bind(instruction)
            .execute(&pool)
            .await
            .map_err(|err| Box::new(err) as BoxError)?
            .last_insert_id();

        query("INSERT INTO recipe_instruction (recipe, instruction) VALUES (?, ?)")
            .bind(recipe_id)
            .bind(instruction_id)
            .execute(&pool)
            .await
            .map_err(|err| Box::new(err) as BoxError)?;
    }
    
    Ok(true)
}

#[tracing::instrument(skip(pool))]
#[must_use]
async fn exists(pool: Pool<MySql>, recipe: &Recipe) -> Result<bool, BoxError> {
    Ok(query("SELECT id FROM recipe WHERE title = ? AND description = ? LIMIT 1")
        .bind(&recipe.title)
        .bind(&recipe.description)
        .fetch_optional(&pool)
        .await
        .map_err(|err| dbg!(Box::new(err) as BoxError))?
        .is_some())
}

#[tracing::instrument(skip(pool))]
#[must_use]
async fn get_keyword_id(pool: Pool<MySql>, keyword: &str) -> Result<Option<i32>, BoxError> {
    query_as::<_, Id>("SELECT id FROM keyword WHERE keyword = ? LIMIT 1")
        .bind(keyword)
        .fetch_optional(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)
        .map(|v| v.map(|v| v.0))
}

