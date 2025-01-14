use std::time::Duration;

use log::{info, warn};
use sqlx::{query, query_as, FromRow, MySql, Pool};
use tokio::time::interval;

use crate::{word::WordStatus, BoxError};

#[derive(FromRow)]
struct OneBigInt(i64);

async fn fetch_word_status(pool: Pool<MySql>, word: WordStatus) -> Result<i64, BoxError> {
    Ok(query_as::<_, OneBigInt>("SELECT COUNT(*) FROM word WHERE status = ?")
        .bind(word.to_string())
        .fetch_one(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?
        .0)
}

pub async fn fetch_table_count(pool: Pool<MySql>, table : &str) -> Result<i64, BoxError> {
    Ok(query_as::<_, OneBigInt>(&format!("SELECT COUNT(*) FROM {}", table))
        .fetch_one(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?
        .0)
}

async fn fetch_total_content_size(pool: Pool<MySql>) -> Result<i64, BoxError> {
    let mut count = query_as::<_, OneBigInt>("SELECT CAST(SUM(extraction_failed_link.content_size) AS INT) FROM extraction_failed_link")
        .fetch_one(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?
        .0;

    count += query_as::<_, OneBigInt>("SELECT CAST(SUM(processed_link.content_size) AS INT) FROM processed_link")
        .fetch_one(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?
        .0;

    Ok(count)
}

async fn fetch_count(pool: Pool<MySql>, table: &str) -> Result<i64, BoxError> {
    Ok(query_as::<_, OneBigInt>(&format!("SELECT COUNT(*) FROM {table}"))
        .fetch_one(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?
        .0)
}

async fn fetch_unique_recipe_ids_in_table(pool: Pool<MySql>, table: &str) -> Result<i64, BoxError> {
    Ok(query_as::<_, OneBigInt>(&format!("SELECT COUNT(DISTINCT recipe.id) FROM recipe JOIN {table} ON recipe.id = {table}.recipe"))
        .fetch_one(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?
        .0)
}

async fn fetch_recipes_with_column(pool: Pool<MySql>, column: &str) -> Result<i64, BoxError> {
    Ok(query_as::<_, OneBigInt>(&format!("SELECT COUNT(*) FROM recipe WHERE {column} IS NOT NULL"))
        .fetch_one(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?
        .0)
}

#[tracing::instrument(skip(pool))]
#[must_use]
async fn update(pool: Pool<MySql>) -> Result<(), BoxError> {
    let timestamp = chrono::offset::Utc::now();

    query("INSERT INTO word_statistic (
timestamp, waiting_for_generation, generating, generation_failed, generation_complete, waiting_for_classification,
classifying, classification_failed, classified_as_invalid, waiting_for_search, searching, search_failed, search_complete
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .bind(timestamp)
        .bind(fetch_word_status(pool.clone(), WordStatus::WaitingForGeneration).await?)
        .bind(fetch_word_status(pool.clone(), WordStatus::Generating).await?)
        .bind(fetch_word_status(pool.clone(), WordStatus::GenerationFailed).await?)
        .bind(fetch_word_status(pool.clone(), WordStatus::GenerationComplete).await?)
        .bind(fetch_word_status(pool.clone(), WordStatus::WaitingForClassification).await?)
        .bind(fetch_word_status(pool.clone(), WordStatus::Classifying).await?)
        .bind(fetch_word_status(pool.clone(), WordStatus::ClassificationFailed).await?)
        .bind(fetch_word_status(pool.clone(), WordStatus::ClassifiedAsInvalid).await?)
        .bind(fetch_word_status(pool.clone(), WordStatus::WaitingForSearch).await?)
        .bind(fetch_word_status(pool.clone(), WordStatus::Searching).await?)
        .bind(fetch_word_status(pool.clone(), WordStatus::SearchFailed).await?)
        .bind(fetch_word_status(pool.clone(), WordStatus::SearchComplete).await?)
        .execute(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    query("INSERT INTO link_statistic (
timestamp, waiting_for_processing, processing, download_failed, extraction_failed, processed, total_content_size
) VALUES (?, ?, ?, ?, ?, ?, ?)")
        .bind(timestamp)
        .bind(fetch_table_count(pool.clone(), "waiting_link").await?)
        .bind(fetch_table_count(pool.clone(), "processing_link").await?)
        .bind(fetch_table_count(pool.clone(), "download_failed_link").await?)
        .bind(fetch_table_count(pool.clone(), "extraction_failed_link").await?)
        .bind(fetch_table_count(pool.clone(), "processed_link").await?)
        .bind(fetch_total_content_size(pool.clone()).await?)
        .execute(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    query("INSERT INTO recipe_component_statistic (
timestamp, recipe_count, keyword_count, author_count, image_count, ingredient_count, instruction_count
) VALUES (?, ?, ?, ?, ?, ?, ?)")
        .bind(timestamp)
        .bind(fetch_count(pool.clone(), "recipe").await?)
        .bind(fetch_count(pool.clone(), "keyword").await?)
        .bind(fetch_count(pool.clone(), "author").await?)
        .bind(fetch_count(pool.clone(), "image").await?)
        .bind(fetch_count(pool.clone(), "ingredient").await?)
        .bind(fetch_count(pool.clone(), "instruction").await?)
        .execute(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    query("INSERT INTO recipe_statistic (
timestamp, with_keywords, with_authors, with_images, with_ingredients, with_instructions, with_title, with_description, with_date,
with_rating, with_rating_count, with_prep_time_seconds, with_cook_time_seconds, with_total_time_seconds, with_servings, 
with_calories, with_carbohydrates, with_cholesterol, with_fat, with_fiber, with_protein, with_saturated_fat, with_sodium, with_sugar
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .bind(timestamp)
        .bind(fetch_unique_recipe_ids_in_table(pool.clone(), "recipe_keyword").await?)
        .bind(fetch_unique_recipe_ids_in_table(pool.clone(), "recipe_author").await?)
        .bind(fetch_unique_recipe_ids_in_table(pool.clone(), "recipe_image").await?)
        .bind(fetch_unique_recipe_ids_in_table(pool.clone(), "recipe_ingredient").await?)
        .bind(fetch_unique_recipe_ids_in_table(pool.clone(), "recipe_instruction").await?)
        .bind(fetch_recipes_with_column(pool.clone(), "title").await?)
        .bind(fetch_recipes_with_column(pool.clone(), "description").await?)
        .bind(fetch_recipes_with_column(pool.clone(), "date").await?)
        .bind(fetch_recipes_with_column(pool.clone(), "rating").await?)
        .bind(fetch_recipes_with_column(pool.clone(), "rating_count").await?)
        .bind(fetch_recipes_with_column(pool.clone(), "prep_time_seconds").await?)
        .bind(fetch_recipes_with_column(pool.clone(), "cook_time_seconds").await?)
        .bind(fetch_recipes_with_column(pool.clone(), "total_time_seconds").await?)
        .bind(fetch_recipes_with_column(pool.clone(), "servings").await?)
        .bind(fetch_recipes_with_column(pool.clone(), "calories").await?)
        .bind(fetch_recipes_with_column(pool.clone(), "carbohydrates").await?)
        .bind(fetch_recipes_with_column(pool.clone(), "cholesterol").await?)
        .bind(fetch_recipes_with_column(pool.clone(), "fat").await?)
        .bind(fetch_recipes_with_column(pool.clone(), "fiber").await?)
        .bind(fetch_recipes_with_column(pool.clone(), "protein").await?)
        .bind(fetch_recipes_with_column(pool.clone(), "saturated_fat").await?)
        .bind(fetch_recipes_with_column(pool.clone(), "sodium").await?)
        .bind(fetch_recipes_with_column(pool.clone(), "sugar").await?)
        .execute(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;

    info!("Statistics updated");

    Ok(())
}

pub async fn run(pool: Pool<MySql>) {
    info!("Started statistic updater");

    let mut interval = interval(Duration::from_secs(30));

    loop {
        interval.tick().await;

        if let Err(err) = update(pool.clone()).await {
            warn!("Error while updating statistics: {} (source: {:?})", err, err.source());
        }
    }
}

