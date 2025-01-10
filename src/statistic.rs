use std::time::Duration;

use log::{info, warn};
use sqlx::{query, query_as, FromRow, MySql, Pool};
use tokio::time::interval;

use crate::{page::PageStatus, word::WordStatus, BoxError};

#[derive(FromRow)]
struct Count(i32);

async fn fetch_word_status(pool: Pool<MySql>, word: WordStatus) -> Result<i32, BoxError> {
    Ok(query_as::<_, Count>("SELECT count(*) FROM word WHERE status = ?")
        .bind(word.to_string())
        .fetch_one(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?
        .0)
}

async fn fetch_page_status(pool: Pool<MySql>, word: PageStatus) -> Result<i32, BoxError> {
    Ok(query_as::<_, Count>("SELECT count(*) FROM page WHERE status = ?")
        .bind(word.to_string())
        .fetch_one(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?
        .0)
}

async fn fetch_total_content_size(pool: Pool<MySql>) -> Result<i32, BoxError> {
    Ok(query_as::<_, Count>("SELECT sum(content_size) FROM page")
        .fetch_one(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?
        .0)
}

async fn fetch_count(pool: Pool<MySql>, table: &str) -> Result<i32, BoxError> {
    Ok(query_as::<_, Count>("SELECT count(*) FROM ?")
        .bind(table.to_string())
        .fetch_one(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?
        .0)
}

async fn fetch_unique_recipe_ids_in_table(pool: Pool<MySql>, table: &str) -> Result<i32, BoxError> {
    Ok(query_as::<_, Count>("SELECT COUNT(DISTINCT recipe.id) FROM recipe JOIN ? ON recipe.id = ?.recipe")
        .bind(table.to_string())
        .fetch_one(&pool)
        .await
        .map_err(|err| Box::new(err) as BoxError)?
        .0)
}

async fn fetch_recipes_with_column(pool: Pool<MySql>, table: &str) -> Result<i32, BoxError> {
    Ok(query_as::<_, Count>("SELECT count(*) FROM recipe WHERE ? IS NOT NULL")
        .bind(table.to_string())
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

    query("INSERT INTO page_statistic (
timestamp, waiting_for_download, downloading, download_failed, waiting_for_extraction, extracting, extraction_failed,
waiting_for_parsing, parsing, parsing_incomplete_recipe, waiting_for_following, following, following_complete, following_failed,
total_content_size
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .bind(timestamp)
        .bind(fetch_page_status(pool.clone(), PageStatus::WaitingForDownload).await?)
        .bind(fetch_page_status(pool.clone(), PageStatus::Downloading).await?)
        .bind(fetch_page_status(pool.clone(), PageStatus::DownloadFailed).await?)
        .bind(fetch_page_status(pool.clone(), PageStatus::WaitingForExtraction).await?)
        .bind(fetch_page_status(pool.clone(), PageStatus::Extracting).await?)
        .bind(fetch_page_status(pool.clone(), PageStatus::ExtractionFailed).await?)
        .bind(fetch_page_status(pool.clone(), PageStatus::WaitingForParsing).await?)
        .bind(fetch_page_status(pool.clone(), PageStatus::Parsing).await?)
        .bind(fetch_page_status(pool.clone(), PageStatus::ParsingIncompleteRecipe).await?)
        .bind(fetch_page_status(pool.clone(), PageStatus::WaitingForFollowing).await?)
        .bind(fetch_page_status(pool.clone(), PageStatus::Following).await?)
        .bind(fetch_page_status(pool.clone(), PageStatus::FollowingComplete).await?)
        .bind(fetch_page_status(pool.clone(), PageStatus::FollowingFailed).await?)
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
        .bind(fetch_recipes_with_column(pool.clone(), "total_time_second").await?)
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

    Ok(())
}

pub async fn run(pool: Pool<MySql>) {
    info!("Started statistic updater");

    let mut interval = interval(Duration::from_secs(30));

    loop {
        if let Err(err) = update(pool.clone()).await {
            warn!("Error while updating statistics: {} (source: {:?})", err, err.source());
        }

        interval.tick().await;
    }
}

