use chrono::NaiveDateTime;
use regex::Regex;
use serde_json::Value;
use url::Url;

use recipe_common::rawcipe::Rawcipe;

fn duration_to_seconds(duration: iso8601::Duration) -> Option<u64> {
    match duration {
        iso8601::Duration::YMDHMS { year, month, day, hour, minute, second, millisecond } => {
            if year != 0 || month != 0 {
                return None
            }

            Some((1000 * millisecond
                + second
                + 60 * minute
                + 60 * 60 * hour
                + 60 * 60 * 24 * day) as u64)
        }
        iso8601::Duration::Weeks(_) => None,
    }
}

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

fn date(v: &Value) -> Option<String> {
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
        .map(|v| v.format("%Y-%m-%d").to_string())
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

fn prep_time(v: &Value) -> Option<u64> {
    v.get("prepTime")
        .and_then(|v| v.as_str())
        .and_then(|v| iso8601::duration(v).ok())
        .and_then(duration_to_seconds)
}

fn cook_time(v: &Value) -> Option<u64> {
    v.get("cookTime")
        .and_then(|v| v.as_str())
        .and_then(|v| iso8601::duration(v).ok())
        .and_then(duration_to_seconds)
}

fn total_time(v: &Value) -> Option<u64> {
    v.get("totalTime")
        .and_then(|v| v.as_str())
        .and_then(|v| iso8601::duration(v).ok())
        .and_then(duration_to_seconds)
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

fn instructions(v: &Value) -> Vec<String> {
    v.get("recipeInstructions")
        .and_then(|v| v.as_array())
        .and_then(|v| v.iter()
            .map(|v| v.as_str()
                .map(|v| v.to_owned())
                .or(v.get("text")
                    .and_then(|v| v.as_str())
                    .map(|v| v.to_owned())
                )
            )
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

fn calories(v: &Value) -> Option<f32> {
    v.get("nutrition")
        .and_then(|v| v.get("calories"))
        .and_then(|v| v.as_str())
        .map(|v| v.replace("kcal", ""))
        .map(|v| v.replace("calories", ""))
        .map(|v| v.trim().to_owned())
        .and_then(|v| v.parse::<f32>().ok())
}

fn carbohydrates(v: &Value) -> Option<f32> {
    v.get("nutrition")
        .and_then(|v| v.get("carbohydrateContent"))
        .and_then(|v| v.as_str())
        .map(|v| v.replace("g", ""))
        .map(|v| v.trim().to_owned())
        .and_then(|v| v.parse::<f32>().ok())
}

fn cholesterol(v: &Value) -> Option<f32> {
    v.get("nutrition")
        .and_then(|v| v.get("cholesterolContent"))
        .and_then(|v| v.as_str())
        .map(|v| v.replace("mg", ""))
        .map(|v| v.trim().to_owned())
        .and_then(|v| v.parse::<f32>().ok())
}

fn fat(v: &Value) -> Option<f32> {
    v.get("nutrition")
        .and_then(|v| v.get("fatContent"))
        .and_then(|v| v.as_str())
        .map(|v| v.replace("g", ""))
        .map(|v| v.trim().to_owned())
        .and_then(|v| v.parse::<f32>().ok())
}

fn fiber(v: &Value) -> Option<f32> {
    v.get("nutrition")
        .and_then(|v| v.get("fiberContent"))
        .and_then(|v| v.as_str())
        .map(|v| v.replace("g", ""))
        .map(|v| v.trim().to_owned())
        .and_then(|v| v.parse::<f32>().ok())
}

fn protein(v: &Value) -> Option<f32> {
    v.get("nutrition")
        .and_then(|v| v.get("proteinContent"))
        .and_then(|v| v.as_str())
        .map(|v| v.replace("g", ""))
        .map(|v| v.trim().to_owned())
        .and_then(|v| v.parse::<f32>().ok())
}

fn saturated_fat(v: &Value) -> Option<f32> {
    v.get("nutrition")
        .and_then(|v| v.get("saturatedFatContent"))
        .and_then(|v| v.as_str())
        .map(|v| v.replace("g", ""))
        .map(|v| v.trim().to_owned())
        .and_then(|v| v.parse::<f32>().ok())}

fn sodium(v: &Value) -> Option<f32> {
    v.get("nutrition")
        .and_then(|v| v.get("sodiumContent"))
        .and_then(|v| v.as_str())
        .map(|v| v.replace("mg", ""))
        .map(|v| v.trim().to_owned())
        .and_then(|v| v.parse::<f32>().ok())
}

fn sugar(v: &Value) -> Option<f32> {
    v.get("nutrition")
        .and_then(|v| v.get("sugarContent"))
        .and_then(|v| v.as_str())
        .map(|v| v.replace("g", ""))
        .map(|v| v.trim().to_owned())
        .and_then(|v| v.parse::<f32>().ok())
}

#[tracing::instrument(skip(schema))]
pub async fn parse(link: String, schema: Value) -> Option<Rawcipe> {
    let title = title(&schema)?;
    let description = description(&schema)?;

    let ingredients = ingredients(&schema);
    if ingredients.is_empty() {
        return None;
    }

    let instructions = instructions(&schema);
    if instructions.is_empty() {
        return None;
    }

    Some(Rawcipe {
        link: link.clone(),
        title,
        ingredients,
        instructions,
        images: image(&schema),
        authors: authors(&schema, link.to_owned()),
        description,
        date: date(&schema),
        servings: servings(&schema),
        prep_time_seconds: prep_time(&schema),
        cook_time_seconds: cook_time(&schema),
        total_time_seconds: total_time(&schema).or_else(|| Some(prep_time(&schema)? + cook_time(&schema)?)),
        rating: rating(&schema),
        rating_count: rating_count(&schema),
        keywords: keywords(&schema),
        calories: calories(&schema),
        carbohydrates: carbohydrates(&schema),
        cholesterol: cholesterol(&schema),
        fat: fat(&schema),
        fiber: fiber(&schema),
        protein: protein(&schema),
        saturated_fat: saturated_fat(&schema),
        sodium: sodium(&schema),
        sugar: sugar(&schema),
    })
}

