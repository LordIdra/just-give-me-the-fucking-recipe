use regex::RegexBuilder;
use serde_json::Value;

use crate::BoxError;

#[tracing::instrument(skip(contents))]
pub async fn extract(contents: &str) -> Result<Option<Value>, BoxError> {
    let script_regex = RegexBuilder::new(r"<script.*?>(.*?)<\/script>")
        .dot_matches_new_line(true)
        .build()
        .unwrap();

    let schema_regex = RegexBuilder::new(r#"\{.{0,1000}?(schema|("@type": "Recipe")).{0,1000}?@type.*\}"#)
        .dot_matches_new_line(true)
        .build()
        .unwrap();

    let mut schema = None;

    for script in script_regex.captures_iter(contents) {
        let script = script.get(1).unwrap().into();

        let Some(other_schema) = schema_regex.captures(script) else {
            continue;
        };

        let other_schema = other_schema.get(0).unwrap();
        let other_schema = serde_json::from_str::<Value>(other_schema.as_str())
            .map_err(|err| Box::new(err) as BoxError)?;

        schema = Some(other_schema);

        break;
    }

    let Some(mut schema) = schema else {
        return Ok(None);
    };

    if let Some(graph) = schema.get("@graph") {
        if let Some(arr) = graph.as_array() {
            let new_schema = arr.iter().find(|v| v.get("@type")
                .and_then(|v| v.as_str())
                .is_some_and(|v| v == "Recipe")
            );

            let Some(new_schema) = new_schema else {
                return Ok(None);
            };

            schema = new_schema.clone();
        }
    }

    Ok(Some(schema))
}

