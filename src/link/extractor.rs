use serde_json::Value;

use crate::BoxError;

mod c_extractor {
    use std::ffi::{c_char, CString};

    #[link(name = "extractor")]
    extern {
        fn extract(input: *const c_char) -> *mut c_char;
    }
    
    pub fn extract_wrapper(input: &str) -> Option<String> {
        unsafe {
            let input = CString::new(input).unwrap();
            let output = extract(input.as_ptr());
            if output.is_null() {
                None
            } else {
                let result = CString::from_raw(output).into_string().unwrap();
                Some(result)
            }
        }
    }
}

#[tracing::instrument(skip(contents))]
pub async fn extract(contents: &str) -> Result<Option<Value>, BoxError> {
    //let script_regex = RegexBuilder::new(r"<script.*?>(.*?)<\/script>")
    //    .dot_matches_new_line(true)
    //    .build()
    //    .unwrap();

    //let schema_regex = RegexBuilder::new(r#"\{.{0,1000}?(schema|("@type": "Recipe")).{0,1000}?@type.*\}"#)
    //    .dot_matches_new_line(true)
    //    .build()
    //    .unwrap();

    let Some(schema) = c_extractor::extract_wrapper(contents) else {
        return Ok(None);
    };

    let mut schema  = serde_json::from_str::<Value>(schema.as_str())
        .map_err(|err| Box::new(err) as BoxError)?;

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

