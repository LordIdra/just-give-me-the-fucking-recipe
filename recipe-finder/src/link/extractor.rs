use anyhow::Error;
use serde_json::Value;

mod c_extractor {
    use std::{ffi::{c_char, CStr, CString}, str};

    #[link(name = "extractor")]
    unsafe extern {
        fn extract(input: *const c_char) -> *mut c_char;
    }
    
    pub fn extract_wrapper(input: &str) -> Option<String> {
        unsafe {
            let input: String = input.chars()
                .filter(|&c| c != '\0')
                .collect();
            let input = CString::new(input).unwrap();
            let output = extract(input.as_ptr());
            if output.is_null() {
                None
            } else {
                let result = str::from_utf8(CStr::from_ptr(output).to_bytes()).unwrap().to_string();
                Some(result)
            }
        }
    }
}

#[tracing::instrument(skip(contents))]
pub async fn extract(link: &str, contents: &str) -> Result<Option<Value>, Error> {
    let Some(schema) = c_extractor::extract_wrapper(contents) else {
        return Ok(None);
    };

    let mut schema = serde_json::from_str::<Value>(schema.as_str())?;

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

