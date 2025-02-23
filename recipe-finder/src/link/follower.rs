use std::sync::LazyLock;

use regex::{Regex, RegexBuilder};
use url::Url;

static LINK_ELEMENT_REGEX: LazyLock<Regex> = LazyLock::new(|| 
    RegexBuilder::new(r#"<a.{0,2000}?>"#)
        .build()
        .unwrap()
);

static HREF_REGEX: LazyLock<Regex> = LazyLock::new(|| 
    RegexBuilder::new(r#"href\s?=\s?"([^"]{0,500})""#)
        .build()
        .unwrap()
);

#[tracing::instrument(skip(contents))]
pub async fn follow(contents: String, link: String) -> Vec<String> {
    let url = Url::parse(&link)
        .expect("How did you screw this up");
    let domain = "https://".to_string() + url.domain()
        .expect("How did you screw this up");

    LINK_ELEMENT_REGEX.captures_iter(&contents)
        .map(|captures| captures.get(0).unwrap().as_str())
        .filter_map(|element| HREF_REGEX.captures(element))
            .map(|captures| captures.get(1).unwrap().as_str())

        // fix relative links (eg '/category/stupid_recipes' -> bbc.co.uk/category/stupid_recipes)
        .map(|v| if v.chars().next().is_some_and(|v| v == '/') { 
                domain.clone() + v 
            } else { 
                v.to_string() 
            })
        .filter(|new_link| Url::parse(new_link).is_ok())

        // eg, bruh.com/some-recipe might have links to bruh.com/some-recipe/comments#36
        .filter(|new_link| !new_link.starts_with(&link))

        // hardcoded fix. often, recipes have www.domain.com/your_shitty_recipe/wprm_print pages
        // these pages have shit schemas. but the original pages are generally fine
        .map(|v| v.replace("/wprm_print", ""))

        .map(|v| v.to_owned())
        .collect()
}

