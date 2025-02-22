use std::error::Error;

pub type BoxError = Box<dyn Error + Send>;

pub mod link;
pub mod link_blacklist;
pub mod recipe;

