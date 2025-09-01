use std::{error::Error, fmt};

// Custom error enum for Db operations
#[derive(Debug)]
pub enum DbError {
    KeyNotFound(String),
    KeyIsNotStream(String),
    StreamStartIdNotFound(String),
    StreamEndIdNotFound(String),
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DbError::KeyNotFound(key) => write!(f, "Key '{key}' not found"),
            Db::KeyIsNotStream(key) => write!(f, "Key '{key}' exists but is not a stream"),
            Db::StreamStartIdNotFound(id) => write!(f, "Stream start ID '{id}' not found"),
            Db::StreamEndIdNotFound(id) => write!(f, "Stream end ID '{id}' not found"),
        }
    }
}

impl Error for DbError {}
