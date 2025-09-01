use std::{error::Error, fmt};

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
            DbError::KeyIsNotStream(key) => write!(f, "Key '{key}' exists but is not a stream"),
            DbError::StreamStartIdNotFound(id) => write!(f, "Stream start ID '{id}' not found"),
            DbError::StreamEndIdNotFound(id) => write!(f, "Stream end ID '{id}' not found"),
        }
    }
}

impl Error for DbError {}
