use std::{
    collections::{HashMap, VecDeque},
    time::Duration,
    fmt, // Add this import
    error::Error, // Add this import
};

use tokio::time::Instant;

#[derive(Debug)]
pub struct Db {
    values: HashMap<String, DbValue>,
    expirations: HashMap<String, Instant>,
}

#[derive(Clone, Debug)]
pub enum DbValue {
    Atom(String),
    List(VecDeque<String>),
    Stream(StreamList),
}

#[derive(Clone, Debug)]
pub struct StreamList(pub Vec<StreamItem>);

#[derive(Clone, Debug)]
pub struct StreamItem {
    pub id: String,
    pub values: HashMap<String, String>,
}

// Custom error enum for Db operations
#[derive(Debug)]
pub enum DbError {
    KeyNotFound(String),
    KeyIsNotStream(String),
    StreamStartIdNotFound(String),
    StreamEndIdNotFound(String),
}

// Implement Display trait for DbError to provide user-friendly messages
impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DbError::KeyNotFound(key) => write!(f, "Key '{}' not found", key),
            DbError::KeyIsNotStream(key) => write!(f, "Key '{}' exists but is not a stream", key),
            DbError::StreamStartIdNotFound(id) => write!(f, "Stream start ID '{}' not found", id),
            DbError::StreamEndIdNotFound(id) => write!(f, "Stream end ID '{}' not found", id),
        }
    }
}

// Implement std::error::Error trait for DbError
impl Error for DbError {}


impl Db {
    pub fn new() -> Self {
        Self {
            values: HashMap::new(),
            expirations: HashMap::new(),
        }
    }

    pub fn insert(&mut self, key: &str, value: DbValue) {
        self.values.insert(key.to_owned(), value);
    }

    pub fn set_expiration(&mut self, key: &str, millis: u64) {
        self.expirations.insert(
            key.to_owned(),
            Instant::now() + Duration::from_millis(millis),
        );
    }

    pub fn rpush(&mut self, key: &str, values: Vec<String>) -> u64 {
        if !self.values.contains_key(key) {
            self.values
                .insert(key.to_owned(), DbValue::List(VecDeque::new()));
        }
        if let Some(db_value) = self.values.get_mut(key)
            && let DbValue::List(list) = db_value
        {
            list.extend(values);
            return list.len() as u64;
        }
        0
    }

    pub fn lpush(&mut self, key: &str, values: Vec<String>) -> u64 {
        if !self.values.contains_key(key) {
            self.values
                .insert(key.to_owned(), DbValue::List(VecDeque::new()));
        }
        if let Some(db_value) = self.values.get_mut(key)
            && let DbValue::List(list) = db_value
        {
            for value in values.into_iter() {
                list.push_front(value);
            }
            return list.len() as u64;
        }
        0
    }

    pub fn lpop(&mut self, key: &str, length: usize) -> Vec<String> {
        if let Some(db_value) = self.values.get_mut(key)
            && let DbValue::List(list) = db_value
            && !list.is_empty()
        {
            let mut poped_list: Vec<String> = Vec::new();
            for _ in 0..length {
                let value = list.pop_front();
                if let Some(value) = value {
                    poped_list.push(value);
                } else {
                    break;
                }
            }
            return poped_list;
        }
        vec![]
    }

    pub fn llen(&mut self, key: &str) -> u64 {
        if let Some(db_value) = self.values.get_mut(key)
            && let DbValue::List(list) = db_value
        {
            return list.len() as u64;
        }
        0
    }

    pub fn is_expired(&mut self, key: &str) -> bool {
        if let Some(expiration) = self.expirations.get(key)
            && Instant::now() >= *expiration
        {
            return true;
        }
        false
    }

    pub fn expire(&mut self, key: &str) {
        self.expirations.remove(key);
        self.values.remove(key);
    }

    pub fn get(&mut self, key: &str) -> Option<DbValue> {
        self.values.get(key).cloned()
    }

    pub fn lrange(&mut self, key: &str, start: isize, stop: isize) -> DbValue {
        if let Some(db_value) = self.values.get(key)
            && let DbValue::List(list) = db_value
        {
            let length = list.len();

            let start = if start < 0 {
                length as isize + start
            } else {
                start
            }
            .max(0) as usize;

            let stop = if stop < 0 {
                length as isize + stop
            } else {
                stop
            }
            .max(0) as usize;

            if start < length && start < stop {
                let stop = stop.min(list.len() - 1);
                return DbValue::List(list.range(start..=stop).cloned().collect());
            }
        }
        DbValue::List(VecDeque::new())
    }

    pub fn xadd(&mut self, key: &str, id: &str, values: HashMap<String, String>) {
        if !self.values.contains_key(key) {
            self.values
                .insert(key.to_owned(), DbValue::Stream(StreamList(vec![])));
        }
        if let Some(db_value) = self.values.get_mut(key)
            && let DbValue::Stream(stream) = db_value
        {
            stream.0.push(StreamItem {
                id: id.into(),
                values,
            });
        }
    }

    pub fn xrange(&mut self, key: &str, start: &str, end: &str) -> Result<&[StreamItem], DbError> {
        let value = self.values.get(key);

        match value {
            Some(DbValue::Stream(stream_list)) => {
                let first_index = stream_list
                    .0
                    .binary_search_by_key(&start, |stream_item| &stream_item.id)
                    .map_err(|_| DbError::StreamStartIdNotFound(start.to_string()))?;

                let last_index = stream_list
                    .0
                    .binary_search_by_key(&end, |stream_item| &stream_item.id)
                    .map_err(|_| DbError::StreamEndIdNotFound(end.to_string()))?;

                Ok(&stream_list.0[first_index..=last_index])
            }
            Some(_) => {
                // Key exists but is not a stream type
                Err(DbError::KeyIsNotStream(key.to_string()))
            }
            None => {
                // Key does not exist in the database
                Err(DbError::KeyNotFound(key.to_string()))
            }
        }
    }

    pub fn xread(&mut self, key: &str, start: &str) -> &[StreamItem] {
        if let Some(value) = self.values.get(key)
            && let DbValue::Stream(stream_list) = value
        {
            let search = stream_list
                .0
                .binary_search_by_key(&start, |stream_item| &stream_item.id);

            let first_index = match search {
                Ok(index) => index + 1,
                Err(index) => {
                    if index > 0 {
                        index - 1
                    } else {
                        0
                    }
                }
            };

            return &stream_list.0[first_index..];
        }
        &[]
    }
}
