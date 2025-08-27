use std::{
    collections::{HashMap, VecDeque},
    time::Duration,
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

    pub fn xrange(&mut self, key: &str, start: &str, end: &str) -> &[StreamItem] {
        if let Some(value) = self.values.get(key)
            && let DbValue::Stream(stream_list) = value
        {
            let first_index = stream_list
                .0
                .binary_search_by_key(&start, |stream_item| &stream_item.id)
                .unwrap();

            let last_index = stream_list
                .0
                .binary_search_by_key(&end, |stream_item| &stream_item.id)
                .unwrap();
            return &stream_list.0[first_index..=last_index];
        }
        &[]
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
