use std::{
    collections::{HashMap, VecDeque},
    error::Error,
    fmt,
    time::Duration,
};

use tokio::{sync::mpsc, time::Instant};
use uuid::Uuid;

use crate::resp::RespValue;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct StreamNotification {
    pub key: String,
    pub item: StreamItem,
}

#[derive(Debug)]
pub enum ClientSender {
    Stream(mpsc::Sender<StreamNotification>),
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct BlockedClient {
    id: String,
    key: String,
    blocked_since: Instant,
    sender: ClientSender,
    xread_start: Option<String>,
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct BlockingQueue {
    waiting_clients: HashMap<String, VecDeque<BlockedClient>>,
}

impl BlockingQueue {
    pub fn new() -> Self {
        Self {
            waiting_clients: HashMap::new(),
        }
    }

    pub fn add_blocked_xread_client(
        &mut self,
        key: String,
        start: String,
        sender: mpsc::Sender<StreamNotification>,
    ) -> String {
        let client_id = Uuid::new_v4().to_string();
        let client = BlockedClient {
            id: client_id.clone(),
            key: key.clone(),
            blocked_since: Instant::now(),
            sender: ClientSender::Stream(sender),
            xread_start: Some(start),
        };
        self.waiting_clients
            .entry(key)
            .or_default()
            .push_back(client);
        client_id
    }

    pub fn remove_blocked_xread_client(&mut self, client_id: &str, key: &str) {
        if let Some(queue) = self.waiting_clients.get_mut(key) {
            queue.retain(|client| client.id != client_id);
        }
    }

    pub fn notify_xread_clients(&mut self, key: String, item: StreamItem) {
        if let Some(queue) = self.waiting_clients.get_mut(&key) {
            let notification = StreamNotification {
                key: key.clone(),
                item,
            };
            queue.retain(|client| match &client.sender {
                ClientSender::Stream(sender) => sender.try_send(notification.clone()).is_ok(),
            });
        }
    }
}

#[derive(Debug)]
pub struct Db {
    values: HashMap<String, DbValue>,
    expirations: HashMap<String, Instant>,
    blocking_queue: BlockingQueue,
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

impl StreamItem {
    pub fn to_resp(&self) -> RespValue {
        let values_array_items = self
            .values
            .iter()
            .flat_map(|(k, v)| {
                vec![
                    RespValue::BulkString(k.clone()),
                    RespValue::BulkString(v.clone()),
                ]
            })
            .collect();

        RespValue::Array(vec![
            RespValue::BulkString(self.id.clone()),
            RespValue::Array(values_array_items),
        ])
    }
}

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
            DbError::KeyIsNotStream(key) => write!(f, "Key '{key}' exists but is not a stream"),
            DbError::StreamStartIdNotFound(id) => write!(f, "Stream start ID '{id}' not found"),
            DbError::StreamEndIdNotFound(id) => write!(f, "Stream end ID '{id}' not found"),
        }
    }
}

impl Error for DbError {}

impl Db {
    pub fn new() -> Self {
        Self {
            values: HashMap::new(),
            expirations: HashMap::new(),
            blocking_queue: BlockingQueue::new(),
        }
    }

    pub fn add_blocked_xread_client(
        &mut self,
        key: String,
        start: String,
        sender: mpsc::Sender<StreamNotification>,
    ) -> String {
        self.blocking_queue
            .add_blocked_xread_client(key, start, sender)
    }

    pub fn remove_blocked_xread_client(&mut self, client_id: &str, key: &str) {
        self.blocking_queue
            .remove_blocked_xread_client(client_id, key)
    }

    pub fn get(&mut self, key: &str) -> Option<DbValue> {
        self.values.get(key).cloned()
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
        let stream = self
            .values
            .entry(key.to_string())
            .or_insert_with(|| DbValue::Stream(StreamList(vec![])));

        if let DbValue::Stream(stream) = stream {
            let stream_item = StreamItem {
                id: id.into(),
                values,
            };
            stream.0.push(stream_item.clone());
            self.blocking_queue
                .notify_xread_clients(key.to_string(), stream_item);
        }
    }

    pub fn xfirst(&self, key: &str) -> Option<&StreamItem> {
        if let Some(value) = self.values.get(key)
            && let DbValue::Stream(stream_list) = value
        {
            stream_list.0.first()
        } else {
            None
        }
    }

    pub fn xlast(&self, key: &str) -> Option<&StreamItem> {
        if let Some(value) = self.values.get(key)
            && let DbValue::Stream(stream_list) = value
        {
            stream_list.0.last()
        } else {
            None
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
            Some(_) => Err(DbError::KeyIsNotStream(key.to_string())),
            None => Err(DbError::KeyNotFound(key.to_string())),
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
