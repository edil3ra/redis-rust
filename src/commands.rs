pub(crate) mod parser;
pub(crate) mod xstream_helpers;

use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::Result;
use tokio::sync::{Mutex, mpsc};

use crate::{
    db::{
        Db, DbValue,
        blocking::{ListNotification, StreamNotification},
    },
    resp::RespValue,
};

use self::xstream_helpers::{XreadDuration, XreadStartId, derive_new_stream_id};

#[derive(Debug)]
pub enum Command {
    Ping,
    Echo {
        message: String,
    },
    Set {
        key: String,
        value: String,
        expiry_millis: Option<u64>,
    },
    Rpush {
        key: String,
        values: Vec<String>,
    },
    Lpush {
        key: String,
        values: Vec<String>,
    },
    Lpop {
        key: String,
        count: usize,
    },
    Blpop {
        key: String,
        timeout_seconds: f64,
    },
    Llen {
        key: String,
    },
    Get {
        key: String,
    },
    Lrange {
        key: String,
        start: isize,
        stop: isize,
    },
    Type {
        key: String,
    },
    Xadd {
        key: String,
        id: String,
        field_value_pairs: Vec<(String, String)>,
    },
    Xrange {
        key: String,
        start: Option<String>,
        end: Option<String>,
    },
    Xread {
        streams: Vec<(String, XreadStartId)>,
        duration: XreadDuration,
    },
}

impl Command {
    pub async fn execute(self, db: Arc<Mutex<Db>>) -> Result<RespValue> {
        match self {
            Command::Ping => Ok(RespValue::SimpleString("PONG".to_string())),
            Command::Echo { message } => Ok(RespValue::BulkString(message)),
            Command::Set {
                key,
                value,
                expiry_millis,
            } => {
                let mut db = db.lock().await;
                if let Some(millis) = expiry_millis {
                    db.set_expiration(&key, millis);
                }
                db.insert(&key, DbValue::Atom(value));
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::Rpush { key, values } => {
                let length = db.lock().await.rpush(&key, values)?;
                Ok(RespValue::Integer(length))
            }
            Command::Lpush { key, values } => {
                let length = db.lock().await.lpush(&key, values)?;
                Ok(RespValue::Integer(length))
            }
            Command::Lpop { key, count } => {
                let poped_list = db.lock().await.lpop(&key, count);
                if poped_list.is_empty() {
                    Ok(RespValue::NullBulkString)
                } else if poped_list.len() == 1 {
                    Ok(RespValue::BulkString(poped_list[0].clone()))
                } else {
                    Ok(RespValue::Array(
                        poped_list.into_iter().map(RespValue::BulkString).collect(),
                    ))
                }
            }
            Command::Blpop {
                key,
                timeout_seconds,
            } => {
                let initial_lpop_result = {
                    let mut db_g = db.lock().await;
                    db_g.lpop(&key, 1)
                };

                if !initial_lpop_result.is_empty() {
                    return Ok(RespValue::Array(
                        std::iter::once(RespValue::BulkString(key))
                            .chain(initial_lpop_result.into_iter().map(RespValue::BulkString))
                            .collect(),
                    ));
                }

                if timeout_seconds == 0.0 {
                    return Ok(RespValue::NullArray);
                }

                let (sender, mut receiver) = mpsc::channel::<ListNotification>(1);
                let client_id = {
                    let mut db_g = db.lock().await;
                    db_g.add_blocked_lpop_client(key.clone(), sender)
                };

                let timeout_duration = Duration::from_secs_f64(timeout_seconds);

                tokio::select! {
                    _ = tokio::time::sleep(timeout_duration) => {
                        let mut db_g = db.lock().await;
                        db_g.remove_blocked_client(&client_id, &key);
                        Ok(RespValue::NullArray)
                    },
                    Some(_notification) = receiver.recv() => {
                        let mut db_g = db.lock().await;
                        db_g.remove_blocked_client(&client_id, &key);
                        let results = db_g.lpop(&key, 1);

                        if !results.is_empty() {
                            Ok(RespValue::Array(
                                std::iter::once(RespValue::BulkString(key))
                                    .chain(results.into_iter().map(RespValue::BulkString))
                                    .collect(),
                            ))
                        } else {
                            Ok(RespValue::NullArray)
                        }
                    }
                }
            }
            Command::Llen { key } => {
                let length = db.lock().await.llen(&key);
                Ok(RespValue::Integer(length))
            }
            Command::Get { key } => {
                let (value, is_expired) = {
                    let mut db_g = db.lock().await;
                    let is_expired = db_g.is_expired(&key);
                    let value = db_g.get(&key);
                    if is_expired {
                        db_g.expire(&key);
                    }
                    (value, is_expired)
                };

                match (value, is_expired) {
                    (Some(value), false) => match value {
                        DbValue::Atom(v) => Ok(RespValue::BulkString(v.to_string())),
                        DbValue::List(_) => Ok(RespValue::NullBulkString),
                        DbValue::Stream(_) => Ok(RespValue::NullBulkString),
                    },
                    _ => Ok(RespValue::NullBulkString),
                }
            }
            Command::Lrange { key, start, stop } => {
                let db_result = db.lock().await.lrange(&key, start, stop);

                if let DbValue::List(l) = db_result {
                    let v = l.into_iter().map(RespValue::BulkString).collect();
                    Ok(RespValue::Array(v))
                } else {
                    Ok(RespValue::NullBulkString)
                }
            }
            Command::Type { key } => {
                let db_result = db.lock().await.get(&key);
                if let Some(result) = db_result {
                    match result {
                        DbValue::Atom(_) => Ok(RespValue::SimpleString("string".to_string())),
                        DbValue::List(_) => Ok(RespValue::SimpleString("list".to_string())),
                        DbValue::Stream(_) => Ok(RespValue::SimpleString("stream".to_string())),
                    }
                } else {
                    Ok(RespValue::SimpleString("none".to_string()))
                }
            }
            Command::Xadd {
                key,
                id,
                field_value_pairs,
            } => {
                let mut db_g = db.lock().await;

                let last_item_id_option = if let Some(DbValue::Stream(stream_list)) = db_g.get(&key)
                {
                    stream_list.0.last().map(|item| item.id.clone())
                } else {
                    None
                };

                let new_id = derive_new_stream_id(&id, last_item_id_option.as_ref())?;

                db_g.xadd(
                    &key,
                    &new_id,
                    field_value_pairs
                        .into_iter()
                        .collect::<HashMap<String, String>>(),
                )?;
                Ok(RespValue::BulkString(new_id))
            }

            Command::Xrange {
                key,
                start: start_opt,
                end: end_opt,
            } => {
                let mut db_g = db.lock().await;

                let start_id = start_opt.map_or_else(
                    || db_g.xfirst(&key).unwrap().id.clone(),
                    |start_val| {
                        if start_val == "-" {
                            db_g.xfirst(&key).unwrap().id.clone()
                        } else {
                            start_val
                        }
                    },
                );

                let end_id = end_opt.map_or_else(
                    || db_g.xlast(&key).unwrap().id.clone(),
                    |end_val| {
                        if end_val == "+" {
                            db_g.xlast(&key).unwrap().id.clone()
                        } else {
                            end_val
                        }
                    },
                );

                let streams = db_g
                    .xrange(&key, &start_id, &end_id)
                    .map_err(|e| anyhow::anyhow!(e.to_string()))?;

                let resp = streams
                    .iter()
                    .map(|item| {
                        let values_array_items: Vec<RespValue> = item
                            .values
                            .iter()
                            .flat_map(|(key, value)| {
                                vec![
                                    RespValue::BulkString(key.clone()),
                                    RespValue::BulkString(value.clone()),
                                ]
                            })
                            .collect();

                        let inner_values_resp_array = RespValue::Array(values_array_items);

                        RespValue::Array(vec![
                            RespValue::BulkString(item.id.clone()),
                            inner_values_resp_array,
                        ])
                    })
                    .collect::<Vec<RespValue>>();
                Ok(RespValue::Array(resp))
            }
            Command::Xread { streams, duration } => {
                {
                    let mut db_g = db.lock().await;

                    let initial_stream_responses = streams
                        .iter()
                        .filter_map(|(key, start)| {
                            let last_id_for_stream = db_g.xlast(key).map(|item| item.id.clone());
                            let start_id_str =
                                start.to_str(last_id_for_stream.as_deref().unwrap_or("0-0"));

                            db_g.xread(key, &start_id_str)
                                .ok()
                                .and_then(|stream_items| {
                                    let resp_stream_content = stream_items
                                        .iter()
                                        .map(|stream_item| stream_item.to_resp())
                                        .collect::<Vec<RespValue>>();
                                    if !resp_stream_content.is_empty() {
                                        Some(RespValue::Array(vec![
                                            RespValue::BulkString(key.to_string()),
                                            RespValue::Array(resp_stream_content),
                                        ]))
                                    } else {
                                        None
                                    }
                                })
                        })
                        .collect::<Vec<RespValue>>();

                    if !initial_stream_responses.is_empty() {
                        return Ok(RespValue::Array(initial_stream_responses));
                    }
                }

                match duration {
                    XreadDuration::None => {}
                    XreadDuration::Inifnity | XreadDuration::Normal(_) => {
                        let (sender, mut receiver) = mpsc::channel::<StreamNotification>(100);
                        let stream = streams[0].clone();
                        let (key, start) = stream;
                        let start_id_str = {
                            let db_g = db.lock().await;
                            let last_id = db_g.xlast(&key).map(|item| item.id.clone());
                            start.to_str(last_id.as_deref().unwrap_or("0-0"))
                        };

                        let client_id = db.lock().await.add_blocked_xread_client(
                            key.clone(),
                            start_id_str.clone(),
                            sender,
                        );

                        tokio::select! {
                            _ = async {
                                match duration {
                                    XreadDuration::Inifnity => {
                                        std::future::pending::<()>().await;
                                    },
                                    XreadDuration::Normal(duration) => {
                                        let timeout_start = tokio::time::Instant::now();
                                        let timeout_duration = Duration::from_millis(duration);
                                        let remaining_timeout = timeout_duration.saturating_sub(timeout_start.elapsed());
                                        tokio::time::sleep(remaining_timeout).await;
                                    },
                                    XreadDuration::None => {
                                        tokio::time::sleep(Duration::from_millis(0)).await;
                                    }
                                }
                            } => {
                                // Timeout or indefinite wait completed
                            },
                            Some(_notification) = receiver.recv() => {
                                // Notification received
                            }
                        }
                        let mut db_g = db.lock().await;
                        db_g.remove_blocked_client(&client_id, &key);

                        let stream_items = db_g.xread(&key, &start_id_str)?;
                        if !stream_items.is_empty() {
                            let resp_stream_content = stream_items
                                .iter()
                                .map(|stream_item| stream_item.to_resp())
                                .collect::<Vec<RespValue>>();
                            return Ok(RespValue::Array(vec![RespValue::Array(vec![
                                RespValue::BulkString(key.to_string()),
                                RespValue::Array(resp_stream_content),
                            ])]));
                        }
                    }
                }
                Ok(RespValue::NullArray)
            }
        }
    }
}
