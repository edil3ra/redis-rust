use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Result, bail};

use tokio::{
    sync::{Mutex, mpsc},
    time::{self, Instant},
};

use crate::{
    db::{Db, DbValue, StreamNotification},
    resp::RespValue,
};

const WAITING_TIME_FOR_BPLOP_MILLI: u64 = 10;

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

#[derive(Debug, Clone)]
pub enum XreadDuration {
    None,
    Inifnity,
    Normal(u64),
}

#[derive(Debug, Clone)]
pub enum XreadStartId {
    Last,
    Normal(String),
}

impl XreadStartId {
    fn to_str(&self, last_id: &str) -> String {
        match self {
            XreadStartId::Last => last_id.into(),
            XreadStartId::Normal(s) => s.into(),
        }
    }
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
                let length = db.lock().await.rpush(&key, values);
                Ok(RespValue::Integer(length))
            }
            Command::Lpush { key, values } => {
                let length = db.lock().await.lpush(&key, values);
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
                let end_time = if timeout_seconds == 0. {
                    None
                } else {
                    Some(Instant::now() + Duration::from_secs_f64(timeout_seconds))
                };

                loop {
                    {
                        let mut db_g = db.lock().await;
                        let results = db_g.lpop(&key, 1);
                        if !results.is_empty() {
                            return Ok(RespValue::Array(
                                std::iter::once(RespValue::BulkString(key))
                                    .chain(results.into_iter().map(RespValue::BulkString))
                                    .collect(),
                            ));
                        }
                    }

                    if let Some(end) = end_time
                        && Instant::now() > end
                    {
                        return Ok(RespValue::NullArray);
                    }
                    time::sleep(Duration::from_millis(WAITING_TIME_FOR_BPLOP_MILLI)).await;
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
                );
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
                            let start_id = start.to_str(&db_g.xlast(key).unwrap().id);
                            let stream_items = db_g.xread(key, &start_id);

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
                        let start_id = {
                            let db_g = db.lock().await;
                            start.to_str(&db_g.xlast(&key).unwrap().id)
                        };

                        let client_id =
                            db.lock()
                                .await
                                .add_blocked_xread_client(key.clone(), start_id.clone(), sender);

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
                        db_g.remove_blocked_xread_client(&client_id, &key);

                        let stream_items = db_g.xread(&key, &start_id);
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

pub fn parse_command(command_name: String, args: Vec<RespValue>) -> Result<Command> {
    match command_name.to_uppercase().as_str() {
        "PING" => {
            if !args.is_empty() {
                return Err(anyhow::anyhow!("PING command takes no arguments"));
            }
            Ok(Command::Ping)
        }
        "ECHO" => {
            let message: String = args
                .first()
                .ok_or_else(|| anyhow::anyhow!("ECHO command requires an argument"))?
                .clone()
                .into();
            Ok(Command::Echo { message })
        }
        "SET" => {
            let key: String = args
                .first()
                .ok_or_else(|| anyhow::anyhow!("SET command requires a key"))?
                .clone()
                .into();

            let value: String = args
                .get(1)
                .ok_or_else(|| anyhow::anyhow!("SET command requires a value"))?
                .clone()
                .into();

            let mut expiry_millis: Option<u64> = None;

            if let Some(px_arg) = args.get(2) {
                let px_str: String = px_arg.clone().into();
                if px_str.to_uppercase() == "PX" {
                    let millis_str: String = args
                        .get(3)
                        .ok_or_else(|| anyhow::anyhow!("Missing milliseconds value for PX"))?
                        .clone()
                        .into();
                    expiry_millis = Some(
                        millis_str
                            .parse::<u64>()
                            .map_err(|e| anyhow::anyhow!("Invalid PX value: {}", e))?,
                    );
                    if args.len() > 4 {
                        return Err(anyhow::anyhow!("Too many arguments for SET command"));
                    }
                } else {
                    return Err(anyhow::anyhow!(
                        "Unknown argument after value. Expected 'PX' or end of command."
                    ));
                }
            } else if args.len() > 2 {
                return Err(anyhow::anyhow!("Too many arguments for SET command"));
            }

            Ok(Command::Set {
                key,
                value,
                expiry_millis,
            })
        }
        "RPUSH" => {
            let key = args
                .first()
                .ok_or_else(|| anyhow::anyhow!("RPUSH command requires a key"))?
                .clone()
                .into();
            if args.len() < 2 {
                return Err(anyhow::anyhow!("RPUSH command requires at least one value"));
            }

            let values = args[1..]
                .iter()
                .map(|resp_value| resp_value.clone().into())
                .collect::<Vec<String>>();

            Ok(Command::Rpush { key, values })
        }
        "LPUSH" => {
            let key = args
                .first()
                .ok_or_else(|| anyhow::anyhow!("LPUSH command requires a key"))?
                .clone()
                .into();
            if args.len() < 2 {
                return Err(anyhow::anyhow!("LPUSH command requires at least one value"));
            }

            let values = args[1..]
                .iter()
                .map(|resp_value| resp_value.clone().into())
                .collect::<Vec<String>>();

            Ok(Command::Lpush { key, values })
        }
        "LPOP" => {
            let key: String = args
                .first()
                .ok_or_else(|| anyhow::anyhow!("LPOP command requires a key"))?
                .clone()
                .into();

            let count: usize = args.get(1).map(|v| v.clone().into()).unwrap_or(1);

            if args.len() > 2 {
                return Err(anyhow::anyhow!("Too many arguments for LPOP command"));
            }

            Ok(Command::Lpop { key, count })
        }
        "BLPOP" => {
            let key: String = args
                .first()
                .ok_or_else(|| anyhow::anyhow!("BLPOP command requires a key"))?
                .clone()
                .into();

            let timeout_seconds: f64 = args.get(1).map(|v| v.clone().into()).unwrap_or(0.0);

            if args.len() > 2 {
                return Err(anyhow::anyhow!("Too many arguments for BLPOP command"));
            }

            Ok(Command::Blpop {
                key,
                timeout_seconds,
            })
        }
        "LLEN" => {
            let key: String = args
                .first()
                .ok_or_else(|| anyhow::anyhow!("LLEN command requires a key"))?
                .clone()
                .into();

            if args.len() > 1 {
                return Err(anyhow::anyhow!("Too many arguments for LLEN command"));
            }

            Ok(Command::Llen { key })
        }
        "GET" => {
            let key: String = args
                .first()
                .ok_or_else(|| anyhow::anyhow!("GET command requires a key"))?
                .clone()
                .into();

            if args.len() > 1 {
                return Err(anyhow::anyhow!("Too many arguments for GET command"));
            }

            Ok(Command::Get { key })
        }
        "LRANGE" => {
            let key: String = args
                .first()
                .ok_or_else(|| anyhow::anyhow!("LRANGE command requires a key"))?
                .clone()
                .into();

            let start: isize = args
                .get(1)
                .ok_or_else(|| anyhow::anyhow!("LRANGE command requires a start value"))?
                .clone()
                .into();

            let stop: isize = args
                .get(2)
                .ok_or_else(|| anyhow::anyhow!("LRANGE command requires a stop value"))?
                .clone()
                .into();

            if args.len() > 3 {
                return Err(anyhow::anyhow!("Too many arguments for LRANGE command"));
            }

            Ok(Command::Lrange { key, start, stop })
        }
        "TYPE" => {
            let key: String = args
                .first()
                .ok_or_else(|| anyhow::anyhow!("TYPE command requires a key"))?
                .clone()
                .into();

            Ok(Command::Type { key })
        }
        "XADD" => {
            let key: String = args
                .first()
                .ok_or_else(|| anyhow::anyhow!("XADD command requires a key"))?
                .clone()
                .into();

            let id: String = args
                .get(1)
                .ok_or_else(|| anyhow::anyhow!("XADD command requires an id"))?
                .clone()
                .into();

            let remaining_args = &args[2..];

            if !remaining_args.len().is_multiple_of(2) {
                return Err(anyhow::anyhow!(
                    "XADD command requires an even number of field-value pairs"
                ));
            }

            let field_value_pairs: Vec<(String, String)> = remaining_args
                .chunks_exact(2)
                .map(|chunk| {
                    let field: String = chunk[0].clone().into();
                    let value: String = chunk[1].clone().into();
                    (field, value)
                })
                .collect();

            Ok(Command::Xadd {
                key,
                id,
                field_value_pairs,
            })
        }

        "XRANGE" => {
            let key: String = args
                .first()
                .ok_or_else(|| anyhow::anyhow!("XRANGE command requires a key"))?
                .clone()
                .into();

            let start = args.get(1).map(|s| s.clone().into());
            let end = args.get(2).map(|s| s.clone().into());

            Ok(Command::Xrange { key, start, end })
        }

        "XREAD" => {
            let first_arg: String = args
                .first()
                .ok_or_else(|| {
                    anyhow::anyhow!("XREAD command requires stream or block as first arg")
                })?
                .clone()
                .into();

            let is_firt_arg_block = first_arg.to_uppercase() == "BLOCK";
            let duration = if is_firt_arg_block {
                let duration: u64 = args
                    .get(1)
                    .ok_or_else(|| {
                        anyhow::anyhow!("XREAD command requires duration in millis after block")
                    })?
                    .clone()
                    .into();
                if duration == 0 {
                    XreadDuration::Inifnity
                } else {
                    XreadDuration::Normal(duration)
                }
            } else {
                XreadDuration::None
            };

            let remaining_args = if is_firt_arg_block {
                &args[2..]
            } else {
                &args[..]
            };

            let stream_arg: String = remaining_args
                .first()
                .ok_or_else(|| {
                    anyhow::anyhow!("XREAD command requires stream or block as first arg")
                })?
                .clone()
                .into();

            if stream_arg.to_uppercase() != "STREAMS" {
                return Err(anyhow::anyhow!("Expected 'streams' keyword"));
            }

            let remaining_args = &remaining_args[1..];
            if !remaining_args.len().is_multiple_of(2) {
                return Err(anyhow::anyhow!(
                    "XREAD STREAMS requires an even number of key-id pairs"
                ));
            }

            let num_streams = remaining_args.len() / 2;
            let keys_slice = &remaining_args[0..num_streams];
            let ids_slice = &remaining_args[num_streams..];

            let streams: Vec<(String, XreadStartId)> = keys_slice
                .iter()
                .zip(ids_slice.iter())
                .map(|(key_resp, id_resp)| {
                    let key: String = key_resp.clone().into();
                    let start_str: String = id_resp.clone().into();
                    let start = if start_str == "$" {
                        XreadStartId::Last
                    } else {
                        XreadStartId::Normal(start_str)
                    };
                    (key, start)
                })
                .collect();

            Ok(Command::Xread { streams, duration })
        }

        c => Err(anyhow::anyhow!("Unknown command: {}", c)),
    }
}

fn derive_new_stream_id(requested_id_str: &str, last_item_id: Option<&String>) -> Result<String> {
    let (last_ms_time, last_seq_num) = if let Some(last_id_str) = last_item_id {
        let (ms_str, seq_str) = last_id_str
            .split_once('-')
            .ok_or_else(|| anyhow::anyhow!("Invalid last stream ID format: {}", last_id_str))?;
        (ms_str.parse::<u128>()?, seq_str.parse::<u64>()?)
    } else {
        (0, 0)
    };

    let (requested_timestamp_part, requested_sequence_part) = if requested_id_str == "*" {
        ("*", "*")
    } else {
        requested_id_str
            .split_once("-")
            .ok_or_else(|| anyhow::anyhow!("Invalid stream ID format: {}", requested_id_str))?
    };

    let current_system_time_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis();

    let new_timestamp: u128 = if requested_timestamp_part == "*" {
        current_system_time_millis
    } else {
        requested_timestamp_part
            .parse()
            .map_err(|_| anyhow::anyhow!("Timestamp is not a valid number"))?
    };

    let new_sequence_number: u64 = if requested_sequence_part == "*" {
        if last_item_id.is_some() {
            if new_timestamp == last_ms_time {
                last_seq_num + 1
            } else {
                0
            }
        } else if requested_timestamp_part == "*" {
            0
        } else {
            1
        }
    } else {
        requested_sequence_part
            .parse()
            .map_err(|_| anyhow::anyhow!("Sequence is not a valid number"))?
    };

    if new_timestamp == 0 && new_sequence_number == 0 {
        bail!("ERR The ID specified in XADD must be greater than 0-0")
    }

    if last_item_id.is_some() && new_timestamp < last_ms_time
        || (new_timestamp == last_ms_time && new_sequence_number <= last_seq_num)
    {
        {
            bail!(
                "ERR The ID specified in XADD is equal or smaller than the target stream top item"
            )
        }
    }

    Ok(format!("{new_timestamp}-{new_sequence_number}"))
}

pub fn extract_command(value: RespValue) -> Result<(String, Vec<RespValue>)> {
    match value {
        RespValue::Array(a) => {
            if a.is_empty() {
                return Err(anyhow::anyhow!("Empty array received as command"));
            }
            Ok((
                unpack_bulk_str(a.first().unwrap().clone())?,
                a.into_iter().skip(1).collect(),
            ))
        }
        _ => Err(anyhow::anyhow!("Unexpected command format")),
    }
}

fn unpack_bulk_str(value: RespValue) -> Result<String> {
    match value {
        RespValue::BulkString(s) => Ok(s),
        RespValue::SimpleString(s) => Ok(s),
        _ => Err(anyhow::anyhow!(
            "Expected command name to be a bulk or simple string"
        )),
    }
}
