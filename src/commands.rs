use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Result, bail};

use tokio::{
    sync::Mutex,
    time::{self, Instant},
};

use crate::{
    db::{Db, DbValue},
    resp::RespValue,
};

const WAITING_TIME_FOR_BPLOP_MILLI: u64 = 10;

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
                        return Ok(RespValue::NullBulkString);
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
                let new_id = if let Some(DbValue::Stream(stream_list)) = db.lock().await.get(&key) {
                    let (last_ms_time, last_seq_num) =
                        stream_list.0.last().unwrap().id.split_once("-").unwrap();
                    let last_timestamp: u128 = last_ms_time.parse().unwrap();
                    let last_sequence_number: u64 = last_seq_num.parse().unwrap_or_default();

                    let (timestamp_str, sequence_str) = {
                        if id == "*" {
                            ("*", "*")
                        } else {
                            id.split_once("-")
                                .ok_or_else(|| anyhow::anyhow!("Invalid stream Id format {id}"))?
                        }
                    };

                    let timestamp = if timestamp_str == "*" {
                        SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("Time went backwards")
                            .as_millis()
                    } else {
                        timestamp_str
                            .parse()
                            .map_err(|_| anyhow::anyhow!("Timestamp is not a valid number"))?
                    };

                    let sequence_number: i64 = if sequence_str == "*" {
                        if last_timestamp == timestamp {
                            (last_sequence_number + 1) as i64
                        } else {
                            0
                        }
                    } else {
                        sequence_str
                            .parse()
                            .map_err(|_| anyhow::anyhow!("Sequence is not a valid number"))?
                    };
                    if timestamp as i64 <= 0 && sequence_number <= 0 {
                        bail!("ERR The ID specified in XADD must be greater than 0-0")
                    }

                    if (timestamp) < last_timestamp
                        || ((timestamp) == last_timestamp
                            && (sequence_number as u64) <= last_sequence_number)
                    {
                        bail!(
                            "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                        )
                    }
                    format!("{timestamp}-{sequence_number}")
                } else {
                    let (timestamp_str, sequence_str) = {
                        if id == "*" {
                            ("*", "*")
                        } else {
                            id.split_once("-")
                                .ok_or_else(|| anyhow::anyhow!("Invalid stream Id format {id}"))?
                        }
                    };

                    let timestamp = if timestamp_str == "*" {
                        SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("Time went backwards")
                            .as_millis()
                    } else {
                        timestamp_str
                            .parse()
                            .map_err(|_| anyhow::anyhow!("Timestamp is not a valid number"))?
                    };

                    let sequence_number: i64 = if sequence_str == "*" {
                        if timestamp_str == "*" {
                            0
                        } else {
                            1
                        }
                    } else {
                        sequence_str
                            .parse()
                            .map_err(|_| anyhow::anyhow!("Sequence is not a valid number"))?
                    };
                    format!("{timestamp}-{sequence_number}")
                };

                db.lock().await.xadd(
                    &key,
                    &new_id,
                    field_value_pairs
                        .into_iter()
                        .collect::<HashMap<String, String>>(),
                );
                Ok(RespValue::BulkString(new_id))
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

        c => Err(anyhow::anyhow!("Unknown command: {}", c)),
    }
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
