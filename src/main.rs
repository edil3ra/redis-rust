mod resp;

use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Duration,
};

use anyhow::Result;
use resp::RespValue;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Mutex,
    time::Instant,
};

#[derive(Debug)]
struct Db {
    values: HashMap<String, DbValue>,
    expirations: HashMap<String, Instant>,
}

#[derive(Clone, Debug)]
enum DbValue {
    Atom(String),
    List(VecDeque<String>),
}

#[derive(Clone, Debug)]
enum DbGetResult {
    Ok(DbValue),
    KeyMissing,
    Expired,
}

impl Db {
    fn new() -> Self {
        Self {
            values: HashMap::new(),
            expirations: HashMap::new(),
        }
    }

    fn insert_atom(&mut self, key: &str, value: String, millis: Option<u64>) {
        if let Some(millis) = millis {
            self.expirations.insert(
                key.to_owned(),
                Instant::now() + Duration::from_millis(millis),
            );
        }
        self.values.insert(key.to_owned(), DbValue::Atom(value));
    }

    fn rpush(&mut self, key: &str, values: Vec<String>) -> u64 {
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

    fn lpush(&mut self, key: &str, values: Vec<String>) -> u64 {
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

    fn lpop(&mut self, key: &str, length: usize) -> Vec<String> {
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

    fn llen(&mut self, key: &str) -> u64 {
        if let Some(db_value) = self.values.get_mut(key)
            && let DbValue::List(list) = db_value
        {
            return list.len() as u64;
        }
        0
    }

    fn get(&mut self, key: &str) -> DbGetResult {
        if let Some(value) = self.values.get(key) {
            if let Some(instant) = self.expirations.get(key) {
                if Instant::now() >= *instant {
                    return DbGetResult::Expired;
                } else {
                    return DbGetResult::Ok(value.clone());
                }
            } else {
                return DbGetResult::Ok(value.clone());
            }
        }
        DbGetResult::KeyMissing
    }

    fn lrange(&mut self, key: &str, start: isize, stop: isize) -> DbGetResult {
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
                return DbGetResult::Ok(DbValue::List(list.range(start..=stop).cloned().collect()));
            }
        }
        DbGetResult::Ok(DbValue::List(VecDeque::new()))
    }
}

async fn handle_conn(stream: TcpStream, db: Arc<Mutex<Db>>) -> Result<()> {
    let mut handler = resp::RespHandler::new(stream);

    loop {
        let input = handler.read_value().await.unwrap();
        let response = if let Some(input) = input {
            let (command, args) = extract_command(input)?;
            match command.to_uppercase().as_str() {
                "PING" => RespValue::SimpleString("PONG".to_string()),
                "ECHO" => {
                    let arg = args
                        .first()
                        .ok_or_else(|| anyhow::anyhow!("ECHO command requires an argument"));
                    arg.cloned()?
                }
                "SET" => {
                    let key_value = args
                        .first()
                        .ok_or_else(|| anyhow::anyhow!("SET command requires a key"))?;
                    let key = key_value.clone();

                    let val_value = args
                        .get(1)
                        .ok_or_else(|| anyhow::anyhow!("SET command requires a value"))?;
                    let value = val_value.clone();

                    let px: Option<String> = args.get(2).map(|px| px.clone().into());
                    let millis_string: Option<String> = args.get(3).map(|px| px.clone().into());
                    let millis_u64: Option<u64> = if let Some(px) = px {
                        if px.as_str().to_uppercase() == "PX" {
                            if let Some(m) = millis_string {
                                let milli = m
                                    .parse::<u64>()
                                    .map_err(|e| anyhow::anyhow!("Invalid PX value: {}", e))?;
                                Some(milli)
                            } else {
                                return Err(anyhow::anyhow!("Missing milliseconds value for PX"));
                            }
                        } else {
                            return Err(anyhow::anyhow!(
                                "Unknown argument after value. Expected 'PX' or end of command."
                            ));
                        }
                    } else {
                        None
                    };
                    db.lock()
                        .await
                        .insert_atom(&String::from(key), value.into(), millis_u64);
                    RespValue::SimpleString("OK".to_string())
                }

                "RPUSH" => {
                    let key = args
                        .first()
                        .ok_or_else(|| anyhow::anyhow!("RPUSH command requires a key"))?
                        .clone();
                    if args.len() < 2 {
                        return Err(anyhow::anyhow!("RPUSH command requires at least one value"));
                    }

                    let values = args[1..]
                        .iter()
                        .map(|resp_value| resp_value.clone().into())
                        .collect::<Vec<String>>();

                    let length = db.lock().await.rpush(&String::from(key), values);
                    RespValue::Integer(length)
                }

                "LPUSH" => {
                    let key = args
                        .first()
                        .ok_or_else(|| anyhow::anyhow!("RPUSH command requires a key"))?
                        .clone();
                    if args.len() < 2 {
                        return Err(anyhow::anyhow!("RPUSH command requires at least one value"));
                    }

                    let values = args[1..]
                        .iter()
                        .map(|resp_value| resp_value.clone().into())
                        .collect::<Vec<String>>();

                    let length = db.lock().await.lpush(&String::from(key), values);
                    RespValue::Integer(length)
                }

                "LPOP" => {
                    let key = args
                        .first()
                        .ok_or_else(|| anyhow::anyhow!("LPOP command requires a key"))?
                        .clone();

                    let length: usize =
                        args.get(1).unwrap_or(&RespValue::Integer(1)).clone().into();

                    let poped_list = db.lock().await.lpop(&String::from(key), length);
                    if poped_list.is_empty() {
                        RespValue::NullBulkString
                    } else if poped_list.len() == 1 {
                        RespValue::SimpleString(poped_list[0].clone())
                    }
                    else {
                        RespValue::Array(
                            poped_list.into_iter().map(RespValue::BulkString).collect(),
                        )
                    }
                }

                "LLEN" => {
                    let key = args
                        .first()
                        .ok_or_else(|| anyhow::anyhow!("LLEN command requires a key"))?
                        .clone();

                    let length = db.lock().await.llen(&String::from(key));
                    RespValue::Integer(length)
                }

                "GET" => {
                    let key: String = args.first().unwrap().clone().into();
                    let db_result = db.lock().await.get(&key);
                    match db_result {
                        DbGetResult::Ok(db_value) => match db_value {
                            DbValue::Atom(v) => RespValue::SimpleString(v.to_string()),
                            DbValue::List(items) => todo!(),
                        },
                        DbGetResult::KeyMissing => RespValue::NullBulkString,
                        DbGetResult::Expired => RespValue::NullBulkString,
                    }
                }

                "LRANGE" => {
                    let key: String = args
                        .first()
                        .ok_or_else(|| anyhow::anyhow!("LRANGE command requires a key"))?
                        .clone()
                        .into();

                    let start: isize = args
                        .get(1)
                        .ok_or_else(|| anyhow::anyhow!("LRANGE command require a start value"))?
                        .clone()
                        .into();

                    let stop: isize = args
                        .get(2)
                        .ok_or_else(|| anyhow::anyhow!("LRANGE command require a stop value"))?
                        .clone()
                        .into();

                    let db_result = db.lock().await.lrange(&key, start, stop);

                    if let DbGetResult::Ok(DbValue::List(l)) = db_result {
                        let v = l.into_iter().map(RespValue::BulkString).collect();
                        RespValue::Array(v)
                    } else {
                        RespValue::NullBulkString
                    }
                }

                c => return Err(anyhow::anyhow!("Cannot handle command {}", c)),
            }
        } else {
            break;
        };
        handler.write_value(response).await?;
    }

    Ok(())
}

fn extract_command(value: RespValue) -> Result<(String, Vec<RespValue>)> {
    match value {
        RespValue::Array(a) => Ok((
            unpack_bulk_str(a.first().unwrap().clone())?,
            a.into_iter().skip(1).collect(),
        )),
        _ => Err(anyhow::anyhow!("Unexpected command format")),
    }
}

fn unpack_bulk_str(value: RespValue) -> Result<String> {
    match value {
        RespValue::BulkString(s) => Ok(s),
        _ => Err(anyhow::anyhow!("Expected command to be a bulk string")),
    }
}

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let db: Arc<Mutex<Db>> = Arc::new(Mutex::new(Db::new()));

    loop {
        let stream = listener.accept().await;
        let db_for_stream = db.clone();
        match stream {
            Ok((stream, _add)) => {
                tokio::spawn(async move {
                    if let Err(e) = handle_conn(stream, db_for_stream).await {
                        eprintln!("Error handling connection: {e}");
                    }
                });
            }
            Err(e) => {
                eprintln!("Error accepting connection: {e}");
            }
        }
    }
}

// *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
