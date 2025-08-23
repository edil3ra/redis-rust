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
    time::{self, Instant},
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

const WAITING_TIME_FOR_BPLOP_MILLI: u64 = 10;

impl Db {
    fn new() -> Self {
        Self {
            values: HashMap::new(),
            expirations: HashMap::new(),
        }
    }

    fn insert_atom(&mut self, key: &str, value: String) {
        self.values.insert(key.to_owned(), DbValue::Atom(value));
    }

    fn set_expiration(&mut self, key: &str, millis: u64) {
        self.expirations.insert(
            key.to_owned(),
            Instant::now() + Duration::from_millis(millis),
        );
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

    fn is_expired(&mut self, key: &str) -> bool {
        if let Some(expiration) = self.expirations.get(key)
            && Instant::now() >= *expiration
        {
            return true;
        }
        false
    }

    fn expire(&mut self, key: &str) {
        self.expirations.remove(key);
        self.values.remove(key);
    }

    fn get(&mut self, key: &str) -> Option<DbValue> {
        self.values.get(key).cloned()
    }

    fn lrange(&mut self, key: &str, start: isize, stop: isize) -> DbValue {
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
}

// --- New Command Enum ---
enum Command {
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
}

impl Command {
    async fn execute(self, db: Arc<Mutex<Db>>) -> Result<RespValue> {
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
                db.insert_atom(&key, value);
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
                        DbValue::List(_) => {
                            Ok(RespValue::NullBulkString)
                        }
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
        }
    }
}


fn parse_command(command_name: String, args: Vec<RespValue>) -> Result<Command> {
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
        c => Err(anyhow::anyhow!("Unknown command: {}", c)),
    }
}

async fn handle_conn(stream: TcpStream, db: Arc<Mutex<Db>>) -> Result<()> {
    let mut handler = resp::RespHandler::new(stream);

    loop {
        let input = handler.read_value().await?; 
        let response = if let Some(input) = input {
            let (command_name, args) = extract_command(input)?;
            let command = parse_command(command_name, args)?;
            command.execute(db.clone()).await? 
        } else {
            break; 
        };
        handler.write_value(response).await?;
    }

    Ok(())
}

fn extract_command(value: RespValue) -> Result<(String, Vec<RespValue>)> {
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
