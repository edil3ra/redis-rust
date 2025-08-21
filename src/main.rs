mod resp;

use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::Result;
use resp::Value;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Mutex,
    time::Instant,
};

struct Db {
    values: HashMap<String, String>,
    expirations: HashMap<String, Instant>,
}

enum DbGetResult {
    Ok(String),
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

    fn insert(&mut self, k: String, v: String, millis: Option<u64>) {
        if let Some(millis) = millis {
            self.expirations
                .insert(k.clone(), Instant::now() + Duration::from_millis(millis));
        }

        self.values.insert(k, v);
    }

    fn get(&mut self, key: &str) -> DbGetResult {
        if let Some(value) = self.values.get(key) {
            if let Some(instant) = self.expirations.get(key) {
                if Instant::now() >= *instant {
                    return DbGetResult::Expired;
                } else {
                    return DbGetResult::Ok(value.into());
                }
            } else {
                return DbGetResult::Ok(value.into());
            }
        }
        DbGetResult::KeyMissing
    }
}

async fn handle_conn(stream: TcpStream, db: Arc<Mutex<Db>>) -> Result<()> {
    let mut handler = resp::RespHandler::new(stream);

    loop {
        let input = handler.read_value().await.unwrap();
        let response = if let Some(input) = input {
            let (command, args) = extract_command(input)?;
            match command.to_uppercase().as_str() {
                "PING" => Value::SimpleString("PONG".to_string()),
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

                    db.lock().await.insert(key.into(), value.into(), millis_u64);
                    Value::SimpleString("OK".to_string())
                }
                "GET" => {
                    let key: String = args.first().unwrap().clone().into();
                    let value = db.lock().await.get(&key);
                    match value {
                        DbGetResult::Ok(value) => Value::SimpleString(value.to_string()),
                        DbGetResult::KeyMissing => Value::NullBulkString,
                        DbGetResult::Expired => Value::NullBulkString,
                    }
                }
                c => return Err(anyhow::anyhow!("Cannot handle command {}", c)),
            }
        } else {
            break;
        };
        dbg!(&response);
        handler.write_value(response).await?;
    }

    Ok(())
}

fn extract_command(value: Value) -> Result<(String, Vec<Value>)> {
    match value {
        Value::Array(a) => Ok((
            unpack_bulk_str(a.first().unwrap().clone())?,
            a.into_iter().skip(1).collect(),
        )),
        _ => Err(anyhow::anyhow!("Unexpected command format")),
    }
}

fn unpack_bulk_str(value: Value) -> Result<String> {
    match value {
        Value::BulkString(s) => Ok(s),
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
                tokio::spawn(async move { handle_conn(stream, db_for_stream).await });
            }
            Err(e) => {
                eprintln!("Error accepting connection: {e}");
            }
        }
    }
}

// *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
