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

async fn handle_conn(stream: TcpStream, db: Arc<Mutex<Db>>) -> Result<usize> {
    let mut handler = resp::RespHandler::new(stream);

    loop {
        let value = handler.read_value().await.unwrap();
        let response = if let Some(v) = value {
            let (command, args) = extract_command(v).unwrap();
            match command.to_uppercase().as_str() {
                "PING" => Value::SimpleString("PONG".to_string()),
                "ECHO" => args.first().unwrap().clone(),
                "SET" => {
                    let key = args.first().unwrap().clone();
                    let value = args.get(1).unwrap().clone();
                    let px: Option<String> = args.get(2).map(|px| px.clone().into());
                    let millis_string: Option<String> = args.get(3).map(|px| px.clone().into());
                    let millis_u64 = if let Some(px) = px {
                        if px.as_str().to_uppercase() == "PX" {
                            if let Some(m) = millis_string {
                                m.parse::<u64>().ok()
                            } else {
                                panic!("Missing millis");
                                // return Err(anyhow::anyhow!("Missing millis"));
                            }
                        } else {
                            panic!("Missing PX");
                            // return Err(anyhow::anyhow!("Missing PX "));
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
                        DbGetResult::KeyMissing => {
                            panic!("Key missing");
                        }
                        DbGetResult::Expired => Value::BulkString("".to_string()),
                    }
                }
                c => panic!("Cannot handle command {c}"),
            }
        } else {
            break;
        };
        handler.write_value(response).await.unwrap();
    }

    Ok(1)
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
                println!("{e}");
            }
        }
    }
}

// *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
