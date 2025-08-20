mod resp;

use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use resp::Value;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let pairs: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let stream = listener.accept().await;
        let pairs_by_stream = pairs.clone();
        match stream {
            Ok((stream, _add)) => {
                tokio::spawn(async move { handle_conn(stream, pairs_by_stream).await });
            }
            Err(e) => {
                println!("{e}");
            }
        }
    }
}

// *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n

async fn handle_conn(stream: TcpStream, pairs: Arc<Mutex<HashMap<String, String>>>) {
    let mut handler = resp::RespHandler::new(stream);

    loop {
        let value = handler.read_value().await.unwrap();
        let response = if let Some(v) = value {
            let (command, args) = extract_command(v).unwrap();
            match command.to_lowercase().as_str() {
                "ping" => Value::SimpleString("PONG".to_string()),
                "echo" => args.first().unwrap().clone(),
                "set" => {
                    let key = args.first().unwrap().clone();
                    let value = args.get(1).unwrap().clone();
                    pairs.lock().await.insert(key.into(), value.into());
                    Value::SimpleString("OK".to_string())
                }
                "get" => {
                    let key: String = args.first().unwrap().clone().into();
                    let value = pairs.lock().await.get(&key).unwrap().clone();
                    Value::SimpleString(value.to_string())
                }
                c => panic!("Cannot handle command {c}"),
            }
        } else {
            break;
        };

        println!("Sending value {response:?}");

        handler.write_value(response).await.unwrap();
    }
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
