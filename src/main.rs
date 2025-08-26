mod commands;
mod db;
mod resp;

use std::sync::Arc;

use anyhow::Result;
use commands::*;
use db::*;
use resp::RespValue;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

async fn handle_conn(stream: TcpStream, db: Arc<Mutex<Db>>) -> Result<()> {
    let mut handler = resp::RespHandler::new(stream);

    loop {
        let input = handler.read_value().await?;
        let response = if let Some(input) = input {
            let (command_name, args) = extract_command(input)?;
            let command = parse_command(command_name, args)?;
            match command.execute(db.clone()).await {
                Ok(resp_value) => resp_value,
                Err(e) => RespValue::SimpleError(format!("{e}")),
            }
        } else {
            break;
        };
        handler.write_value(response).await?;
    }

    Ok(())
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
