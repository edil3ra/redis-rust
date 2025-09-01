use super::{
    Command,
    xstream_helpers::{XreadDuration, XreadStartId},
};
use crate::resp::RespValue;
use anyhow::{Result, anyhow};

pub fn parse_command(command_name: String, args: Vec<RespValue>) -> Result<Command> {
    match command_name.to_uppercase().as_str() {
        "PING" => {
            if !args.is_empty() {
                return Err(anyhow!("PING command takes no arguments"));
            }
            Ok(Command::Ping)
        }
        "ECHO" => {
            let message: String = args
                .first()
                .ok_or_else(|| anyhow!("ECHO command requires an argument"))?
                .clone()
                .into();
            Ok(Command::Echo { message })
        }
        "SET" => {
            let key: String = args
                .first()
                .ok_or_else(|| anyhow!("SET command requires a key"))?
                .clone()
                .into();

            let value: String = args
                .get(1)
                .ok_or_else(|| anyhow!("SET command requires a value"))?
                .clone()
                .into();

            let mut expiry_millis: Option<u64> = None;

            if let Some(px_arg) = args.get(2) {
                let px_str: String = px_arg.clone().into();
                if px_str.to_uppercase() == "PX" {
                    let millis_str: String = args
                        .get(3)
                        .ok_or_else(|| anyhow!("Missing milliseconds value for PX"))?
                        .clone()
                        .into();
                    expiry_millis = Some(
                        millis_str
                            .parse::<u64>()
                            .map_err(|e| anyhow!("Invalid PX value: {}", e))?,
                    );
                    if args.len() > 4 {
                        return Err(anyhow!("Too many arguments for SET command"));
                    }
                } else {
                    return Err(anyhow!(
                        "Unknown argument after value. Expected 'PX' or end of command."
                    ));
                }
            } else if args.len() > 2 {
                return Err(anyhow!("Too many arguments for SET command"));
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
                .ok_or_else(|| anyhow!("RPUSH command requires a key"))?
                .clone()
                .into();
            if args.len() < 2 {
                return Err(anyhow!("RPUSH command requires at least one value"));
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
                .ok_or_else(|| anyhow!("LPUSH command requires a key"))?
                .clone()
                .into();
            if args.len() < 2 {
                return Err(anyhow!("LPUSH command requires at least one value"));
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
                .ok_or_else(|| anyhow!("LPOP command requires a key"))?
                .clone()
                .into();

            let count: usize = args.get(1).map(|v| v.clone().into()).unwrap_or(1);

            if args.len() > 2 {
                return Err(anyhow!("Too many arguments for LPOP command"));
            }

            Ok(Command::Lpop { key, count })
        }
        "BLPOP" => {
            let key: String = args
                .first()
                .ok_or_else(|| anyhow!("BLPOP command requires a key"))?
                .clone()
                .into();

            let timeout_seconds: f64 = args.get(1).map(|v| v.clone().into()).unwrap_or(0.0);

            if args.len() > 2 {
                return Err(anyhow!("Too many arguments for BLPOP command"));
            }

            Ok(Command::Blpop {
                key,
                timeout_seconds,
            })
        }
        "LLEN" => {
            let key: String = args
                .first()
                .ok_or_else(|| anyhow!("LLEN command requires a key"))?
                .clone()
                .into();

            if args.len() > 1 {
                return Err(anyhow!("Too many arguments for LLEN command"));
            }

            Ok(Command::Llen { key })
        }
        "GET" => {
            let key: String = args
                .first()
                .ok_or_else(|| anyhow!("GET command requires a key"))?
                .clone()
                .into();

            if args.len() > 1 {
                return Err(anyhow!("Too many arguments for GET command"));
            }

            Ok(Command::Get { key })
        }
        "LRANGE" => {
            let key: String = args
                .first()
                .ok_or_else(|| anyhow!("LRANGE command requires a key"))?
                .clone()
                .into();

            let start: isize = args
                .get(1)
                .ok_or_else(|| anyhow!("LRANGE command requires a start value"))?
                .clone()
                .into();

            let stop: isize = args
                .get(2)
                .ok_or_else(|| anyhow!("LRANGE command requires a stop value"))?
                .clone()
                .into();

            if args.len() > 3 {
                return Err(anyhow!("Too many arguments for LRANGE command"));
            }

            Ok(Command::Lrange { key, start, stop })
        }
        "TYPE" => {
            let key: String = args
                .first()
                .ok_or_else(|| anyhow!("TYPE command requires a key"))?
                .clone()
                .into();

            Ok(Command::Type { key })
        }
        "XADD" => {
            let key: String = args
                .first()
                .ok_or_else(|| anyhow!("XADD command requires a key"))?
                .clone()
                .into();

            let id: String = args
                .get(1)
                .ok_or_else(|| anyhow!("XADD command requires an id"))?
                .clone()
                .into();

            let remaining_args = &args[2..];

            if !remaining_args.len().is_multiple_of(2) {
                return Err(anyhow!(
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
                .ok_or_else(|| anyhow!("XRANGE command requires a key"))?
                .clone()
                .into();

            let start = args.get(1).map(|s| s.clone().into());
            let end = args.get(2).map(|s| s.clone().into());

            Ok(Command::Xrange { key, start, end })
        }

        "XREAD" => {
            let first_arg: String = args
                .first()
                .ok_or_else(|| anyhow!("XREAD command requires stream or block as first arg"))?
                .clone()
                .into();

            let is_firt_arg_block = first_arg.to_uppercase() == "BLOCK";
            let duration = if is_firt_arg_block {
                let duration: u64 = args
                    .get(1)
                    .ok_or_else(|| {
                        anyhow!("XREAD command requires duration in millis after block")
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
                .ok_or_else(|| anyhow!("XREAD command requires stream or block as first arg"))?
                .clone()
                .into();

            if stream_arg.to_uppercase() != "STREAMS" {
                return Err(anyhow!("Expected 'streams' keyword"));
            }

            let remaining_args = &remaining_args[1..];
            if !remaining_args.len().is_multiple_of(2) {
                return Err(anyhow!(
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

        c => Err(anyhow!("Unknown command: {}", c)),
    }
}

pub fn extract_command(value: RespValue) -> Result<(String, Vec<RespValue>)> {
    match value {
        RespValue::Array(a) => {
            if a.is_empty() {
                return Err(anyhow!("Empty array received as command"));
            }
            Ok((
                unpack_bulk_str(a.first().unwrap().clone())?,
                a.into_iter().skip(1).collect(),
            ))
        }
        _ => Err(anyhow!("Unexpected command format")),
    }
}

fn unpack_bulk_str(value: RespValue) -> Result<String> {
    match value {
        RespValue::BulkString(s) => Ok(s),
        RespValue::SimpleString(s) => Ok(s),
        _ => Err(anyhow!(
            "Expected command name to be a bulk or simple string"
        )),
    }
}
