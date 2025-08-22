use anyhow::Result;
use bytes::BytesMut;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

#[derive(Clone, Debug)]
pub enum RespValue {
    SimpleString(String),
    Integer(u64),
    BulkString(String),
    NullBulkString,
    Array(Vec<RespValue>),
}

impl From<RespValue> for String {
    fn from(value: RespValue) -> Self {
        match value {
            RespValue::Integer(u) => u.to_string(),
            RespValue::SimpleString(s) => s,
            RespValue::BulkString(s) => s,
            _ => {
                panic!("Cannot convert to string");
            }
        }
    }
}

impl From<RespValue> for isize {
    fn from(value: RespValue) -> Self {
        match value {
            RespValue::Integer(u) => u as isize,
            RespValue::SimpleString(s) => s.parse().unwrap(),
            RespValue::BulkString(s) => s.parse().unwrap(),
            _ => {
                panic!("Cannot convert to isize");
            }
        }
    }
}

impl From<RespValue> for usize {
    fn from(value: RespValue) -> Self {
        match value {
            RespValue::Integer(u) => u as usize,
            RespValue::SimpleString(s) => s.parse().unwrap(),
            RespValue::BulkString(s) => s.parse().unwrap(),
            _ => {
                panic!("Cannot convert to usize");
            }
        }
    }
}

impl RespValue {
    pub fn serialize(self) -> String {
        match self {
            RespValue::SimpleString(s) => format!("+{s}\r\n"),
            RespValue::BulkString(s) => format!("${}\r\n{}\r\n", s.chars().count(), s),
            RespValue::NullBulkString => "$-1\r\n".to_string(),
            RespValue::Integer(v) => format!(":{v}\r\n"),
            RespValue::Array(v) => {
                let length = v.len();
                let items_serialized: String = v.into_iter().map(|item| item.serialize()).collect();
                format!("*{length}\r\n{items_serialized}")
            }
        }
    }
}

pub struct RespHandler {
    stream: TcpStream,
    buffer: BytesMut,
}

impl RespHandler {
    pub fn new(stream: TcpStream) -> Self {
        RespHandler {
            stream,
            buffer: BytesMut::with_capacity(512),
        }
    }

    pub async fn read_value(&mut self) -> Result<Option<RespValue>> {
        let bytes_read = self.stream.read_buf(&mut self.buffer).await?;

        if bytes_read == 0 {
            return Ok(None);
        }

        let (v, _) = parse_message(self.buffer.split())?;
        Ok(Some(v))
    }

    pub async fn write_value(&mut self, value: RespValue) -> Result<()> {
        self.stream.write_all(value.serialize().as_bytes()).await?;

        Ok(())
    }
}

fn parse_message(buffer: BytesMut) -> Result<(RespValue, usize)> {
    match buffer[0] as char {
        '+' => parse_simple_string(buffer),
        '*' => parse_array(buffer),
        '$' => parse_bulk_string(buffer),
        _ => Err(anyhow::anyhow!("Not a known value type {buffer:?}")),
    }
}

fn parse_simple_string(buffer: BytesMut) -> Result<(RespValue, usize)> {
    if let Some((line, len)) = read_until_crlf(&buffer[1..]) {
        let string = String::from_utf8(line.to_vec()).unwrap();

        return Ok((RespValue::SimpleString(string), len + 1));
    }

    Err(anyhow::anyhow!("Invalid string {buffer:?}"))
}

fn parse_array(buffer: BytesMut) -> Result<(RespValue, usize)> {
    let (array_length, mut bytes_consumed) =
        if let Some((line, len)) = read_until_crlf(&buffer[1..]) {
            let array_length = parse_int(line)?;

            (array_length, len + 1)
        } else {
            return Err(anyhow::anyhow!("Invalid array format {:?}", buffer));
        };

    let mut items = vec![];
    for _ in 0..array_length {
        let (array_item, len) = parse_message(BytesMut::from(&buffer[bytes_consumed..]))?;

        items.push(array_item);
        bytes_consumed += len;
    }

    Ok((RespValue::Array(items), bytes_consumed))
}

fn parse_bulk_string(buffer: BytesMut) -> Result<(RespValue, usize)> {
    let (bulk_str_len, bytes_consumed) = if let Some((line, len)) = read_until_crlf(&buffer[1..]) {
        let bulk_str_len = parse_int(line)?;

        (bulk_str_len, len + 1)
    } else {
        return Err(anyhow::anyhow!("Invalid array format {:?}", buffer));
    };

    let end_of_bulk_str = bytes_consumed + bulk_str_len as usize;
    let total_parsed = end_of_bulk_str + 2;

    Ok((
        RespValue::BulkString(String::from_utf8(
            buffer[bytes_consumed..end_of_bulk_str].to_vec(),
        )?),
        total_parsed,
    ))
}

fn read_until_crlf(buffer: &[u8]) -> Option<(&[u8], usize)> {
    for i in 1..buffer.len() {
        if buffer[i - 1] == b'\r' && buffer[i] == b'\n' {
            return Some((&buffer[0..(i - 1)], i + 1));
        }
    }
    None
}

fn parse_int(buffer: &[u8]) -> Result<i64> {
    Ok(String::from_utf8(buffer.to_vec())?.parse::<i64>()?)
}
