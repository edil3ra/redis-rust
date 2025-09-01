use anyhow::{Result, anyhow, bail};
use std::time::{SystemTime, UNIX_EPOCH};

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
    pub fn to_str(&self, last_id: &str) -> String {
        match self {
            XreadStartId::Last => last_id.into(),
            XreadStartId::Normal(s) => s.into(),
        }
    }
}

pub fn derive_new_stream_id(
    requested_id_str: &str,
    last_item_id: Option<&String>,
) -> Result<String> {
    let (last_ms_time, last_seq_num) = if let Some(last_id_str) = last_item_id {
        let (ms_str, seq_str) = last_id_str
            .split_once('-')
            .ok_or_else(|| anyhow!("Invalid last stream ID format: {}", last_id_str))?;
        (ms_str.parse::<u128>()?, seq_str.parse::<u64>()?)
    } else {
        (0, 0)
    };

    let (requested_timestamp_part, requested_sequence_part) = if requested_id_str == "*" {
        ("*", "*")
    } else {
        requested_id_str
            .split_once("-")
            .ok_or_else(|| anyhow!("Invalid stream ID format: {}", requested_id_str))?
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
            .map_err(|_| anyhow!("Timestamp is not a valid number"))?
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
            .map_err(|_| anyhow!("Sequence is not a valid number"))?
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
