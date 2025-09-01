use crate::resp::RespValue;
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct StreamList(pub Vec<StreamItem>);

#[derive(Clone, Debug)]
pub struct StreamItem {
    pub id: String,
    pub values: HashMap<String, String>,
}

impl StreamItem {
    pub fn to_resp(&self) -> RespValue {
        let values_array_items = self
            .values
            .iter()
            .flat_map(|(k, v)| {
                vec![
                    RespValue::BulkString(k.clone()),
                    RespValue::BulkString(v.clone()),
                ]
            })
            .collect();

        RespValue::Array(vec![
            RespValue::BulkString(self.id.clone()),
            RespValue::Array(values_array_items),
        ])
    }
}
