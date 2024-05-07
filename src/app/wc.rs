
use regex::Regex;

use crate::KeyValue;

pub fn map(input: &str) -> Vec<KeyValue> {
    let re = Regex::new(r"[^\w\s]").unwrap();
    let result = re.replace_all(input, "");
    result
        .split_whitespace()
        .map(|x| KeyValue::new(x.to_owned(), 1.to_string()))
        .collect()
}

pub fn reduce(_key: &str, value: Vec<&str>) -> String {
    value.len().to_string()
}

     
