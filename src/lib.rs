#![forbid(unsafe_code)]
pub mod mr;
pub mod app;





#[derive(Clone, Eq, PartialEq, Hash)]
pub struct KeyValue {

    pub key: String,

    pub value: String,
}

impl KeyValue {

    pub fn new(key: String, value: String) -> Self {
        Self { key, value }
    }

}


