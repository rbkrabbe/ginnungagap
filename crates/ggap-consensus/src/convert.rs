use ggap_types::GgapError;

pub(crate) fn encode<T: serde::Serialize>(v: &T) -> Result<Vec<u8>, GgapError> {
    bincode::serde::encode_to_vec(v, bincode::config::standard())
        .map_err(|e| GgapError::Storage(e.to_string()))
}

pub(crate) fn decode<T: for<'de> serde::Deserialize<'de>>(b: &[u8]) -> Result<T, GgapError> {
    bincode::serde::decode_from_slice(b, bincode::config::standard())
        .map(|(v, _)| v)
        .map_err(|e| GgapError::Storage(e.to_string()))
}


