use crate::bittensor::crypto::ss58_decode;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Hotkey(Arc<str>);

impl Deref for Hotkey {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl From<Hotkey> for String {
    fn from(hotkey: Hotkey) -> Self {
        hotkey.0.to_string()
    }
}

impl FromStr for Hotkey {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        ss58_decode(s)?;
        Ok(Hotkey(s.into()))
    }
}

impl std::fmt::Display for Hotkey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Serialize for Hotkey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for Hotkey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Hotkey::from_str(&s).map_err(serde::de::Error::custom)
    }
}

impl Hotkey {
    pub fn from_bytes(bytes: &[u8; 32]) -> Self {
        Hotkey(crate::bittensor::crypto::ss58_encode(bytes).into())
    }
}

impl AsRef<str> for Hotkey {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    const VALID_SS58: &str = "5DqJdDLU23m7yf6rZSmbLTshU7Bfn9eCTBkduhF4r9i73B9Y";
    const INVALID_SS58: &str = "invalid";

    #[test]
    fn test_from_str_valid() {
        let hotkey = Hotkey::from_str(VALID_SS58).unwrap();
        assert_eq!(hotkey.as_ref(), VALID_SS58);
    }

    #[test]
    fn test_from_str_invalid() {
        let result = Hotkey::from_str(INVALID_SS58);
        assert!(result.is_err());
    }

    #[test]
    fn test_deref() {
        let hotkey = Hotkey::from_str(VALID_SS58).unwrap();
        assert_eq!(hotkey.deref(), VALID_SS58);
    }

    #[test]
    fn test_into_string() {
        let hotkey = Hotkey::from_str(VALID_SS58).unwrap();
        let s: String = hotkey.into();
        assert_eq!(s, VALID_SS58);
    }

    #[test]
    fn test_serialize_deserialize() {
        let hotkey = Hotkey::from_str(VALID_SS58).unwrap();
        let json = serde_json::to_string(&hotkey).unwrap();
        let deserialized: Hotkey = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, hotkey);
    }
}
