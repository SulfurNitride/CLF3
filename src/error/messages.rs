#![allow(dead_code)]

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ErrorMessage {
    pub error_code: String,
    pub user_message: String,
    #[serde(default)]
    pub troubleshooting: Vec<String>,
    #[serde(default, rename = "isRetryable")]
    pub is_retryable: bool,
    #[serde(default, rename = "referenceId")]
    pub reference_id: Option<String>,
}

impl ErrorMessage {
    pub fn new(error_code: &str, user_message: &str) -> Self {
        Self {
            error_code: error_code.to_string(),
            user_message: user_message.to_string(),
            troubleshooting: Vec::new(),
            is_retryable: false,
            reference_id: None,
        }
    }

    pub fn with_troubleshooting(mut self, steps: Vec<String>) -> Self {
        self.troubleshooting = steps;
        self
    }

    pub fn retryable(mut self) -> Self {
        self.is_retryable = true;
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorCatalog {
    pub version: String,
    pub language: String,
    pub messages: Vec<ErrorMessage>,
}

impl Default for ErrorCatalog {
    fn default() -> Self {
        Self {
            version: "1.0.0".to_string(),
            language: "en".to_string(),
            messages: Vec::new(),
        }
    }
}
