#![allow(dead_code)]

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ErrorCode {
    pub category: String,
    pub code: String,
    pub exit_code: i32,
}

impl ErrorCode {
    pub fn new(category: &str, code: &str, exit_code: i32) -> Self {
        Self {
            category: category.to_string(),
            code: code.to_string(),
            exit_code,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ErrorCategory {
    Network,
    FileSystem,
    Configuration,
    Authentication,
    Archive,
    Installation,
    General,
}

impl ErrorCategory {
    pub fn as_str(&self) -> &'static str {
        match self {
            ErrorCategory::Network => "network",
            ErrorCategory::FileSystem => "filesystem",
            ErrorCategory::Configuration => "config",
            ErrorCategory::Authentication => "auth",
            ErrorCategory::Archive => "archive",
            ErrorCategory::Installation => "installation",
            ErrorCategory::General => "general",
        }
    }

    pub fn exit_code(&self) -> i32 {
        match self {
            ErrorCategory::Network => 3,
            ErrorCategory::FileSystem => 4,
            ErrorCategory::Configuration => 2,
            ErrorCategory::Authentication => 1,
            ErrorCategory::Archive => 5,
            ErrorCategory::Installation => 6,
            ErrorCategory::General => 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_category_exit_codes() {
        assert_eq!(ErrorCategory::Network.exit_code(), 3);
        assert_eq!(ErrorCategory::FileSystem.exit_code(), 4);
        assert_eq!(ErrorCategory::Configuration.exit_code(), 2);
    }
}
