#![allow(dead_code)]

pub mod codes;
pub mod messages;

use codes::ErrorCategory;
use messages::ErrorCatalog;
use std::collections::HashMap;
use tracing::error;
use uuid::Uuid;

pub trait FriendlyError {
    fn user_message(&self) -> String;
    fn troubleshooting(&self) -> Vec<String>;
    fn is_retryable(&self) -> bool;
    fn error_code(&self) -> &str;
    fn category(&self) -> ErrorCategory;
}

pub fn detect_error_category(error_msg: &str) -> ErrorCategory {
    let msg_lower = error_msg.to_lowercase();

    if msg_lower.contains("network")
        || msg_lower.contains("connection")
        || msg_lower.contains("timeout")
        || msg_lower.contains("dns")
        || msg_lower.contains("http")
        || msg_lower.contains("download")
    {
        ErrorCategory::Network
    } else if msg_lower.contains("file")
        || msg_lower.contains("directory")
        || msg_lower.contains("permission")
        || msg_lower.contains("disk")
        || msg_lower.contains("path")
        || msg_lower.contains("not found")
    {
        ErrorCategory::FileSystem
    } else if msg_lower.contains("config")
        || msg_lower.contains("setting")
        || msg_lower.contains("invalid")
        || msg_lower.contains("missing key")
    {
        ErrorCategory::Configuration
    } else if msg_lower.contains("auth")
        || msg_lower.contains("login")
        || msg_lower.contains("token")
        || msg_lower.contains("password")
        || msg_lower.contains("unauthorized")
        || msg_lower.contains("permission denied")
    {
        ErrorCategory::Authentication
    } else if msg_lower.contains("archive")
        || msg_lower.contains("zip")
        || msg_lower.contains("rar")
        || msg_lower.contains("7z")
        || msg_lower.contains("bsa")
        || msg_lower.contains("corrupt")
    {
        ErrorCategory::Archive
    } else if msg_lower.contains("install")
        || msg_lower.contains("installing")
        || msg_lower.contains("installation")
        || msg_lower.contains("space")
    {
        ErrorCategory::Installation
    } else {
        ErrorCategory::General
    }
}

pub fn get_default_troubleshooting(category: &ErrorCategory) -> Vec<String> {
    match category {
        ErrorCategory::Network => vec![
            "Check your internet connection".to_string(),
            "Try again later if the server may be down".to_string(),
            "Verify the address is correct".to_string(),
        ],
        ErrorCategory::FileSystem => vec![
            "Verify the file path is correct".to_string(),
            "Check file permissions with: ls -la".to_string(),
            "Ensure the disk has enough space".to_string(),
        ],
        ErrorCategory::Configuration => vec![
            "Check the configuration file for errors".to_string(),
            "Verify all required settings are present".to_string(),
            "Try using default configuration".to_string(),
        ],
        ErrorCategory::Authentication => vec![
            "Verify your credentials are correct".to_string(),
            "Check if your account is active".to_string(),
            "Try logging in again".to_string(),
        ],
        ErrorCategory::Archive => vec![
            "Verify the archive file is not corrupted".to_string(),
            "Try downloading the file again".to_string(),
            "Check if the format is supported".to_string(),
        ],
        ErrorCategory::Installation => vec![
            "Ensure sufficient disk space is available".to_string(),
            "Close any programs that may be using the target files".to_string(),
            "Try running as administrator".to_string(),
        ],
        ErrorCategory::General => vec![
            "Try running the command again".to_string(),
            "Check the logs for more details".to_string(),
            "Report this issue if it persists".to_string(),
        ],
    }
}

pub fn format_anyhow_error(error: &anyhow::Error, verbose: bool) -> (String, i32) {
    let error_msg = error.to_string();
    let category = detect_error_category(&error_msg);
    let troubleshooting = get_default_troubleshooting(&category);
    let exit_code = category.exit_code();
    let reference_id = Uuid::new_v4().to_string();

    error!(
        error_category = %category.as_str(),
        is_retryable = false,
        reference_id = %reference_id,
        "Error occurred: {}",
        error_msg
    );

    let mut output = String::new();

    output.push_str(&format!("[ERROR] {}\n\n", error_msg));

    for step in troubleshooting {
        output.push_str(&format!("  → {}\n", step));
    }

    output.push('\n');
    output.push_str(&format!("Reference ID: {}", reference_id));

    if verbose {
        output.push_str(&format!("\nTechnical details: {:?}", error));
    }

    (output, exit_code)
}

pub struct ErrorFormatter {
    verbose: bool,
    catalog: ErrorCatalog,
    code_map: HashMap<String, String>,
}

impl ErrorFormatter {
    pub fn new() -> Self {
        let catalog = ErrorCatalog::default();
        let code_map = HashMap::new();
        Self {
            verbose: false,
            catalog,
            code_map,
        }
    }

    pub fn with_verbose(mut self, verbose: bool) -> Self {
        self.verbose = verbose;
        self
    }

    pub fn with_catalog(mut self, catalog: ErrorCatalog) -> Self {
        self.catalog = catalog;
        self.code_map = self
            .catalog
            .messages
            .iter()
            .map(|m| (m.error_code.clone(), m.user_message.clone()))
            .collect();
        self
    }

    pub fn format_error<E: FriendlyError>(&self, error: &E) -> String {
        let reference_id = Uuid::new_v4().to_string();

        error!(
            error_code = %error.error_code(),
            category = %error.category().as_str(),
            is_retryable = error.is_retryable(),
            reference_id = %reference_id,
            "Error occurred"
        );

        let mut output = String::new();

        output.push_str(&format!("[ERROR] {}\n\n", error.user_message()));

        if !error.troubleshooting().is_empty() {
            for step in error.troubleshooting() {
                output.push_str(&format!("  → {}\n", step));
            }
            output.push('\n');
        }

        if error.is_retryable() {
            output.push_str("This error may be temporary. Try running the command again.\n\n");
        }

        output.push_str(&format!("Reference ID: {}", reference_id));

        if self.verbose {
            output.push_str(&format!("\nTechnical details: {}", error.error_code()));
        }

        output
    }
}

impl Default for ErrorFormatter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_formatter_basic() {
        let formatter = ErrorFormatter::new();
        struct TestError;
        impl FriendlyError for TestError {
            fn user_message(&self) -> String {
                "Test error occurred".to_string()
            }
            fn troubleshooting(&self) -> Vec<String> {
                vec!["Check the logs".to_string()]
            }
            fn is_retryable(&self) -> bool {
                false
            }
            fn error_code(&self) -> &str {
                "TEST_ERROR"
            }
            fn category(&self) -> ErrorCategory {
                ErrorCategory::General
            }
        }

        let formatted = formatter.format_error(&TestError);
        assert!(formatted.contains("[ERROR]"));
        assert!(formatted.contains("Test error occurred"));
        assert!(formatted.contains("→ Check the logs"));
        assert!(formatted.contains("Reference ID:"));
    }
}
