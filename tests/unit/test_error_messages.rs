#[cfg(test)]
mod tests {
    use clf3::error::codes::{ErrorCategory, ErrorCode};

    #[test]
    fn test_error_code_creation() {
        let code = ErrorCode::new("network", "CONNECTION_FAILED", 3);
        assert_eq!(code.category, "network");
        assert_eq!(code.code, "CONNECTION_FAILED");
        assert_eq!(code.exit_code, 3);
    }

    #[test]
    fn test_error_category_exit_codes() {
        assert_eq!(ErrorCategory::Network.exit_code(), 3);
        assert_eq!(ErrorCategory::FileSystem.exit_code(), 4);
        assert_eq!(ErrorCategory::Configuration.exit_code(), 2);
        assert_eq!(ErrorCategory::Authentication.exit_code(), 1);
        assert_eq!(ErrorCategory::Archive.exit_code(), 5);
        assert_eq!(ErrorCategory::Installation.exit_code(), 6);
        assert_eq!(ErrorCategory::General.exit_code(), 1);
    }
}
