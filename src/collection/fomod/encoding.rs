//! XML encoding detection and conversion.
//!
//! FOMOD ModuleConfig.xml files can be encoded in various formats:
//! - UTF-16 LE (with BOM 0xFF 0xFE)
//! - UTF-16 BE (with BOM 0xFE 0xFF)
//! - UTF-8 with BOM (0xEF 0xBB 0xBF)
//! - Plain UTF-8/ASCII
//!
//! Since quick-xml doesn't support UTF-16 directly, we detect the encoding
//! and convert to UTF-8 using encoding_rs before parsing.

use anyhow::{bail, Context, Result};
use std::fs;
use std::path::Path;

/// UTF-16 LE BOM bytes
const UTF16_LE_BOM: [u8; 2] = [0xFF, 0xFE];
/// UTF-16 BE BOM bytes
const UTF16_BE_BOM: [u8; 2] = [0xFE, 0xFF];
/// UTF-8 BOM bytes
const UTF8_BOM: [u8; 3] = [0xEF, 0xBB, 0xBF];

/// Detected encoding of an XML file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XmlEncoding {
    Utf16Le,
    Utf16Be,
    Utf8Bom,
    Utf8,
}

/// Detect the encoding of XML content by checking BOM bytes.
pub fn detect_encoding(bytes: &[u8]) -> XmlEncoding {
    if bytes.len() >= 2 && bytes[0..2] == UTF16_LE_BOM {
        XmlEncoding::Utf16Le
    } else if bytes.len() >= 2 && bytes[0..2] == UTF16_BE_BOM {
        XmlEncoding::Utf16Be
    } else if bytes.len() >= 3 && bytes[0..3] == UTF8_BOM {
        XmlEncoding::Utf8Bom
    } else {
        XmlEncoding::Utf8
    }
}

/// Read an XML file and convert to UTF-8 string, handling various encodings.
///
/// Detects encoding by BOM and converts UTF-16 to UTF-8 using encoding_rs.
pub fn read_xml_with_encoding(path: &Path) -> Result<String> {
    let bytes = fs::read(path)
        .with_context(|| format!("Failed to read XML file: {}", path.display()))?;

    decode_xml_bytes(&bytes)
}

/// Decode XML bytes to UTF-8 string, handling various encodings.
pub fn decode_xml_bytes(bytes: &[u8]) -> Result<String> {
    let encoding = detect_encoding(bytes);

    match encoding {
        XmlEncoding::Utf16Le => {
            // Skip BOM and decode
            let (cow, _, had_errors) = encoding_rs::UTF_16LE.decode(&bytes[2..]);
            if had_errors {
                bail!("UTF-16 LE decoding error");
            }
            Ok(cow.into_owned())
        }
        XmlEncoding::Utf16Be => {
            // Skip BOM and decode
            let (cow, _, had_errors) = encoding_rs::UTF_16BE.decode(&bytes[2..]);
            if had_errors {
                bail!("UTF-16 BE decoding error");
            }
            Ok(cow.into_owned())
        }
        XmlEncoding::Utf8Bom => {
            // Skip BOM and parse as UTF-8
            String::from_utf8(bytes[3..].to_vec())
                .context("Invalid UTF-8 after BOM")
        }
        XmlEncoding::Utf8 => {
            // Parse directly as UTF-8
            String::from_utf8(bytes.to_vec())
                .context("Invalid UTF-8 encoding")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_utf16_le() {
        let bytes = [0xFF, 0xFE, 0x3C, 0x00]; // BOM + "<"
        assert_eq!(detect_encoding(&bytes), XmlEncoding::Utf16Le);
    }

    #[test]
    fn test_detect_utf16_be() {
        let bytes = [0xFE, 0xFF, 0x00, 0x3C]; // BOM + "<"
        assert_eq!(detect_encoding(&bytes), XmlEncoding::Utf16Be);
    }

    #[test]
    fn test_detect_utf8_bom() {
        let bytes = [0xEF, 0xBB, 0xBF, 0x3C]; // BOM + "<"
        assert_eq!(detect_encoding(&bytes), XmlEncoding::Utf8Bom);
    }

    #[test]
    fn test_detect_utf8() {
        let bytes = [0x3C, 0x3F, 0x78, 0x6D]; // "<?xm"
        assert_eq!(detect_encoding(&bytes), XmlEncoding::Utf8);
    }

    #[test]
    fn test_decode_utf16_le() {
        // UTF-16 LE: BOM + "<config/>"
        let bytes: Vec<u8> = vec![
            0xFF, 0xFE, // BOM
            0x3C, 0x00, // <
            0x63, 0x00, // c
            0x6F, 0x00, // o
            0x6E, 0x00, // n
            0x66, 0x00, // f
            0x69, 0x00, // i
            0x67, 0x00, // g
            0x2F, 0x00, // /
            0x3E, 0x00, // >
        ];
        let result = decode_xml_bytes(&bytes).unwrap();
        assert_eq!(result, "<config/>");
    }

    #[test]
    fn test_decode_utf16_be() {
        // UTF-16 BE: BOM + "<config/>"
        let bytes: Vec<u8> = vec![
            0xFE, 0xFF, // BOM
            0x00, 0x3C, // <
            0x00, 0x63, // c
            0x00, 0x6F, // o
            0x00, 0x6E, // n
            0x00, 0x66, // f
            0x00, 0x69, // i
            0x00, 0x67, // g
            0x00, 0x2F, // /
            0x00, 0x3E, // >
        ];
        let result = decode_xml_bytes(&bytes).unwrap();
        assert_eq!(result, "<config/>");
    }

    #[test]
    fn test_decode_utf8_bom() {
        let bytes: Vec<u8> = vec![0xEF, 0xBB, 0xBF, b'<', b'c', b'o', b'n', b'f', b'i', b'g', b'/', b'>'];
        let result = decode_xml_bytes(&bytes).unwrap();
        assert_eq!(result, "<config/>");
    }

    #[test]
    fn test_decode_utf8() {
        let bytes = b"<config/>".to_vec();
        let result = decode_xml_bytes(&bytes).unwrap();
        assert_eq!(result, "<config/>");
    }
}
