//! Wabbajack modlist JSON types
//!
//! Clean serde definitions for parsing .wabbajack modlist files.
//! Based on analysis of Tuxborn.wabbajack format.

// BSA is the standard acronym in Bethesda modding community
#![allow(clippy::upper_case_acronyms)]
// Some methods not yet used until installation pipeline is built
#![allow(dead_code)]

use serde::{Deserialize, Serialize};

/// Root modlist structure
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Modlist {
    pub name: String,
    #[serde(default)]
    pub author: String,
    #[serde(default)]
    pub description: String,
    pub version: String,
    pub wabbajack_version: String,
    pub game_type: String,
    #[serde(default)]
    pub website: String,
    #[serde(default)]
    pub readme: String,
    #[serde(default)]
    pub image: String,
    #[serde(rename = "IsNSFW")]
    pub is_nsfw: bool,
    pub archives: Vec<Archive>,
    pub directives: Vec<Directive>,
}

/// Archive (download) definition
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Archive {
    pub hash: String,
    pub meta: String,
    pub name: String,
    pub size: u64,
    pub state: DownloadState,
}

/// Download source state - tagged union based on $type
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "$type")]
pub enum DownloadState {
    #[serde(rename = "NexusDownloader, Wabbajack.Lib")]
    Nexus(NexusState),

    #[serde(rename = "HttpDownloader, Wabbajack.Lib")]
    Http(HttpState),

    #[serde(rename = "GoogleDriveDownloader, Wabbajack.Lib")]
    GoogleDrive(GoogleDriveState),

    #[serde(rename = "MegaDownloader, Wabbajack.Lib")]
    Mega(MegaState),

    #[serde(rename = "MediaFireDownloader+State, Wabbajack.Lib")]
    MediaFire(MediaFireState),

    #[serde(rename = "ManualDownloader, Wabbajack.Lib")]
    Manual(ManualState),

    #[serde(rename = "WabbajackCDNDownloader+State, Wabbajack.Lib")]
    WabbajackCDN(WabbajackCDNState),

    #[serde(rename = "GameFileSourceDownloader, Wabbajack.Lib")]
    GameFileSource(GameFileSourceState),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct NexusState {
    pub game_name: String,
    #[serde(rename = "ModID")]
    pub mod_id: u64,
    #[serde(rename = "FileID")]
    pub file_id: u64,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub author: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub version: String,
    #[serde(rename = "ImageURL")]
    pub image_url: Option<String>,
    #[serde(rename = "IsNSFW")]
    pub is_nsfw: bool,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct HttpState {
    pub url: String,
    #[serde(default)]
    pub headers: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct GoogleDriveState {
    pub id: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct MegaState {
    pub url: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct MediaFireState {
    pub url: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ManualState {
    pub url: String,
    pub prompt: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct WabbajackCDNState {
    pub url: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct GameFileSourceState {
    pub game: String,
    pub game_file: String,
    pub game_version: String,
    pub hash: String,
}

/// Installation directive - tagged union based on $type
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "$type")]
pub enum Directive {
    FromArchive(FromArchiveDirective),
    PatchedFromArchive(PatchedFromArchiveDirective),
    InlineFile(InlineFileDirective),
    RemappedInlineFile(RemappedInlineFileDirective),
    TransformedTexture(TransformedTextureDirective),
    CreateBSA(CreateBSADirective),
}

impl Directive {
    /// Get the destination path for this directive
    pub fn to_path(&self) -> &str {
        match self {
            Directive::FromArchive(d) => &d.to,
            Directive::PatchedFromArchive(d) => &d.to,
            Directive::InlineFile(d) => &d.to,
            Directive::RemappedInlineFile(d) => &d.to,
            Directive::TransformedTexture(d) => &d.to,
            Directive::CreateBSA(d) => &d.to,
        }
    }

    /// Get the expected output size
    pub fn size(&self) -> u64 {
        match self {
            Directive::FromArchive(d) => d.size,
            Directive::PatchedFromArchive(d) => d.size,
            Directive::InlineFile(d) => d.size,
            Directive::RemappedInlineFile(d) => d.size,
            Directive::TransformedTexture(d) => d.size,
            Directive::CreateBSA(d) => d.file_states.iter().map(|f| f.size()).sum(),
        }
    }

    /// Get directive type as string
    pub fn directive_type(&self) -> &'static str {
        match self {
            Directive::FromArchive(_) => "FromArchive",
            Directive::PatchedFromArchive(_) => "PatchedFromArchive",
            Directive::InlineFile(_) => "InlineFile",
            Directive::RemappedInlineFile(_) => "RemappedInlineFile",
            Directive::TransformedTexture(_) => "TransformedTexture",
            Directive::CreateBSA(_) => "CreateBSA",
        }
    }
}

/// Extract file directly from an archive
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct FromArchiveDirective {
    pub to: String,
    pub hash: String,
    pub size: u64,
    /// [archive_hash, path_in_archive]
    pub archive_hash_path: Vec<String>,
}

/// Extract file and apply binary patch
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct PatchedFromArchiveDirective {
    pub to: String,
    pub hash: String,
    pub size: u64,
    pub archive_hash_path: Vec<String>,
    pub from_hash: String,
    #[serde(rename = "PatchID")]
    pub patch_id: uuid::Uuid,
}

/// File embedded in the .wabbajack archive
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct InlineFileDirective {
    pub to: String,
    pub hash: String,
    pub size: u64,
    #[serde(rename = "SourceDataID")]
    pub source_data_id: uuid::Uuid,
}

/// Embedded file with path remapping
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct RemappedInlineFileDirective {
    pub to: String,
    pub hash: String,
    pub size: u64,
    #[serde(rename = "SourceDataID")]
    pub source_data_id: uuid::Uuid,
}

/// Texture that needs transformation (resize/recompress)
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct TransformedTextureDirective {
    pub to: String,
    pub hash: String,
    pub size: u64,
    pub archive_hash_path: Vec<String>,
    pub image_state: ImageState,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ImageState {
    pub width: u32,
    pub height: u32,
    pub format: String,
    pub mip_levels: u32,
    pub perceptual_hash: String,
}

/// Create a Bethesda archive (BSA/BA2)
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct CreateBSADirective {
    pub to: String,
    pub hash: String,
    #[serde(rename = "TempID")]
    pub temp_id: uuid::Uuid,
    pub file_states: Vec<BSAFileState>,
    pub state: BSAState,
}

/// State for BSA/BA2 creation
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "$type")]
pub enum BSAState {
    #[serde(rename = "BSAState, Compression.BSA")]
    BSA(BSAStateData),

    #[serde(rename = "BA2State, Compression.BSA")]
    BA2(BA2StateData),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct BSAStateData {
    pub magic: String,
    pub version: u32,
    pub archive_flags: u32,
    pub file_flags: u32,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct BA2StateData {
    #[serde(alias = "HeaderMagic")]
    pub magic: String,
    pub version: u32,
    #[serde(rename = "Type")]
    pub archive_type: BA2ArchiveType,
    pub has_name_table: bool,
}

/// BA2 archive type - can be string or integer depending on wabbajack version
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BA2ArchiveType {
    String(String),
    Int(u32),
}

impl BA2ArchiveType {
    pub fn as_str(&self) -> &str {
        match self {
            BA2ArchiveType::String(s) => s,
            BA2ArchiveType::Int(1) => "DX10",
            BA2ArchiveType::Int(_) => "GNRL",
        }
    }

    /// Check if this is a DX10 (texture) archive
    pub fn is_dx10(&self) -> bool {
        match self {
            BA2ArchiveType::String(s) => s.to_uppercase().contains("DX10"),
            BA2ArchiveType::Int(1) => true,
            BA2ArchiveType::Int(_) => false,
        }
    }
}

/// File entry in a BSA
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "$type")]
pub enum BSAFileState {
    #[serde(rename = "BSAFileState, Compression.BSA", alias = "BSAFile")]
    BSA(BSAFileStateData),

    #[serde(rename = "BA2FileState, Compression.BSA", alias = "BA2File")]
    BA2(BA2FileStateData),

    #[serde(rename = "BA2DX10FileState, Compression.BSA", alias = "BA2DX10FileState")]
    BA2DX10(BA2DX10FileStateData),

    #[serde(rename = "BA2DX10Entry, Compression.BSA", alias = "BA2DX10Entry")]
    BA2DX10Entry(BA2DX10FileStateData),
}

impl BSAFileState {
    pub fn path(&self) -> &str {
        match self {
            BSAFileState::BSA(d) => &d.path,
            BSAFileState::BA2(d) => &d.path,
            BSAFileState::BA2DX10(d) => &d.path,
            BSAFileState::BA2DX10Entry(d) => &d.path,
        }
    }

    pub fn size(&self) -> u64 {
        // Return a reasonable estimate - actual size computed during creation
        0
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct BSAFileStateData {
    pub path: String,
    pub index: u32,
    pub flip_compression: bool,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct BA2FileStateData {
    pub path: String,
    pub index: u32,
    #[serde(default)]
    pub dir_hash: u64,
    #[serde(default)]
    pub name_hash: u64,
    #[serde(default)]
    pub extension: String,
    #[serde(default)]
    pub flags: u64,
    #[serde(default)]
    pub align: u64,
    #[serde(default)]
    pub compressed: bool,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct BA2DX10FileStateData {
    pub path: String,
    pub index: u32,
    #[serde(default)]
    pub chunks: Vec<BA2DX10Chunk>,
    #[serde(default)]
    pub dir_hash: u64,
    #[serde(default)]
    pub name_hash: u64,
    #[serde(default)]
    pub extension: String,
    #[serde(default)]
    pub width: u32,
    #[serde(default)]
    pub height: u32,
    #[serde(default)]
    pub num_mips: u32,
    #[serde(default)]
    pub pixel_format: u32,
    #[serde(default)]
    pub tile_mode: u32,
    #[serde(default)]
    pub is_cube_map: u32,
    #[serde(default)]
    pub chunk_hdr_len: u32,
    #[serde(default)]
    pub unk8: u32,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct BA2DX10Chunk {
    #[serde(default)]
    pub start_mip: u32,
    #[serde(default)]
    pub end_mip: u32,
    #[serde(default)]
    pub align: u64,
    #[serde(default)]
    pub compressed: bool,
    #[serde(default)]
    pub full_sz: u64,
}
