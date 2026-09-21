//! Nexus Collections planning, independent of Wabbajack and account credentials.
//!
//! The original Collections branch supplies the manifest vocabulary. Planning
//! retains the source document and keeps artifact, member and plugin identities
//! separate. Frontends own authenticated access; the engine consumes local files.

pub mod cli;
pub mod fomod;
pub mod games;
pub mod host;
pub mod ini;
pub mod package;
pub mod paths;
pub mod plan;
pub mod progress;
pub mod publish;
pub mod stage;
pub mod types;
pub mod url;
pub mod worker;
mod xml_encoding;

pub use package::CollectionPackage;
pub use plan::{CollectionPlan, PlanOptions};
