//! FOMOD ModuleConfig.xml parser.
//!
//! Parses the FOMOD configuration XML into Rust structs for processing.

use anyhow::{Context, Result};
use std::path::Path;

use super::encoding::read_xml_with_encoding;

/// Parsed FOMOD configuration from ModuleConfig.xml.
#[derive(Debug, Clone, Default)]
pub struct FomodConfig {
    /// Module name from the config.
    pub module_name: String,
    /// Files/folders that are always installed.
    pub required_files: Vec<InstallFile>,
    /// Interactive installation steps.
    pub install_steps: Vec<InstallStep>,
    /// Conditional file installations based on flags.
    pub conditional_installs: Vec<ConditionalPattern>,
}

/// An installation step in the FOMOD wizard.
#[derive(Debug, Clone, Default)]
pub struct InstallStep {
    /// Step name.
    pub name: String,
    /// Groups of options in this step.
    pub groups: Vec<OptionGroup>,
}

/// A group of selectable options.
#[derive(Debug, Clone, Default)]
pub struct OptionGroup {
    /// Group name.
    pub name: String,
    /// Selection type (SelectExactlyOne, SelectAtLeastOne, SelectAny, etc.)
    pub group_type: GroupType,
    /// Available plugins/options in this group.
    pub plugins: Vec<Plugin>,
}

/// Group selection type.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[allow(clippy::enum_variant_names)]
pub enum GroupType {
    #[default]
    SelectAny,
    SelectExactlyOne,
    SelectAtLeastOne,
    SelectAtMostOne,
    SelectAll,
}

impl GroupType {
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "selectexactlyone" => GroupType::SelectExactlyOne,
            "selectatleastone" => GroupType::SelectAtLeastOne,
            "selectatmostone" => GroupType::SelectAtMostOne,
            "selectall" => GroupType::SelectAll,
            _ => GroupType::SelectAny,
        }
    }
}

/// A plugin/option that can be selected.
#[derive(Debug, Clone, Default)]
pub struct Plugin {
    /// Plugin name.
    pub name: String,
    /// Description text.
    pub description: String,
    /// Files to install if this plugin is selected.
    pub files: Vec<InstallFile>,
    /// Flags to set if this plugin is selected.
    pub condition_flags: Vec<ConditionFlag>,
    /// Plugin type (Optional, Required, Recommended, etc.)
    pub type_descriptor: PluginType,
}

/// Plugin type descriptor.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PluginType {
    #[default]
    Optional,
    Required,
    Recommended,
    NotUsable,
    CouldBeUsable,
}

impl PluginType {
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "required" => PluginType::Required,
            "recommended" => PluginType::Recommended,
            "notusable" => PluginType::NotUsable,
            "couldbeusable" => PluginType::CouldBeUsable,
            _ => PluginType::Optional,
        }
    }
}

/// A file or folder to install.
#[derive(Debug, Clone, Default)]
pub struct InstallFile {
    /// Source path relative to FOMOD data root.
    pub source: String,
    /// Destination path relative to mod root (empty = use source or root).
    pub destination: String,
    /// Installation priority (higher = install later, overwrite earlier).
    pub priority: i32,
    /// Whether this is a folder (true) or file (false).
    pub is_folder: bool,
}

/// A condition flag to set.
#[derive(Debug, Clone, Default)]
pub struct ConditionFlag {
    /// Flag name.
    pub name: String,
    /// Flag value.
    pub value: String,
}

/// A conditional installation pattern.
#[derive(Debug, Clone, Default)]
pub struct ConditionalPattern {
    /// Dependencies that must be satisfied.
    pub dependencies: Dependencies,
    /// Files to install if dependencies are met.
    pub files: Vec<InstallFile>,
}

/// Dependency conditions for conditional installs.
#[derive(Debug, Clone, Default)]
pub struct Dependencies {
    /// Logical operator (And/Or).
    pub operator: DependencyOperator,
    /// Flag dependencies.
    pub flags: Vec<FlagDependency>,
    /// Nested dependency groups.
    pub nested: Vec<Dependencies>,
}

/// Logical operator for combining dependencies.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DependencyOperator {
    #[default]
    And,
    Or,
}

impl DependencyOperator {
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "or" => DependencyOperator::Or,
            _ => DependencyOperator::And,
        }
    }
}

/// A single flag dependency check.
#[derive(Debug, Clone, Default)]
pub struct FlagDependency {
    /// Flag name to check.
    pub flag: String,
    /// Expected value.
    pub value: String,
}

/// Parse a FOMOD ModuleConfig.xml file.
pub fn parse_fomod(path: &Path) -> Result<FomodConfig> {
    let xml = read_xml_with_encoding(path)
        .with_context(|| format!("Failed to read FOMOD config: {}", path.display()))?;

    parse_fomod_xml(&xml)
}

/// Helper to get unescaped attribute value (decodes &quot; &amp; etc.)
fn unescape_attr(attr: &quick_xml::events::attributes::Attribute) -> String {
    attr.unescape_value()
        .map(|s| s.to_string())
        .unwrap_or_else(|_| unescape_attr(attr))
}

/// Parse FOMOD configuration from XML string.
pub fn parse_fomod_xml(xml: &str) -> Result<FomodConfig> {
    use quick_xml::events::Event;
    use quick_xml::Reader;

    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);

    let mut config = FomodConfig::default();
    let mut buf = Vec::new();

    // Track current parsing context
    let mut in_required_files = false;
    let mut in_install_steps = false;
    let mut in_conditional_installs = false;
    let mut current_step: Option<InstallStep> = None;
    let mut current_group: Option<OptionGroup> = None;
    let mut current_plugin: Option<Plugin> = None;
    let mut current_pattern: Option<ConditionalPattern> = None;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Eof) => break,
            Ok(Event::Start(ref e)) => {
                let name = std::str::from_utf8(e.name().as_ref())
                    .unwrap_or("")
                    .to_lowercase();

                match name.as_str() {
                    "modulename" => {
                        config.module_name = reader.read_text(e.name()).unwrap_or_default().to_string();
                    }
                    "requiredinstallfiles" => {
                        in_required_files = true;
                    }
                    "installsteps" => {
                        in_install_steps = true;
                    }
                    "conditionalfileinstalls" => {
                        in_conditional_installs = true;
                    }
                    "installstep" => {
                        if in_install_steps {
                            let mut step = InstallStep::default();
                            for attr in e.attributes().flatten() {
                                if attr.key.as_ref() == b"name" {
                                    step.name = unescape_attr(&attr);
                                }
                            }
                            current_step = Some(step);
                        }
                    }
                    "group" => {
                        if current_step.is_some() {
                            let mut group = OptionGroup::default();
                            for attr in e.attributes().flatten() {
                                match attr.key.as_ref() {
                                    b"name" => {
                                        group.name = unescape_attr(&attr);
                                    }
                                    b"type" => {
                                        group.group_type = GroupType::from_str(
                                            &unescape_attr(&attr),
                                        );
                                    }
                                    _ => {}
                                }
                            }
                            current_group = Some(group);
                        }
                    }
                    "plugin" => {
                        if current_group.is_some() {
                            let mut plugin = Plugin::default();
                            for attr in e.attributes().flatten() {
                                if attr.key.as_ref() == b"name" {
                                    plugin.name = unescape_attr(&attr);
                                }
                            }
                            current_plugin = Some(plugin);
                        }
                    }
                    "file" | "folder" => {
                        let mut install_file = InstallFile {
                            is_folder: name == "folder",
                            ..Default::default()
                        };
                        for attr in e.attributes().flatten() {
                            match attr.key.as_ref() {
                                b"source" => {
                                    install_file.source = unescape_attr(&attr);
                                }
                                b"destination" => {
                                    install_file.destination = unescape_attr(&attr);
                                }
                                b"priority" => {
                                    install_file.priority = unescape_attr(&attr).parse()
                                        .unwrap_or(0);
                                }
                                _ => {}
                            }
                        }

                        // Add to appropriate container
                        if let Some(ref mut plugin) = current_plugin {
                            plugin.files.push(install_file);
                        } else if let Some(ref mut pattern) = current_pattern {
                            pattern.files.push(install_file);
                        } else if in_required_files {
                            config.required_files.push(install_file);
                        }
                    }
                    "flag" => {
                        if let Some(ref mut plugin) = current_plugin {
                            let mut flag = ConditionFlag::default();
                            for attr in e.attributes().flatten() {
                                if attr.key.as_ref() == b"name" {
                                    flag.name = unescape_attr(&attr);
                                }
                            }
                            flag.value = reader.read_text(e.name()).unwrap_or_default().to_string();
                            plugin.condition_flags.push(flag);
                        }
                    }
                    "type" => {
                        if let Some(ref mut plugin) = current_plugin {
                            for attr in e.attributes().flatten() {
                                if attr.key.as_ref() == b"name" {
                                    plugin.type_descriptor = PluginType::from_str(
                                        &unescape_attr(&attr),
                                    );
                                }
                            }
                        }
                    }
                    "pattern" => {
                        if in_conditional_installs {
                            current_pattern = Some(ConditionalPattern::default());
                        }
                    }
                    "dependencies" => {
                        if let Some(ref mut pattern) = current_pattern {
                            for attr in e.attributes().flatten() {
                                if attr.key.as_ref() == b"operator" {
                                    pattern.dependencies.operator = DependencyOperator::from_str(
                                        &unescape_attr(&attr),
                                    );
                                }
                            }
                        }
                    }
                    "flagdependency" => {
                        if let Some(ref mut pattern) = current_pattern {
                            let mut flag_dep = FlagDependency::default();
                            for attr in e.attributes().flatten() {
                                match attr.key.as_ref() {
                                    b"flag" => {
                                        flag_dep.flag = unescape_attr(&attr);
                                    }
                                    b"value" => {
                                        flag_dep.value = unescape_attr(&attr);
                                    }
                                    _ => {}
                                }
                            }
                            pattern.dependencies.flags.push(flag_dep);
                        }
                    }
                    "description" => {
                        if let Some(ref mut plugin) = current_plugin {
                            plugin.description = reader.read_text(e.name()).unwrap_or_default().to_string();
                        }
                    }
                    _ => {}
                }
            }
            // Handle self-closing tags like <file source="..." />
            Ok(Event::Empty(ref e)) => {
                let name = std::str::from_utf8(e.name().as_ref())
                    .unwrap_or("")
                    .to_lowercase();

                match name.as_str() {
                    "file" | "folder" => {
                        let mut install_file = InstallFile {
                            is_folder: name == "folder",
                            ..Default::default()
                        };
                        for attr in e.attributes().flatten() {
                            match attr.key.as_ref() {
                                b"source" => {
                                    install_file.source = unescape_attr(&attr);
                                }
                                b"destination" => {
                                    install_file.destination = unescape_attr(&attr);
                                }
                                b"priority" => {
                                    install_file.priority = unescape_attr(&attr).parse()
                                        .unwrap_or(0);
                                }
                                _ => {}
                            }
                        }

                        // Add to appropriate container
                        if let Some(ref mut plugin) = current_plugin {
                            plugin.files.push(install_file);
                        } else if let Some(ref mut pattern) = current_pattern {
                            pattern.files.push(install_file);
                        } else if in_required_files {
                            config.required_files.push(install_file);
                        }
                    }
                    "type" => {
                        if let Some(ref mut plugin) = current_plugin {
                            for attr in e.attributes().flatten() {
                                if attr.key.as_ref() == b"name" {
                                    plugin.type_descriptor = PluginType::from_str(
                                        &unescape_attr(&attr),
                                    );
                                }
                            }
                        }
                    }
                    "flagdependency" => {
                        if let Some(ref mut pattern) = current_pattern {
                            let mut flag_dep = FlagDependency::default();
                            for attr in e.attributes().flatten() {
                                match attr.key.as_ref() {
                                    b"flag" => {
                                        flag_dep.flag = unescape_attr(&attr);
                                    }
                                    b"value" => {
                                        flag_dep.value = unescape_attr(&attr);
                                    }
                                    _ => {}
                                }
                            }
                            pattern.dependencies.flags.push(flag_dep);
                        }
                    }
                    _ => {}
                }
            }
            Ok(Event::End(ref e)) => {
                let name = std::str::from_utf8(e.name().as_ref())
                    .unwrap_or("")
                    .to_lowercase();

                match name.as_str() {
                    "requiredinstallfiles" => {
                        in_required_files = false;
                    }
                    "installsteps" => {
                        in_install_steps = false;
                    }
                    "conditionalfileinstalls" => {
                        in_conditional_installs = false;
                    }
                    "installstep" => {
                        if let Some(step) = current_step.take() {
                            config.install_steps.push(step);
                        }
                    }
                    "group" => {
                        if let Some(group) = current_group.take() {
                            if let Some(ref mut step) = current_step {
                                step.groups.push(group);
                            }
                        }
                    }
                    "plugin" => {
                        if let Some(plugin) = current_plugin.take() {
                            if let Some(ref mut group) = current_group {
                                group.plugins.push(plugin);
                            }
                        }
                    }
                    "pattern" => {
                        if let Some(pattern) = current_pattern.take() {
                            config.conditional_installs.push(pattern);
                        }
                    }
                    _ => {}
                }
            }
            Ok(_) => {}
            Err(e) => {
                return Err(anyhow::anyhow!("XML parse error at position {}: {:?}", reader.buffer_position(), e));
            }
        }
        buf.clear();
    }

    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_fomod() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<config>
    <moduleName>Test Mod</moduleName>
    <requiredInstallFiles>
        <file source="data/test.esp"/>
    </requiredInstallFiles>
</config>"#;

        let config = parse_fomod_xml(xml).unwrap();
        assert_eq!(config.module_name, "Test Mod");
        assert_eq!(config.required_files.len(), 1);
        assert_eq!(config.required_files[0].source, "data/test.esp");
    }

    #[test]
    fn test_parse_install_steps() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<config>
    <moduleName>Test</moduleName>
    <installSteps>
        <installStep name="Choose Version">
            <optionalFileGroups>
                <group name="Versions" type="SelectExactlyOne">
                    <plugins>
                        <plugin name="Option A">
                            <files>
                                <file source="optionA/test.esp"/>
                            </files>
                        </plugin>
                        <plugin name="Option B">
                            <files>
                                <folder source="optionB"/>
                            </files>
                        </plugin>
                    </plugins>
                </group>
            </optionalFileGroups>
        </installStep>
    </installSteps>
</config>"#;

        let config = parse_fomod_xml(xml).unwrap();
        assert_eq!(config.install_steps.len(), 1);

        let step = &config.install_steps[0];
        assert_eq!(step.name, "Choose Version");
        assert_eq!(step.groups.len(), 1);

        let group = &step.groups[0];
        assert_eq!(group.name, "Versions");
        assert_eq!(group.group_type, GroupType::SelectExactlyOne);
        assert_eq!(group.plugins.len(), 2);

        assert_eq!(group.plugins[0].name, "Option A");
        assert_eq!(group.plugins[0].files[0].source, "optionA/test.esp");
        assert!(!group.plugins[0].files[0].is_folder);

        assert_eq!(group.plugins[1].name, "Option B");
        assert_eq!(group.plugins[1].files[0].source, "optionB");
        assert!(group.plugins[1].files[0].is_folder);
    }

    #[test]
    fn test_parse_condition_flags() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<config>
    <installSteps>
        <installStep name="Step">
            <optionalFileGroups>
                <group name="Group" type="SelectAny">
                    <plugins>
                        <plugin name="Enable Feature">
                            <conditionFlags>
                                <flag name="FeatureEnabled">true</flag>
                            </conditionFlags>
                        </plugin>
                    </plugins>
                </group>
            </optionalFileGroups>
        </installStep>
    </installSteps>
</config>"#;

        let config = parse_fomod_xml(xml).unwrap();
        let plugin = &config.install_steps[0].groups[0].plugins[0];
        assert_eq!(plugin.condition_flags.len(), 1);
        assert_eq!(plugin.condition_flags[0].name, "FeatureEnabled");
        assert_eq!(plugin.condition_flags[0].value, "true");
    }

    #[test]
    fn test_parse_conditional_installs() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<config>
    <conditionalFileInstalls>
        <patterns>
            <pattern>
                <dependencies operator="And">
                    <flagDependency flag="FeatureEnabled" value="true"/>
                </dependencies>
                <files>
                    <file source="feature/extra.esp"/>
                </files>
            </pattern>
        </patterns>
    </conditionalFileInstalls>
</config>"#;

        let config = parse_fomod_xml(xml).unwrap();
        assert_eq!(config.conditional_installs.len(), 1);

        let pattern = &config.conditional_installs[0];
        assert_eq!(pattern.dependencies.operator, DependencyOperator::And);
        assert_eq!(pattern.dependencies.flags.len(), 1);
        assert_eq!(pattern.dependencies.flags[0].flag, "FeatureEnabled");
        assert_eq!(pattern.dependencies.flags[0].value, "true");
        assert_eq!(pattern.files.len(), 1);
        assert_eq!(pattern.files[0].source, "feature/extra.esp");
    }

    #[test]
    fn test_group_type_parsing() {
        assert_eq!(GroupType::from_str("SelectExactlyOne"), GroupType::SelectExactlyOne);
        assert_eq!(GroupType::from_str("selectatleastone"), GroupType::SelectAtLeastOne);
        assert_eq!(GroupType::from_str("SELECTATMOSTONE"), GroupType::SelectAtMostOne);
        assert_eq!(GroupType::from_str("unknown"), GroupType::SelectAny);
    }
}
