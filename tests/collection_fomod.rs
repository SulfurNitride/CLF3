use clf3::collection::fomod::{replay, Files, InstallerContext};
use serde_json::json;

fn fixture(xml: &str) -> (tempfile::TempDir, Files) {
    let temp = tempfile::tempdir().unwrap();
    let mut files = Files::new();
    for (name, bytes) in [
        ("fomod/ModuleConfig.xml", xml.as_bytes()),
        ("a.txt", b"a"),
        ("b.txt", b"b"),
        ("c.txt", b"c"),
    ] {
        let path = temp.path().join(name);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, bytes).unwrap();
        files.insert(name.into(), path);
    }
    (temp, files)
}

#[test]
fn outer_installer_takes_precedence_over_nested_payload_installers() {
    let xml = r#"<config><requiredInstallFiles><file source="a.txt" destination="chosen.txt"/></requiredInstallFiles></config>"#;
    let (temp, mut files) = fixture(xml);
    let nested = temp.path().join("main patches/fomod/ModuleConfig.xml");
    std::fs::create_dir_all(nested.parent().unwrap()).unwrap();
    std::fs::write(&nested, "deliberately invalid nested XML").unwrap();
    files.insert("main patches/fomod/ModuleConfig.xml".into(), nested);
    let output = replay(&files, &serde_json::Value::Null, &Default::default()).unwrap();
    assert_eq!(std::fs::read(&output["chosen.txt"]).unwrap(), b"a");
    let outer = files.remove("fomod/ModuleConfig.xml").unwrap();
    files.insert("independent/fomod/ModuleConfig.xml".into(), outer);
    assert!(
        replay(&files, &serde_json::Value::Null, &Default::default())
            .unwrap_err()
            .to_string()
            .contains("Ambiguous independent")
    );
}

#[test]
fn three_phase_priorities_and_selected_none_are_preserved() {
    let xml = r#"<config><requiredInstallFiles><file source="a.txt" destination="result.txt" priority="900"/></requiredInstallFiles><installSteps><installStep name="Main"><optionalFileGroups><group name="Choice" type="SelectAny"><plugins><plugin name="Recommended"><files><file source="b.txt" destination="unwanted.txt"/></files><typeDescriptor><type name="Recommended"/></typeDescriptor></plugin><plugin name="Always"><files><file source="b.txt" destination="result.txt" alwaysInstall="true"/></files></plugin></plugins></group></optionalFileGroups></installStep></installSteps><conditionalFileInstalls><patterns><pattern><dependencies operator="And"/><files><file source="c.txt" destination="result.txt" priority="-1"/></files></pattern></patterns></conditionalFileInstalls></config>"#;
    let (_temp, files) = fixture(xml);
    let choices = json!({"type":"fomod","options":[{"name":"Main","groups":[{"name":"Choice","choices":[]}]}]});
    let outputs = replay(&files, &choices, &Default::default()).unwrap();
    assert_eq!(outputs.len(), 1);
    assert_eq!(std::fs::read(&outputs["result.txt"]).unwrap(), b"c");
}

#[test]
fn duplicate_names_are_scoped_by_step_and_group_and_index() {
    let xml = r#"<config><installSteps><installStep name=""><optionalFileGroups><group name="Choice" type="SelectAny"><plugins><plugin name="Same"><files><file source="a.txt" destination="a.txt"/></files></plugin><plugin name="Same"><files><file source="b.txt" destination="b.txt"/></files></plugin></plugins></group></optionalFileGroups></installStep><installStep name=""><optionalFileGroups><group name="Choice" type="SelectAny"><plugins><plugin name="Same"><files><file source="c.txt" destination="c.txt"/></files></plugin></plugins></group></optionalFileGroups></installStep></installSteps></config>"#;
    let (_temp, files) = fixture(xml);
    let choices = json!({"type":"fomod","options":[{"name":"","groups":[{"name":"Choice","choices":[{"name":"Same","idx":1}]}]},{"name":"","groups":[{"name":"Choice","choices":[]}]}]});
    let outputs = replay(&files, &choices, &Default::default()).unwrap();
    assert_eq!(outputs.keys().cloned().collect::<Vec<_>>(), vec!["b.txt"]);
}

#[test]
fn inactive_and_missing_dependencies_are_distinct() {
    let xml = r#"<config><conditionalFileInstalls><patterns><pattern><dependencies><fileDependency file="Other.esp" state="Inactive"/></dependencies><files><file source="a.txt" destination="inactive.txt"/></files></pattern><pattern><dependencies><fileDependency file="Other.esp" state="Missing"/></dependencies><files><file source="b.txt" destination="missing.txt"/></files></pattern></patterns></conditionalFileInstalls></config>"#;
    let (_temp, files) = fixture(xml);
    let mut context = InstallerContext::default();
    context.installed.insert("other.esp".into());
    let output = replay(&files, &serde_json::Value::Null, &context).unwrap();
    assert_eq!(
        output.keys().cloned().collect::<Vec<_>>(),
        vec!["inactive.txt"]
    );
}

#[test]
fn curator_choice_overrides_visibility_but_unmatched_choices_fail() {
    let xml = r#"<config><installSteps><installStep name="Actual"><visible><flagDependency flag="hidden" value="yes"/></visible><optionalFileGroups><group name="Group"><plugins><plugin name="Choice"><files><file source="a.txt" destination=".\chosen.txt"/></files><typeDescriptor><type name="NotUsable"/></typeDescriptor></plugin></plugins></group></optionalFileGroups></installStep></installSteps></config>"#;
    let (_temp, files) = fixture(xml);
    let mut choices = json!({"type":"fomod","options":[{"name":"","groups":[{"name":"Group","choices":[{"name":"Choice","idx":0}]}]}]});
    assert!(replay(&files, &choices, &Default::default())
        .unwrap()
        .contains_key("chosen.txt"));
    choices["options"][0]["groups"][0]["choices"][0] = json!({"name":"Unmatched","idx":9});
    assert!(replay(&files, &choices, &Default::default()).is_err());
}

#[test]
fn ambiguous_fresh_installers_and_traversal_do_not_install() {
    let xml = r#"<config><installSteps><installStep name="Main"><optionalFileGroups><group name="Choice" type="SelectExactlyOne"><plugins><plugin name="A"/><plugin name="B"/></plugins></group></optionalFileGroups></installStep></installSteps></config>"#;
    let (_temp, files) = fixture(xml);
    assert!(
        replay(&files, &serde_json::Value::Null, &Default::default())
            .unwrap_err()
            .to_string()
            .contains("interaction required")
    );
    for destination in ["../outside", "C:\\outside", "/outside"] {
        let xml = format!(
            r#"<config><requiredInstallFiles><file source="a.txt" destination="{destination}"/></requiredInstallFiles></config>"#
        );
        let (_temp, files) = fixture(&xml);
        assert!(replay(&files, &serde_json::Value::Null, &Default::default()).is_err());
    }
}

#[test]
fn empty_folder_markers_are_accepted_only_when_present() {
    let xml = r#"<config><requiredInstallFiles><folder source="== Installer ==" destination="."/><file source="a.txt" destination="a.txt"/></requiredInstallFiles></config>"#;
    let (temp, files) = fixture(xml);
    assert!(replay(&files, &serde_json::Value::Null, &Default::default()).is_err());
    std::fs::create_dir(temp.path().join("== Installer ==")).unwrap();
    let output = replay(&files, &serde_json::Value::Null, &Default::default()).unwrap();
    assert_eq!(output.keys().cloned().collect::<Vec<_>>(), vec!["a.txt"]);
}
