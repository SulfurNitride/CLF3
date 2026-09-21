use clf3::collection::{
    host::CollectionHostCommand, paths::relative_path, url::parse_collection_url,
    CollectionPackage, CollectionPlan, PlanOptions,
};
use serde_json::{json, Value};
use std::io::Write;

fn member(name: &str, tag: &str) -> Value {
    json!({"name":name,"version":"1.0","source":{"type":"nexus","modId":1,"fileId":2,"tag":tag,"logicalFilename":"source.zip","md5":"0123456789abcdef0123456789abcdef"}})
}
fn manifest(mods: Vec<Value>) -> Value {
    json!({"info":{"name":"Fixture","domainName":"skyrimspecialedition"},"mods":mods})
}
fn open(value: &Value) -> (tempfile::TempDir, CollectionPackage) {
    let temp = tempfile::tempdir().unwrap();
    std::fs::write(
        temp.path().join("collection.json"),
        serde_json::to_vec(value).unwrap(),
    )
    .unwrap();
    let package = CollectionPackage::open(temp.path()).unwrap();
    (temp, package)
}
fn options() -> PlanOptions {
    PlanOptions {
        schema_id: Some(1),
        ..Default::default()
    }
}

#[test]
fn one_archive_can_produce_distinct_members_and_signatures() {
    let mut a = member("Variant A", "a");
    a["choices"] = json!({"type":"fomod","options":[{"name":"","groups":[{"name":"","choices":[{"name":"","idx":0}]}]}]});
    let mut b = member("Variant B", "b");
    b["choices"] =
        json!({"type":"fomod","options":[{"name":"","groups":[{"name":"","choices":[]}]}]});
    let (_dir, package) = open(&manifest(vec![a.clone(), b]));
    let plan = CollectionPlan::build(&package, &options()).unwrap();
    assert_eq!(plan.artifacts.len(), 1);
    assert_ne!(plan.members[0].id, plan.members[1].id);
    assert_ne!(
        plan.members[0].install_signature,
        plan.members[1].install_signature
    );
    assert_eq!(package.collection.mods[0].choices, a["choices"]);
}

#[test]
fn cross_game_numeric_ids_never_share_an_artifact() {
    let mut b = member("B", "b");
    b["domainName"] = json!("skyrim");
    let (_dir, package) = open(&manifest(vec![member("A", "a"), b]));
    let plan = CollectionPlan::build(&package, &options()).unwrap();
    assert_eq!(plan.artifacts.len(), 2);
    assert!(plan.artifacts.iter().any(|a| a.domain == "skyrim"));
}

#[test]
fn optionals_control_acquisition_without_disabling_included_members() {
    let mut b = member("B", "b");
    b["optional"] = json!(true);
    b["source"]["fileId"] = json!(3);
    let (_dir, package) = open(&manifest(vec![member("A", "a"), b]));
    let mut opts = options();
    let omitted = CollectionPlan::build(&package, &opts).unwrap();
    assert_eq!(omitted.artifacts.len(), 1);
    assert!(!omitted.members[1].selected);
    opts.selected_optional.insert("b".into());
    let included = CollectionPlan::build(&package, &opts).unwrap();
    assert!(included.members[1].selected);
    assert_eq!(included.asset_order.len(), 2);
    assert_eq!(included.artifacts.len(), 2);
    assert_eq!(included.members[1].id, omitted.members[1].id);
}

#[test]
fn exclusions_are_per_provider_and_plugin_order_is_independent() {
    let mut a = member("A", "a");
    a["fileOverrides"] = json!(["textures\\x.dds"]);
    a["phase"] = json!(2);
    let mut b = member("B", "b");
    b["fileOverrides"] = json!(["textures/y.dds"]);
    b["phase"] = json!(1);
    let mut value = manifest(vec![a, b]);
    value["plugins"] = json!([{"name":"B.esp","enabled":true},{"name":"A.esp","enabled":false}]);
    value["pluginRules"] = json!({"groups":[{"name":"late"}]});
    let (_dir, package) = open(&value);
    let plan = CollectionPlan::build(&package, &options()).unwrap();
    assert_eq!(plan.members[0].excluded_paths, vec!["textures/x.dds"]);
    assert_eq!(plan.members[1].excluded_paths, vec!["textures/y.dds"]);
    assert_eq!(
        plan.asset_order,
        vec![plan.members[0].id.clone(), plan.members[1].id.clone()]
    );
    assert_eq!(
        plan.installation_order,
        vec![plan.members[1].id.clone(), plan.members[0].id.clone()]
    );
    assert_eq!(plan.plugins[0].name, "B.esp");
    assert!(!plan.plugins[1].enabled);
    assert_eq!(plan.plugin_rules, value["pluginRules"]);
}

#[test]
fn cycles_and_ambiguous_rules_block_an_exact_plan() {
    let mut value = manifest(vec![member("A", "a"), member("B", "b")]);
    value["modRules"] = json!([
        {"type":"after","source":{"tag":"a"},"reference":{"tag":"b"}},
        {"type":"after","source":{"tag":"b"},"reference":{"tag":"a"}}
    ]);
    let (_dir, package) = open(&value);
    let plan = CollectionPlan::build(&package, &options()).unwrap();
    assert!(plan.asset_order.is_empty());
    assert!(plan.blockers.iter().any(|d| d.code == "cyclic_mod_rules"));
    value["modRules"] = json!([{ "type":"after","source":{"tag":"a"},"reference":{"logicalFileName":"source.zip"}}]);
    let (_dir, package) = open(&value);
    assert!(CollectionPlan::build(&package, &options())
        .unwrap()
        .blockers
        .iter()
        .any(|d| d.code == "unresolved_mod_rule"));
}

#[test]
fn raw_extensions_survive_but_unsupported_behavior_is_reported() {
    let mut value = manifest(vec![member("A", "a")]);
    value["futureExtension"] = json!({"nested":[1,2,3]});
    value["mods"][0]["source"]["updatePolicy"] = json!("latest");
    let (_dir, package) = open(&value);
    assert_eq!(package.raw, value);
    assert_eq!(
        serde_json::to_value(&package.collection).unwrap()["futureExtension"],
        value["futureExtension"]
    );
    let plan = CollectionPlan::build(&package, &options()).unwrap();
    assert!(plan
        .blockers
        .iter()
        .any(|d| d.code == "unsupported_collection_field"));
    assert!(plan
        .blockers
        .iter()
        .any(|d| d.code == "unsupported_update_policy"));
}

#[test]
fn paths_reject_traversal_drives_reserved_names_and_ambiguous_components() {
    for value in [
        "../escape",
        "a/../escape",
        "/absolute",
        "C:\\game\\file",
        "a:stream",
        "a//b",
        "./file",
        "NUL.txt",
        "x. ",
        "COM1",
        "a\0b",
    ] {
        assert!(relative_path(value).is_err(), "{value:?}");
    }
    assert_eq!(
        relative_path("Textures\\Armor.dds").unwrap(),
        "Textures/Armor.dds"
    );
}

#[test]
fn directory_package_rejects_case_collisions_and_symlinks() {
    let (temp, _package) = open(&manifest(vec![]));
    std::fs::write(temp.path().join("COLLECTION.JSON"), b"{}").unwrap();
    assert!(CollectionPackage::open(temp.path()).is_err());
    std::fs::remove_file(temp.path().join("COLLECTION.JSON")).unwrap();
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink("/etc/passwd", temp.path().join("link")).unwrap();
        assert!(CollectionPackage::open(temp.path()).is_err());
    }
}

#[test]
fn zip_packages_are_inspected_without_extraction() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("package.zip");
    let mut zip = zip::ZipWriter::new(std::fs::File::create(&path).unwrap());
    zip.start_file("collection.json", zip::write::SimpleFileOptions::default())
        .unwrap();
    zip.write_all(&serde_json::to_vec(&manifest(vec![member("A", "a")])).unwrap())
        .unwrap();
    zip.finish().unwrap();
    let package = CollectionPackage::open(&path).unwrap();
    assert_eq!(package.collection.mods.len(), 1);
    assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 1);
}

#[test]
fn seven_zip_packages_read_manifest_and_payload_without_extraction() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(
        source.join("collection.json"),
        serde_json::to_vec(&manifest(vec![])).unwrap(),
    )
    .unwrap();
    std::fs::write(source.join("payload.txt"), b"payload").unwrap();
    let archive = temp.path().join("package.7z");
    sevenz_rust2::compress_to_path(&source, &archive).unwrap();
    let package = CollectionPackage::open(&archive).unwrap();
    assert_eq!(package.read("payload.txt", 1024).unwrap(), b"payload");
    assert!(!temp.path().join("payload.txt").exists());
}

#[test]
fn solid_seven_zip_reads_later_entries_after_consuming_earlier_streams() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("first.ini"), b"[General]\nValue=1\n").unwrap();
    std::fs::write(source.join("last.txt"), b"last payload").unwrap();
    std::fs::write(
        source.join("collection.json"),
        serde_json::to_vec(&manifest(vec![])).unwrap(),
    )
    .unwrap();
    let archive = temp.path().join("solid.7z");
    let result = std::process::Command::new("7z")
        .current_dir(&source)
        .args(["a", "-t7z", "-ms=on"])
        .arg(&archive)
        .args(["first.ini", "collection.json", "last.txt"])
        .output()
        .unwrap();
    assert!(result.status.success());
    let native =
        sevenz_rust2::ArchiveReader::open(&archive, sevenz_rust2::Password::empty()).unwrap();
    assert!(native.archive().is_solid);
    let package = CollectionPackage::open(&archive).unwrap();
    assert_eq!(
        package.read("first.ini", 1024).unwrap(),
        b"[General]\nValue=1\n"
    );
    assert_eq!(package.read("last.txt", 1024).unwrap(), b"last payload");
}

#[test]
fn preferred_sources_keep_pinned_identity_and_skse_tool_is_mapped() {
    let mut a = member("A", "a");
    a["source"]["updatePolicy"] = json!("prefer");
    let mut value = manifest(vec![a]);
    value["tools"] = json!([{"name":"Skyrim Script Extender 64","exe":"skse64_loader.exe","args":[],"cwd":"C:\\Games\\Skyrim Special Edition","detach":true}]);
    let (_dir, package) = open(&value);
    let plan = CollectionPlan::build(&package, &options()).unwrap();
    assert!(plan.blockers.is_empty());
    assert_eq!(plan.artifacts[0].file_id, 2);
    assert_eq!(
        plan.artifacts[0].expected_md5,
        "0123456789abcdef0123456789abcdef"
    );
    assert_eq!(
        plan.launcher_name.as_deref(),
        Some("Skyrim Script Extender 64")
    );
    assert!(plan
        .warnings
        .iter()
        .any(|d| d.code == "prefer_pinned_artifact"));
    value["tools"][0]["exe"] = json!("cmd.exe");
    let (_dir, package) = open(&value);
    assert!(CollectionPlan::build(&package, &options())
        .unwrap()
        .blockers
        .iter()
        .any(|d| d.code == "unsupported_collection_field"));
}

#[test]
fn missing_or_wrong_manifest_structure_is_not_an_empty_install() {
    let temp = tempfile::tempdir().unwrap();
    for bytes in [b"{}".as_slice(), b"{\"mods\":null}", b"[]", b"not-json"] {
        std::fs::write(temp.path().join("collection.json"), bytes).unwrap();
        assert!(CollectionPackage::open(temp.path()).is_err());
    }
}

#[test]
fn changed_patch_payload_invalidates_signature_even_with_same_crc() {
    let mut a = member("A", "a");
    a["patches"] = json!({"x.dll":"1234ABCD"});
    let (dir, _package) = open(&manifest(vec![a]));
    std::fs::create_dir_all(dir.path().join("patches/A")).unwrap();
    std::fs::write(dir.path().join("patches/A/x.dll.diff"), b"BSDIFF40first").unwrap();
    let p1 =
        CollectionPlan::build(&CollectionPackage::open(dir.path()).unwrap(), &options()).unwrap();
    std::fs::write(dir.path().join("patches/A/x.dll.diff"), b"BSDIFF40second").unwrap();
    let p2 =
        CollectionPlan::build(&CollectionPackage::open(dir.path()).unwrap(), &options()).unwrap();
    assert_ne!(
        p1.members[0].install_signature,
        p2.members[0].install_signature
    );
}

#[test]
fn signed_source_urls_stay_out_of_plan_reports() {
    let mut a = member("A", "a");
    a["source"]["type"] = json!("direct");
    a["source"]["url"] = json!("https://example.org/file.zip?token=do-not-log-this#private");
    let (_dir, package) = open(&manifest(vec![a]));
    let plan =
        serde_json::to_string(&CollectionPlan::build(&package, &options()).unwrap()).unwrap();
    assert!(!plan.contains("do-not-log-this"));
    assert!(!plan.contains("https://"));
}

#[test]
fn collection_url_is_host_validated_and_revision_pinned() {
    let url = parse_collection_url(
        "https://www.nexusmods.com/games/skyrimspecialedition/collections/qfftpq/revisions/12",
    )
    .unwrap();
    assert_eq!(url.revision, Some(12));
    assert_eq!(url.slug, "qfftpq");
    for bad in [
        "https://evil.test/nexusmods.com/games/skyrim/collections/a",
        "https://nexusmods.com.evil.test/skyrim/collections/a",
        "https://token@nexusmods.com/skyrim/collections/a",
        "https://nexusmods.com/skyrim/collections/a/revisions/1?revision=2",
        "https://nexusmods.com/skyrim/collections/a/revisions/0",
    ] {
        assert!(parse_collection_url(bad).is_err(), "{bad}");
    }
}

#[test]
fn host_responses_cannot_change_job_revision_or_receive_credentials() {
    let locator = parse_collection_url(
        "https://www.nexusmods.com/games/skyrimspecialedition/collections/qfftpq/revisions/12",
    )
    .unwrap();
    let response = json!({"type":"collection_package_result","job_id":"job","request_id":"request","locator":locator,"schema_id":1,"package_path":"/tmp/collection.7z"});
    let command: CollectionHostCommand = serde_json::from_value(response.clone()).unwrap();
    assert!(command.validate_package("job", "request", &locator).is_ok());
    assert!(command
        .validate_package("other", "request", &locator)
        .is_err());
    let mut wrong = response.clone();
    wrong["locator"]["revision"] = json!(13);
    let command: CollectionHostCommand = serde_json::from_value(wrong).unwrap();
    assert!(command
        .validate_package("job", "request", &locator)
        .is_err());
    let mut credentials = response;
    credentials["api_key"] = json!("never-pass-a-key");
    assert!(serde_json::from_value::<CollectionHostCommand>(credentials).is_err());
}

fn archive_fixture(
    temp: &tempfile::TempDir,
    files: &[(&str, &[u8])],
    recorded: Vec<Value>,
) -> (
    CollectionPackage,
    CollectionPlan,
    std::collections::BTreeMap<String, std::path::PathBuf>,
) {
    let archive_path = temp.path().join("source.zip");
    let mut archive = zip::ZipWriter::new(std::fs::File::create(&archive_path).unwrap());
    for (path, bytes) in files {
        archive
            .start_file(*path, zip::write::SimpleFileOptions::default())
            .unwrap();
        archive.write_all(bytes).unwrap();
    }
    archive.finish().unwrap();
    let mut a = member("A", "a");
    a["source"]["md5"] = json!(format!(
        "{:x}",
        md5::compute(std::fs::read(&archive_path).unwrap())
    ));
    a["hashes"] = json!(recorded);
    let root = temp.path().join("package");
    std::fs::create_dir(&root).unwrap();
    std::fs::write(
        root.join("collection.json"),
        serde_json::to_vec(&manifest(vec![a])).unwrap(),
    )
    .unwrap();
    let package = CollectionPackage::open(&root).unwrap();
    let plan = CollectionPlan::build(&package, &options()).unwrap();
    let artifacts =
        std::collections::BTreeMap::from([(plan.artifacts[0].id.clone(), archive_path)]);
    (package, plan, artifacts)
}

#[test]
fn raw_install_preserves_framework_paths_and_only_peels_confirmed_wrappers() {
    for (source, expected) in [
        (
            "NetScriptFramework/Plugins/GrassControl.dll",
            "NetScriptFramework/Plugins/GrassControl.dll",
        ),
        (
            "NetScriptFramework/Plugins/GrassControl.config.txt",
            "NetScriptFramework/Plugins/GrassControl.config.txt",
        ),
        ("MCM/Settings/TrueHUD.ini", "MCM/Settings/TrueHUD.ini"),
        (
            "UnknownFramework/Plugins/Future.dll",
            "UnknownFramework/Plugins/Future.dll",
        ),
        (
            "UnknownFramework/Settings/Future.ini",
            "UnknownFramework/Settings/Future.ini",
        ),
        (
            "Wrapped mod/NetScriptFramework/Plugins/GrassControl.dll",
            "NetScriptFramework/Plugins/GrassControl.dll",
        ),
        (
            "Outer/Inner/Data/MCM/Settings/TrueHUD.ini",
            "MCM/Settings/TrueHUD.ini",
        ),
        ("Wrapped mod/Patch.esp", "Patch.esp"),
        ("İnner/Textures/test.dds", "Textures/test.dds"),
        ("Wrapped mod/SKSE/Plugins/Test.dll", "SKSE/Plugins/Test.dll"),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let (package, plan, artifacts) = archive_fixture(&temp, &[(source, b"payload")], vec![]);
        let root = temp.path().join("job");
        let job =
            clf3::collection::stage::stage(&package, &plan, &artifacts, &root, &Default::default())
                .unwrap();
        let staged = &job.members[&plan.members[0].id];
        assert_eq!(staged.files.len(), 1);
        assert_eq!(staged.files[0].staged_path, expected, "source: {source}");
        assert_eq!(staged.files[0].deployment_root, "data");
        assert_eq!(
            std::fs::read(root.join(&staged.directory).join(expected)).unwrap(),
            b"payload"
        );
    }
}

#[test]
fn raw_install_matches_basic_prefix_selection_and_retains_other_variants() {
    for (files, expected) in [
        (
            vec![
                ("Variant A/Patch.esp", b"a".as_slice()),
                ("Variant B/Patch.esp", b"b"),
            ],
            vec!["Patch.esp", "Variant B/Patch.esp"],
        ),
        (
            vec![
                ("Wrapper/Textures/test.dds", b"texture".as_slice()),
                ("readme.txt", b"readme"),
            ],
            vec!["Textures/test.dds", "readme.txt"],
        ),
        (
            vec![
                ("No Fur/Meshes/armor.nif", b"no fur".as_slice()),
                ("Original/Armor.esl", b"plugin"),
                ("Original/Meshes/armor.nif", b"fur"),
            ],
            vec![
                "Meshes/armor.nif",
                "Original/Armor.esl",
                "Original/Meshes/armor.nif",
            ],
        ),
        (
            vec![
                ("No Fur/Meshes/armor.nif", b"no fur".as_slice()),
                ("Armor.esl", b"plugin"),
            ],
            vec!["No Fur/Meshes/armor.nif", "Armor.esl"],
        ),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let (package, plan, artifacts) = archive_fixture(&temp, &files, vec![]);
        let job = clf3::collection::stage::stage(
            &package,
            &plan,
            &artifacts,
            &temp.path().join("job"),
            &Default::default(),
        )
        .unwrap();
        let staged = &job.members[&plan.members[0].id];
        let outputs: std::collections::BTreeSet<_> = staged
            .files
            .iter()
            .map(|f| f.staged_path.as_str())
            .collect();
        assert_eq!(outputs, expected.into_iter().collect());
    }
}

#[test]
fn recorded_files_reuse_identical_content_at_multiple_destinations() {
    let temp = tempfile::tempdir().unwrap();
    let hash = format!("{:x}", md5::compute(b"shared"));
    let (package, plan, artifacts) = archive_fixture(
        &temp,
        &[("variant/file.txt", b"shared")],
        vec![
            json!({"path":"a.txt","md5":hash}),
            json!({"path":"b.txt","md5":hash}),
        ],
    );
    let job = clf3::collection::stage::stage(
        &package,
        &plan,
        &artifacts,
        &temp.path().join("job"),
        &Default::default(),
    )
    .unwrap();
    assert_eq!(job.status, "staged");
    let staged = &job.members[&plan.members[0].id];
    assert_eq!(staged.files.len(), 2);
    for file in &staged.files {
        assert_eq!(
            std::fs::read(
                temp.path()
                    .join("job")
                    .join(&staged.directory)
                    .join(&file.staged_path)
            )
            .unwrap(),
            b"shared"
        );
    }
}

#[test]
fn missing_recorded_outputs_and_fresh_fomods_never_complete_a_member() {
    for fresh in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let records = if fresh {
            vec![]
        } else {
            vec![json!({"path":"a.txt","md5":"00000000000000000000000000000000"})]
        };
        let (package, plan, artifacts) = archive_fixture(
            &temp,
            &[
                ("fomod/ModuleConfig.xml", b"<config/>"),
                ("variant/file.txt", b"shared"),
            ],
            records,
        );
        let output = temp.path().join("job");
        assert!(clf3::collection::stage::stage(
            &package,
            &plan,
            &artifacts,
            &output,
            &Default::default()
        )
        .is_err());
        let job: Value =
            serde_json::from_slice(&std::fs::read(output.join("collection-job.json")).unwrap())
                .unwrap();
        assert_eq!(job["status"], "incomplete");
        assert_eq!(job["members"], json!({}));
    }
}

#[test]
fn patches_apply_before_root_routing_and_crc_failures_block_completion() {
    for wrong_crc in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let (package, _plan, artifacts) = archive_fixture(
            &temp,
            &[
                ("skse64_loader.exe", b"before"),
                ("Data/Scripts/test.pex", b"script"),
            ],
            vec![],
        );
        let mut raw = package.raw.clone();
        raw["mods"][0]["patches"] = json!({"skse64_loader.exe":if wrong_crc {"00000000".into()} else {format!("{:08X}",crc32fast::hash(b"before"))}});
        let root = temp.path().join("package");
        std::fs::write(
            root.join("collection.json"),
            serde_json::to_vec(&raw).unwrap(),
        )
        .unwrap();
        std::fs::create_dir_all(root.join("patches/A")).unwrap();
        let mut diff = Vec::new();
        qbsdiff::Bsdiff::new(b"before", b"after")
            .compare(&mut diff)
            .unwrap();
        std::fs::write(root.join("patches/A/skse64_loader.exe.diff"), diff).unwrap();
        let package = CollectionPackage::open(&root).unwrap();
        let plan = CollectionPlan::build(&package, &options()).unwrap();
        let result = clf3::collection::stage::stage(
            &package,
            &plan,
            &artifacts,
            &temp.path().join("job"),
            &Default::default(),
        );
        if wrong_crc {
            assert!(result.is_err());
            continue;
        }
        let job = result.unwrap();
        let staged = &job.members[&plan.members[0].id];
        let path = temp.path().join("job").join(&staged.directory);
        assert_eq!(
            std::fs::read(path.join("Root/skse64_loader.exe")).unwrap(),
            b"after"
        );
        assert_eq!(
            std::fs::read(path.join("Scripts/test.pex")).unwrap(),
            b"script"
        );
    }
}

#[test]
fn resume_verifies_outputs_and_preserves_modified_old_generations() {
    let temp = tempfile::tempdir().unwrap();
    let (package, plan, artifacts) = archive_fixture(&temp, &[("test.esp", b"original")], vec![]);
    let output = temp.path().join("job");
    let first =
        clf3::collection::stage::stage(&package, &plan, &artifacts, &output, &Default::default())
            .unwrap();
    let second =
        clf3::collection::stage::stage(&package, &plan, &artifacts, &output, &Default::default())
            .unwrap();
    let id = &plan.members[0].id;
    assert_eq!(first.members[id].directory, second.members[id].directory);
    let old = output.join(&first.members[id].directory).join("test.esp");
    std::fs::write(&old, b"user modification").unwrap();
    let repaired =
        clf3::collection::stage::stage(&package, &plan, &artifacts, &output, &Default::default())
            .unwrap();
    assert_ne!(first.members[id].directory, repaired.members[id].directory);
    assert_eq!(std::fs::read(&old).unwrap(), b"user modification");
    assert_eq!(
        std::fs::read(
            output
                .join(&repaired.members[id].directory)
                .join("test.esp")
        )
        .unwrap(),
        b"original"
    );
}

#[test]
#[cfg(target_os = "linux")]
fn indexed_resume_rejects_split_directory_case_without_losing_old_files() {
    let temp = tempfile::tempdir().unwrap();
    let (package, plan, artifacts) = archive_fixture(
        &temp,
        &[("Meshes/A.nif", b"a"), ("Meshes/B.nif", b"b")],
        vec![],
    );
    let output = temp.path().join("job");
    let first =
        clf3::collection::stage::stage(&package, &plan, &artifacts, &output, &Default::default())
            .unwrap();
    let id = &plan.members[0].id;
    let old = output.join(&first.members[id].directory);
    std::fs::create_dir(old.join("meshes")).unwrap();
    std::fs::rename(old.join("Meshes/B.nif"), old.join("meshes/B.nif")).unwrap();
    let repaired =
        clf3::collection::stage::stage(&package, &plan, &artifacts, &output, &Default::default())
            .unwrap();
    assert_ne!(first.members[id].directory, repaired.members[id].directory);
    assert_eq!(std::fs::read(old.join("meshes/B.nif")).unwrap(), b"b");
    assert!(output
        .join(&repaired.members[id].directory)
        .join("Meshes/B.nif")
        .is_file());
}

#[test]
fn staging_refuses_existing_instances_and_records_cancellation() {
    let temp = tempfile::tempdir().unwrap();
    let (package, plan, artifacts) = archive_fixture(&temp, &[("test.esp", b"original")], vec![]);
    let live = temp.path().join("live");
    std::fs::create_dir(&live).unwrap();
    std::fs::write(live.join("ModOrganizer.ini"), b"untouched").unwrap();
    assert!(clf3::collection::stage::stage(
        &package,
        &plan,
        &artifacts,
        &live,
        &Default::default()
    )
    .is_err());
    assert_eq!(std::fs::read_dir(&live).unwrap().count(), 1);
    let token = tokio_util::sync::CancellationToken::new();
    token.cancel();
    let output = temp.path().join("cancelled");
    assert!(clf3::collection::stage::stage(&package, &plan, &artifacts, &output, &token).is_err());
    let journal: Value =
        serde_json::from_slice(&std::fs::read(output.join("collection-job.json")).unwrap())
            .unwrap();
    assert_eq!(journal["status"], "cancelled");
    assert_eq!(journal["members"], json!({}));
}

#[test]
fn bundle_exclusions_are_retained_for_host_publication() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("package");
    std::fs::create_dir_all(root.join("bundled/one/textures")).unwrap();
    std::fs::write(root.join("bundled/one/textures/x.dds"), b"texture").unwrap();
    let mut a = member("A", "a");
    a["source"] = json!({"type":"bundle","fileExpression":"one","tag":"a"});
    a["fileOverrides"] = json!(["textures/x.dds"]);
    std::fs::write(
        root.join("collection.json"),
        serde_json::to_vec(&manifest(vec![a])).unwrap(),
    )
    .unwrap();
    let package = CollectionPackage::open(&root).unwrap();
    let plan = CollectionPlan::build(&package, &options()).unwrap();
    let staged = clf3::collection::stage::stage(
        &package,
        &plan,
        &Default::default(),
        &temp.path().join("job"),
        &Default::default(),
    )
    .unwrap();
    assert!(staged.members[&plan.members[0].id].files[0].excluded);
}
