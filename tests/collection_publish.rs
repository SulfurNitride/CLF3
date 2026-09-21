use clf3::collection::{
    publish::publish, stage::stage, CollectionPackage, CollectionPlan, PlanOptions,
};
use serde_json::json;
use std::{collections::BTreeMap, path::Path};

fn write(path: &Path, bytes: impl AsRef<[u8]>) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, bytes).unwrap();
}

fn plugin(master: Option<&str>, esm: bool) -> Vec<u8> {
    let mut body = Vec::new();
    if let Some(master) = master {
        body.extend_from_slice(b"MAST");
        body.extend_from_slice(&((master.len() + 1) as u16).to_le_bytes());
        body.extend_from_slice(master.as_bytes());
        body.push(0);
    }
    let mut bytes = b"TES4".to_vec();
    bytes.extend_from_slice(&(body.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&(if esm { 1u32 } else { 0u32 }).to_le_bytes());
    bytes.extend_from_slice(&[0u8; 12]);
    bytes.extend_from_slice(&body);
    bytes
}

#[test]
fn publication_uses_isolated_game_and_preserves_original_root_files() {
    for missing_master in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let package_dir = temp.path().join("package");
        write(
            &package_dir.join("collection.json"),
            serde_json::to_vec(&json!({
                "info":{"domainName":"skyrimspecialedition","name":"Test"},
                "mods":[{"name":"A","source":{"type":"bundle","fileExpression":"A","tag":"a"}}],
                "plugins":[{"name":"a.esp","enabled":true},{"name":"b.esp","enabled":true}]
            }))
            .unwrap(),
        );
        write(&package_dir.join("bundled/A/Root/version.dll"), b"modded");
        write(
            &package_dir.join("INI Tweaks/Profile [Skyrim].ini"),
            "[General]\nbAlwaysActive=1\nsIntroSequence=\n",
        );
        write(
            &package_dir.join("bundled/A/meta.ini"),
            b"[ArchiveMetadata]\noriginal=true\n",
        );
        write(
            &package_dir.join("bundled/A/Data/A.esp"),
            plugin(
                Some(if missing_master {
                    "Missing.esm"
                } else {
                    "B.esp"
                }),
                false,
            ),
        );
        let a_path = package_dir.join("bundled/A/Data/A.esp");
        let mut light = std::fs::read(&a_path).unwrap();
        light[8..12].copy_from_slice(&0x200u32.to_le_bytes());
        write(&a_path, light);
        write(
            &package_dir.join("bundled/A/Data/B.esp"),
            plugin(Some("Skyrim.esm"), false),
        );
        write(
            &package_dir.join("bundled/A/Data/Unselected.esp"),
            plugin(Some("AbsentOptionalMaster.esm"), false),
        );
        let package = CollectionPackage::open(&package_dir).unwrap();
        let plan = CollectionPlan::build(&package, &PlanOptions::default()).unwrap();
        let job = temp.path().join("job");
        stage(&package, &plan, &BTreeMap::new(), &job, &Default::default()).unwrap();
        let game = temp.path().join("game");
        write(&game.join("SkyrimSE.exe"), b"game");
        write(&game.join("Skyrim_Default.ini"), b"[General]\n");
        write(&game.join("version.dll"), b"original");
        write(&game.join("Data/Skyrim.esm"), plugin(None, true));
        let ini = temp.path().join("ini");
        write(&ini.join("Skyrim.ini"), "[General]\n");
        write(
            &temp.path().join("other-profile.ini"),
            "[OtherProfile]\nDoNotInherit=true\n",
        );
        #[cfg(unix)]
        std::os::unix::fs::symlink(
            temp.path().join("other-profile.ini"),
            ini.join("SkyrimCustom.ini"),
        )
        .unwrap();
        let output = temp.path().join("instance");
        let result = publish(
            &job,
            &game,
            &output,
            &clf3::collection::publish::PublicationOptions {
                profile_ini: Some(&ini),
                ..Default::default()
            },
            &Default::default(),
        );
        assert_eq!(
            std::fs::read(game.join("version.dll")).unwrap(),
            b"original"
        );
        if missing_master {
            assert!(result.unwrap_err().to_string().contains("missing master"));
            assert!(!output.exists());
        } else {
            let report = result.unwrap();
            assert_eq!(report.installed_members, 1);
            assert_eq!(report.preserved_metadata_files, 1);
            assert_eq!(report.profile_ini_edits, 2);
            assert_eq!(
                std::fs::read_to_string(ini.join("Skyrim.ini")).unwrap(),
                "[General]\n"
            );
            assert_eq!(
                std::fs::read_to_string(output.join("profiles/Default/Skyrim.ini")).unwrap(),
                "[General]\nbAlwaysActive=1\nsIntroSequence=\n"
            );
            assert_eq!(
                std::fs::read_to_string(output.join(".collection/ini-tweaks/Skyrim.ini.before"))
                    .unwrap(),
                "[General]\n"
            );
            let installed: serde_json::Value = serde_json::from_slice(
                &std::fs::read(output.join(".collection/installation.json")).unwrap(),
            )
            .unwrap();
            let m = installed["members"]
                .as_object()
                .unwrap()
                .values()
                .next()
                .unwrap();
            let file = m["files"]
                .as_array()
                .unwrap()
                .iter()
                .find(|f| f["deployment_root"] == "metadata")
                .unwrap();
            let preserved = output
                .join(m["directory"].as_str().unwrap())
                .join(file["staged_path"].as_str().unwrap());
            assert_eq!(
                std::fs::read(preserved).unwrap(),
                b"[ArchiveMetadata]\noriginal=true\n"
            );
            assert_eq!(report.enabled_plugins, 3);
            assert_eq!(
                std::fs::read_to_string(output.join("profiles/Default/settings.ini")).unwrap(),
                "[General]\nLocalSaves=true\nLocalSettings=true\n"
            );
            assert_eq!(
                std::fs::read(output.join("profiles/Default/plugins.txt")).unwrap(),
                std::fs::read(output.join(".collection/profile-snapshot/plugins.txt")).unwrap()
            );
            assert_eq!(
                std::fs::read(output.join("Stock Game/version.dll")).unwrap(),
                b"modded"
            );
            assert!(output.join("ModOrganizer.ini").is_file());
            assert!(
                !std::fs::read_to_string(output.join("profiles/Default/SkyrimCustom.ini"))
                    .unwrap()
                    .contains("DoNotInherit")
            );
            assert_eq!(
                std::fs::read_to_string(output.join("profiles/Default/loadorder.txt")).unwrap(),
                "Skyrim.esm\nB.esp\nA.esp\nUnselected.esp\n"
            );
            assert_eq!(
                std::fs::read_to_string(output.join("profiles/Default/plugins.txt")).unwrap(),
                "# Generated by CLF3 Collections\n*Skyrim.esm\n*B.esp\n*A.esp\nUnselected.esp\n"
            );
            assert_eq!(report.added_plugin_records, vec!["Unselected.esp"]);
            assert!(publish(
                &job,
                &game,
                &output,
                &Default::default(),
                &Default::default()
            )
            .is_err());
        }
    }
}

#[test]
fn publication_rejects_a_missing_authored_enabled_plugin() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("package");
    write(
        &package_dir.join("collection.json"),
        serde_json::to_vec(&json!({
            "info":{"domainName":"skyrimspecialedition","name":"Missing plugin"},
            "mods":[],"plugins":[{"name":"Required.esp","enabled":true}]
        }))
        .unwrap(),
    );
    let package = CollectionPackage::open(&package_dir).unwrap();
    let plan = CollectionPlan::build(&package, &PlanOptions::default()).unwrap();
    let job = temp.path().join("job");
    stage(&package, &plan, &BTreeMap::new(), &job, &Default::default()).unwrap();
    let game = temp.path().join("game");
    write(&game.join("SkyrimSE.exe"), b"game");
    write(&game.join("Data/Skyrim.esm"), plugin(None, true));
    let output = temp.path().join("instance");
    let error = publish(
        &job,
        &game,
        &output,
        &Default::default(),
        &Default::default(),
    )
    .unwrap_err();
    assert!(error
        .to_string()
        .contains("Authored enabled plugin is missing"));
    assert!(!output.exists());
}

#[test]
fn provider_exclusions_publish_opposite_file_winners() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("package");
    write(&package_dir.join("collection.json"), serde_json::to_vec(&json!({
        "info":{"domainName":"skyrimspecialedition","name":"Exclusions"},
        "mods":[
            {"name":"A","source":{"type":"bundle","fileExpression":"A","tag":"a"},"fileOverrides":["textures/x.dds"]},
            {"name":"B","source":{"type":"bundle","fileExpression":"B","tag":"b"},"fileOverrides":["textures/y.dds"]}
        ]
    })).unwrap());
    for name in ["A", "B"] {
        for file in ["x.dds", "y.dds"] {
            write(
                &package_dir.join(format!("bundled/{name}/textures/{file}")),
                name,
            );
        }
    }
    let package = CollectionPackage::open(&package_dir).unwrap();
    let plan = CollectionPlan::build(&package, &PlanOptions::default()).unwrap();
    let job = temp.path().join("job");
    stage(&package, &plan, &BTreeMap::new(), &job, &Default::default()).unwrap();
    let game = temp.path().join("game");
    write(&game.join("SkyrimSE.exe"), b"game");
    write(&game.join("Skyrim_Default.ini"), b"[General]\n");
    write(&game.join("Data/Skyrim.esm"), plugin(None, true));
    let output = temp.path().join("instance");
    publish(
        &job,
        &game,
        &output,
        &Default::default(),
        &Default::default(),
    )
    .unwrap();
    let order = std::fs::read_to_string(output.join("profiles/Default/modlist.txt")).unwrap();
    let lines: Vec<_> = order.lines().filter(|l| !l.starts_with('#')).collect();
    assert_eq!(lines.first(), Some(&"-Mod Additions_separator"));
    assert_eq!(lines.last(), Some(&"-Collection Mods_separator"));
    assert_eq!(lines.iter().filter(|l| l.starts_with('+')).count(), 2);
    for name in ["Collection Mods_separator", "Mod Additions_separator"] {
        assert_eq!(
            std::fs::read_dir(output.join("mods").join(name))
                .unwrap()
                .count(),
            0
        );
    }
    let mut visible = BTreeMap::new();
    for line in order.lines().rev().filter_map(|l| l.strip_prefix('+')) {
        for file in ["x.dds", "y.dds"] {
            let path = output.join("mods").join(line).join("textures").join(file);
            if path.is_file() {
                visible.insert(file, std::fs::read(path).unwrap());
            }
        }
    }
    assert_eq!(visible["x.dds"], b"B");
    assert_eq!(visible["y.dds"], b"A");
}
