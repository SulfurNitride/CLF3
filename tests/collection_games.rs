//! Cross-game conformance uses small synthetic payloads, never user installs.
use clf3::collection::{
    games::{self, GameProfile},
    package::digest_file,
    publish::{publish, PublicationOptions},
    stage::stage,
    worker::{execute, recover, WorkerRequest},
    CollectionPackage, CollectionPlan, PlanOptions,
};
use clf3::collection_app::{atomic_json, Inputs};
use serde_json::json;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

fn write(path: &Path, bytes: impl AsRef<[u8]>) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, bytes).unwrap();
}

fn plugin(game: &GameProfile, master: Option<&str>, flags: u32) -> Vec<u8> {
    let mut body = b"HEDR\x0c\0".to_vec();
    body.extend_from_slice(&1.0f32.to_le_bytes());
    body.extend_from_slice(&0u32.to_le_bytes());
    body.extend_from_slice(&0x800u32.to_le_bytes());
    if let Some(master) = master {
        body.extend_from_slice(b"MAST");
        body.extend_from_slice(&((master.len() + 1) as u16).to_le_bytes());
        body.extend_from_slice(master.as_bytes());
        body.push(0);
        body.extend_from_slice(b"DATA\x08\0");
        body.extend_from_slice(&[0; 8]);
    }
    let mut data = vec![0; game.plugin_header_size];
    data[..4].copy_from_slice(b"TES4");
    data[4..8].copy_from_slice(&(body.len() as u32).to_le_bytes());
    data[8..12].copy_from_slice(&flags.to_le_bytes());
    data.extend(body);
    data
}

fn fixture(root: &Path, game: &GameProfile) -> (CollectionPackage, CollectionPlan) {
    let dir = root.join("package");
    write(&dir.join("collection.json"), serde_json::to_vec(&json!({
        "info":{"domainName":game.domain,"name":"Cross-game fixture"},
        "mods":[{"name":"Exact payload","source":{"type":"bundle","fileExpression":"A","tag":"a"}}],
        "plugins":[{"name":"A.esp","enabled":true},{"name":"B.esp","enabled":true},{"name":"Off.esp","enabled":false}]
    })).unwrap());
    write(
        &dir.join(format!("bundled/A/{}", game.extender)),
        b"fixture extender",
    );
    write(
        &dir.join(format!(
            "bundled/A/Data/{}/Plugins/Keep.dll",
            game.extender_directory
        )),
        b"extender plugin",
    );
    write(
        &dir.join("bundled/A/Data/A.esp"),
        plugin(game, Some("B.esp"), 0),
    );
    write(
        &dir.join("bundled/A/Data/B.esp"),
        plugin(game, Some(game.base_plugins[0]), 0),
    );
    write(
        &dir.join("bundled/A/Data/Off.esp"),
        plugin(game, Some("MissingOptional.esm"), 0),
    );
    let stem = game.ini_files[0].trim_end_matches(".ini");
    write(
        &dir.join(format!("INI Tweaks/Profile [{stem}].ini")),
        b"[General]\nbAlwaysActive=1\n",
    );
    write(
        &root.join("game").join(game.executable),
        b"fixture game executable",
    );
    write(
        &root.join("game").join(game.default_ini),
        b"[General]\noriginal=1\n",
    );
    write(
        &root.join("game/Data").join(game.base_plugins[0]),
        plugin(game, None, 1),
    );
    let package = CollectionPackage::open(&dir).unwrap();
    let plan = CollectionPlan::build(
        &package,
        &PlanOptions {
            schema_id: Some(1),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(
        plan.blockers.is_empty(),
        "{}: {:?}",
        game.domain,
        plan.blockers
    );
    (package, plan)
}

#[test]
fn each_game_publishes_its_own_layout_ini_launcher_and_activation_with_and_without_loot() {
    for game in games::PROFILES {
        for with_loot in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let root = temp.path();
            let (package, plan) = fixture(root, game);
            let source = root.join("game/Data").join(game.base_plugins[0]);
            let source_hash = digest_file(&source).unwrap();
            let source_time = source.metadata().unwrap().modified().unwrap();
            let staged = stage(
                &package,
                &plan,
                &BTreeMap::new(),
                &root.join("stage"),
                &Default::default(),
            )
            .unwrap();
            let member = staged.members.values().next().unwrap();
            assert!(member
                .files
                .iter()
                .any(|f| f.staged_path == format!("Root/{}", game.extender)
                    && f.deployment_root == "game"));
            assert!(member.files.iter().any(|f| f.staged_path
                == format!("{}/Plugins/Keep.dll", game.extender_directory)
                && f.deployment_root == "data"));
            let masterlist = root.join("masterlist.yaml");
            write(&masterlist, b"plugins: []\n");
            let masterlist = std::env::var_os("CLF3_GAME_MASTERLIST_DIR")
                .map(|p| PathBuf::from(p).join(game.domain).join("masterlist.yaml"))
                .filter(|p| p.is_file())
                .unwrap_or(masterlist);
            let output = root.join("installed");
            let report = publish(
                &root.join("stage"),
                &root.join("game"),
                &output,
                &PublicationOptions {
                    masterlist: with_loot.then_some(masterlist.as_path()),
                    ..Default::default()
                },
                &Default::default(),
            )
            .unwrap_or_else(|e| panic!("{} LOOT={with_loot}: {e:#}", game.domain));
            assert_eq!(report.enabled_plugins, 3);
            assert_eq!(report.profile_ini_edits, 1);
            assert_eq!(report.verified_mod_files, 5);
            assert_eq!(digest_file(&source).unwrap(), source_hash);
            assert_eq!(source.metadata().unwrap().modified().unwrap(), source_time);
            let config = std::fs::read_to_string(output.join("ModOrganizer.ini")).unwrap();
            assert!(config.contains(&format!("gameName={}\n", game.manager_name())));
            assert!(config.contains(&format!("/Stock Game/{}\n", game.extender)));
            if game.domain != "skyrimspecialedition" {
                assert!(!config.contains("SkyrimSE.exe"));
                assert!(!config.contains("skse64_loader.exe"));
            }
            let profile = output.join("profiles/Default");
            let ini = std::fs::read_to_string(profile.join(game.ini_files[0])).unwrap();
            assert!(ini.contains("bAlwaysActive=1"));
            let activation = std::fs::read_to_string(profile.join("plugins.txt")).unwrap();
            if game.asterisk_plugins {
                assert!(activation.contains("*A.esp"));
                assert!(activation.contains("Off.esp"));
            } else {
                assert!(activation.contains("\nA.esp\n"));
                assert!(!activation.contains('*'));
                assert!(!activation.contains("Off.esp"));
            }
            let order = std::fs::read_to_string(profile.join("loadorder.txt")).unwrap();
            assert!(order.find(game.base_plugins[0]).unwrap() < order.find("B.esp").unwrap());
            assert!(order.find("B.esp").unwrap() < order.find("A.esp").unwrap());
            if game.timestamp_order {
                let times: BTreeMap<String, u64> = serde_json::from_slice(
                    &std::fs::read(output.join(".collection/plugin-timestamps.json")).unwrap(),
                )
                .unwrap();
                let a = times.iter().find(|(p, _)| p.ends_with("/A.esp")).unwrap().1;
                let b = times.iter().find(|(p, _)| p.ends_with("/B.esp")).unwrap().1;
                assert!(a > b);
                for (path, seconds) in times {
                    assert_eq!(
                        output
                            .join(path)
                            .metadata()
                            .unwrap()
                            .modified()
                            .unwrap()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_secs(),
                        seconds
                    );
                }
            }
            if with_loot {
                let result = std::process::Command::new("python3")
                    .arg(concat!(
                        env!("CARGO_MANIFEST_DIR"),
                        "/scripts/collection_verify.py"
                    ))
                    .arg(&output)
                    .arg("--output")
                    .arg(root.join("audit.json"))
                    .output()
                    .unwrap();
                assert!(
                    result.status.success(),
                    "{} independent audit: {} {}",
                    game.domain,
                    String::from_utf8_lossy(&result.stdout),
                    String::from_utf8_lossy(&result.stderr)
                );
            }
        }
    }
}

#[test]
fn wrong_game_and_cross_game_ini_are_refused_before_installation() {
    let temp = tempfile::tempdir().unwrap();
    let game = games::require("fallout4").unwrap();
    let (_, _) = fixture(temp.path(), game);
    let inputs = Inputs {
        source: temp.path().join("package").to_string_lossy().into_owned(),
        game: temp.path().join("wrong-game"),
        cache: temp.path().join("cache"),
        ..Default::default()
    };
    write(&inputs.game.join("SkyrimSE.exe"), b"wrong game");
    let error = clf3::collection_app::prepare(inputs, None, "", &Default::default())
        .err()
        .expect("wrong source game must fail");
    assert!(format!("{error:#}").contains("Fallout4.exe"));
    write(
        &temp.path().join("package/INI Tweaks/Wrong [Skyrim].ini"),
        b"[General]\nx=1\n",
    );
    let package = CollectionPackage::open(&temp.path().join("package")).unwrap();
    let plan = CollectionPlan::build(&package, &PlanOptions::default()).unwrap();
    assert!(plan
        .blockers
        .iter()
        .any(|b| b.code == "unsupported_ini_tweak"));
}

#[test]
fn classic_games_reject_light_plugins_and_encode_activation_without_stars() {
    for domain in ["newvegas", "fallout3", "oblivion", "skyrim"] {
        let game = games::require(domain).unwrap();
        let temp = tempfile::tempdir().unwrap();
        let (package, plan) = fixture(temp.path(), game);
        // Change the bundle before opening/replanning; no plan mutation bypass.
        drop(package);
        drop(plan);
        write(
            &temp.path().join("package/bundled/A/Data/A.esp"),
            plugin(game, Some("B.esp"), 0x200),
        );
        let package = CollectionPackage::open(&temp.path().join("package")).unwrap();
        let plan = CollectionPlan::build(&package, &PlanOptions::default()).unwrap();
        stage(
            &package,
            &plan,
            &BTreeMap::new(),
            &temp.path().join("stage"),
            &Default::default(),
        )
        .unwrap();
        let error = publish(
            &temp.path().join("stage"),
            &temp.path().join("game"),
            &temp.path().join("installed"),
            &Default::default(),
            &Default::default(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("does not support light plugins"));
        assert!(!temp.path().join("installed").exists());
        assert_eq!(
            game.plugin_list([("Café.esp", true), ("Off.esp", false)])
                .unwrap(),
            b"Caf\xe9.esp\n"
        );
    }
}

#[test]
fn worker_recovery_checks_each_games_ini_and_timestamp_state() {
    for domain in ["fallout4", "newvegas"] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let game = games::require(domain).unwrap();
        let (_, plan) = fixture(root, game);
        atomic_json(&root.join("plan.json"), &plan).unwrap();
        atomic_json(
            &root.join("artifacts.json"),
            &BTreeMap::<String, PathBuf>::new(),
        )
        .unwrap();
        let request = WorkerRequest {
            protocol_version: 1,
            job_identity: Some("game-fixture".into()),
            package: root.join("package"),
            plan: root.join("plan.json"),
            artifacts: root.join("artifacts.json"),
            stage: root.join("stage"),
            game: root.join("game"),
            output: root.join("installed"),
            profile_ini: None,
            masterlist: None,
            masterlist_sha256: None,
        };
        execute(&request, &Default::default(), &|_| {}).unwrap();
        recover(
            &request.output,
            "game-fixture",
            &plan,
            &Default::default(),
            &|_| {},
        )
        .unwrap();
        if game.timestamp_order {
            let manifest = request.output.join(".collection/plugin-timestamps.json");
            let original = std::fs::read(&manifest).unwrap();
            write(&manifest, b"{}");
            assert!(recover(
                &request.output,
                "game-fixture",
                &plan,
                &Default::default(),
                &|_| {}
            )
            .unwrap_err()
            .to_string()
            .contains("timestamps"));
            write(&manifest, &original);
            let path = request
                .output
                .join("Stock Game/Data")
                .join(game.base_plugins[0]);
            std::fs::File::open(path)
                .unwrap()
                .set_modified(std::time::SystemTime::now())
                .unwrap();
            assert!(recover(
                &request.output,
                "game-fixture",
                &plan,
                &Default::default(),
                &|_| {}
            )
            .unwrap_err()
            .to_string()
            .contains("timestamps"));
        } else {
            write(
                &request
                    .output
                    .join("profiles/Default")
                    .join(game.ini_files[0]),
                b"changed",
            );
            assert!(recover(
                &request.output,
                "game-fixture",
                &plan,
                &Default::default(),
                &|_| {}
            )
            .unwrap_err()
            .to_string()
            .contains("profile changed"));
        }
    }
}

#[test]
fn unimplemented_adapters_remain_blocked_with_game_specific_reasons() {
    for domain in [
        "cyberpunk2077",
        "stardewvalley",
        "baldursgate3",
        "morrowind",
        "starfield",
        "oblivionremastered",
        "skyrimvr",
    ] {
        let temp = tempfile::tempdir().unwrap();
        write(
            &temp.path().join("collection.json"),
            serde_json::to_vec(
                &json!({"info":{"domainName":domain,"name":"Unsupported"},"mods":[]}),
            )
            .unwrap(),
        );
        let package = CollectionPackage::open(temp.path()).unwrap();
        let plan = CollectionPlan::build(&package, &PlanOptions::default()).unwrap();
        assert!(plan.blockers.iter().any(
            |b| b.code == "unsupported_game" && b.message == games::unsupported_reason(domain)
        ));
        assert!(stage(
            &package,
            &plan,
            &BTreeMap::new(),
            &temp.path().join("stage"),
            &Default::default()
        )
        .is_err());
    }
}
