use clf3::collection::{
    package::digest_file,
    progress::WorkerEvent,
    worker::{execute, WorkerRequest},
    CollectionPackage, CollectionPlan, PlanOptions,
};
use clf3::collection_app::{atomic_json, credential_variable};
use serde_json::json;
use std::{
    collections::BTreeMap,
    io::{BufRead, BufReader, Write},
    path::Path,
    process::{Command, Stdio},
    sync::Mutex,
};

fn write(path: &Path, bytes: impl AsRef<[u8]>) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, bytes).unwrap();
}
fn fixture(root: &Path) -> WorkerRequest {
    let package = root.join("package");
    write(&package.join("collection.json"),serde_json::to_vec(&json!({
        "info":{"domainName":"skyrimspecialedition","name":"GUI fixture"},
        "mods":[{"name":"Required mod","source":{"type":"bundle","fileExpression":"A","tag":"a"}},
                {"name":"Optional mod","optional":true,"source":{"type":"bundle","fileExpression":"B","tag":"b"}}]
    })).unwrap());
    write(
        &package.join("bundled/A/Textures/required.txt"),
        b"required payload",
    );
    write(
        &package.join("bundled/B/Textures/optional.txt"),
        b"optional payload",
    );
    let opened = CollectionPackage::open(&package).unwrap();
    let plan = CollectionPlan::build(
        &opened,
        &PlanOptions {
            all_optional: true,
            ..Default::default()
        },
    )
    .unwrap();
    atomic_json(&root.join("plan.json"), &plan).unwrap();
    atomic_json(
        &root.join("artifacts.json"),
        &BTreeMap::<String, String>::new(),
    )
    .unwrap();
    write(&root.join("game/SkyrimSE.exe"), b"fixture executable");
    write(&root.join("game/Skyrim_Default.ini"), b"[General]\n");
    let mut plugin = b"TES4".to_vec();
    plugin.extend_from_slice(&[0u8; 20]);
    plugin[8] = 1;
    write(&root.join("game/Data/Skyrim.esm"), plugin);
    WorkerRequest {
        protocol_version: 1,
        job_identity: Some("gui-fixture-job".into()),
        package,
        plan: root.join("plan.json"),
        artifacts: root.join("artifacts.json"),
        stage: root.join("stage"),
        game: root.join("game"),
        output: root.join("installed"),
        profile_ini: None,
        masterlist: None,
        masterlist_sha256: None,
    }
}

#[test]
fn worker_installs_reviewed_optional_choices_and_verifies_before_success() {
    let temp = tempfile::tempdir().unwrap();
    let request = fixture(temp.path());
    let before = digest_file(&request.game.join("SkyrimSE.exe")).unwrap();
    let events = Mutex::new(Vec::new());
    let path = execute(&request, &Default::default(), &|e| {
        events.lock().unwrap().push(e)
    })
    .unwrap();
    let report: serde_json::Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    assert_eq!(report["installed_members"], 2);
    assert_eq!(report["verified_mod_files"], 2);
    assert_eq!(
        before,
        digest_file(&request.game.join("SkyrimSE.exe")).unwrap()
    );
    let modlist =
        std::fs::read_to_string(request.output.join("profiles/Default/modlist.txt")).unwrap();
    assert!(modlist.contains("-Collection Mods_separator"));
    assert!(modlist.contains("-Mod Additions_separator"));
    assert!(events.lock().unwrap().iter().any(
        |e| matches!(e,WorkerEvent::Progress{phase,..} if phase=="Verifying installed files")
    ));
    let notes =
        std::fs::read_to_string(request.output.join(".collection/launch-notes.txt")).unwrap();
    assert!(!notes.contains("Fluorine"));
    assert!(
        execute(&request, &Default::default(), &|_| {}).is_err(),
        "Never replace an existing destination"
    );
}

#[test]
fn cancellation_resumes_but_changed_plan_or_masterlist_cannot() {
    let temp = tempfile::tempdir().unwrap();
    let mut request = fixture(temp.path());
    let token = tokio_util::sync::CancellationToken::new();
    assert!(execute(
        &request,
        &token,
        &|event| if matches!(event, WorkerEvent::Progress { .. }) {
            token.cancel();
        }
    )
    .is_err());
    assert!(!request.output.exists());
    let masterlist = temp.path().join("masterlist.yaml");
    write(&masterlist, b"plugins: []\n");
    request.masterlist = Some(masterlist.clone());
    request.masterlist_sha256 = Some("incorrect".into());
    assert!(execute(&request, &Default::default(), &|_| {})
        .unwrap_err()
        .to_string()
        .contains("masterlist changed"));
    request.masterlist = None;
    request.masterlist_sha256 = None;
    let original = std::fs::read(&request.plan).unwrap();
    let mut changed: serde_json::Value = serde_json::from_slice(&original).unwrap();
    changed["members"][0]["version"] = json!("substituted");
    atomic_json(&request.plan, &changed).unwrap();
    assert!(execute(&request, &Default::default(), &|_| {})
        .unwrap_err()
        .to_string()
        .contains("plan changed"));
    write(&request.plan, original);
    execute(&request, &Default::default(), &|_| {}).unwrap();
}

#[test]
fn worker_stdio_and_environment_boundary() {
    for name in [
        "NEXUS_API_KEY",
        "NXM_TOKEN",
        "Cookie",
        "some_SECRET",
        "AUTHORIZATION",
    ] {
        assert!(credential_variable(name));
    }
    assert!(!credential_variable("PATH"));
    let temp = tempfile::tempdir().unwrap();
    let request = fixture(temp.path());
    let request_path = temp.path().join("worker.json");
    atomic_json(&request_path, &request).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_clf3"))
        .args(["collection", "gui-worker"])
        .arg(&request_path)
        .env("XDG_CONFIG_HOME", temp.path().join("absent-settings"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let reader = BufReader::new(child.stdout.take().unwrap());
    let mut completed = false;
    for line in reader.lines() {
        let line = line.unwrap();
        assert!(line.len() < 64 * 1024);
        let event: WorkerEvent = serde_json::from_str(&line).unwrap();
        if matches!(event, WorkerEvent::Completed { .. }) {
            completed = true;
        }
    }
    assert!(child.wait().unwrap().success());
    assert!(completed);
    assert!(!temp.path().join("absent-settings").exists());

    let temp = tempfile::tempdir().unwrap();
    let request = fixture(temp.path());
    let path = temp.path().join("worker.json");
    atomic_json(&path, &request).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_clf3"))
        .args(["collection", "gui-worker"])
        .arg(path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(b"cancel\n").unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(String::from_utf8(output.stdout)
        .unwrap()
        .contains("cancelled"));
    assert!(!request.output.exists());
}

#[test]
fn committed_result_recovers_only_for_its_job_and_unchanged_files() {
    use clf3::collection::worker::{read_json, recover};
    let temp = tempfile::tempdir().unwrap();
    let request = fixture(temp.path());
    execute(&request, &Default::default(), &|_| {}).unwrap();
    let plan: CollectionPlan = read_json(&request.plan, 32 * 1024 * 1024).unwrap();
    assert!(recover(
        &request.output,
        "another-job",
        &plan,
        &Default::default(),
        &|_| {}
    )
    .is_err());
    let recovered = recover(
        &request.output,
        "gui-fixture-job",
        &plan,
        &Default::default(),
        &|_| {},
    )
    .unwrap();
    assert_eq!(recovered.verified_mod_files, 2);
    let payload = walkdir::WalkDir::new(request.output.join("mods"))
        .into_iter()
        .filter_map(Result::ok)
        .find(|e| e.file_name() == "required.txt")
        .unwrap()
        .into_path();
    let extra = payload.with_file_name("unexpected.txt");
    std::fs::write(&extra, b"unexpected payload").unwrap();
    assert!(recover(
        &request.output,
        "gui-fixture-job",
        &plan,
        &Default::default(),
        &|_| {}
    )
    .is_err());
    std::fs::remove_file(extra).unwrap();
    std::fs::write(&payload, b"damaged payload").unwrap();
    assert!(recover(
        &request.output,
        "gui-fixture-job",
        &plan,
        &Default::default(),
        &|_| {}
    )
    .is_err());
    assert_eq!(
        std::fs::read(&payload).unwrap(),
        b"damaged payload",
        "Recovery never overwrites changed files"
    );
}
