//! Local HTTP regressions for interrupted Wabbajack downloads (issue #184).
use clf3::downloaders::WabbajackCdnDownloader;
use clf3::hash::compute_bytes_hash;
use std::{
    collections::HashMap,
    io::{BufRead, BufReader, Cursor, Read, Write},
    net::TcpListener,
    path::Path,
    process::{Command, Stdio},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread,
    time::Duration,
};

#[derive(Clone, Copy)]
enum Mode {
    Good,
    RetryChecksum,
    BadChecksum,
    BadWholeHash,
    BadLayout,
    Slow,
}

struct Server {
    url: String,
    requests: Arc<Mutex<HashMap<String, usize>>>,
    stop: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
}

impl Server {
    fn new(payload: &[u8], mode: Mode) -> Self {
        let split = payload.len() / 2;
        let parts = [payload[..split].to_vec(), payload[split..].to_vec()];
        let mut definition = serde_json::json!({
            "Author":"fixture", "OriginalFileName":"Fixture.wabbajack", "MungedName":"fixture",
            "Hash":compute_bytes_hash(payload), "Size":payload.len(),
            "Parts":[
                {"Index":0,"Offset":0,"Size":parts[0].len(),"Hash":compute_bytes_hash(&parts[0])},
                {"Index":1,"Offset":split,"Size":parts[1].len(),"Hash":compute_bytes_hash(&parts[1])}
            ]
        });
        if matches!(mode, Mode::BadWholeHash) {
            definition["Hash"] = compute_bytes_hash(b"wrong whole hash").into();
        }
        if matches!(mode, Mode::BadLayout) {
            definition["Parts"][1]["Offset"] = 0.into();
        }
        let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        gzip.write_all(&serde_json::to_vec(&definition).unwrap())
            .unwrap();
        let definition = gzip.finish().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!(
            "http://{}/Fixture.wabbajack",
            listener.local_addr().unwrap()
        );
        listener.set_nonblocking(true).unwrap();
        let requests = Arc::new(Mutex::new(HashMap::new()));
        let seen = requests.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = stop.clone();
        let worker = thread::spawn(move || {
            while !stopping.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        stream
                            .set_read_timeout(Some(Duration::from_secs(3)))
                            .unwrap();
                        let mut input = Vec::new();
                        let mut byte = [0];
                        while !input.ends_with(b"\r\n\r\n") {
                            if stream.read(&mut byte).unwrap_or(0) == 0 {
                                break;
                            }
                            input.push(byte[0]);
                        }
                        let request = String::from_utf8_lossy(&input);
                        let path = request.split_whitespace().nth(1).unwrap_or("").to_owned();
                        let count = {
                            let mut seen = seen.lock().unwrap();
                            let n = seen.entry(path.clone()).or_insert(0);
                            *n += 1;
                            *n
                        };
                        if matches!(mode, Mode::Slow) && path.contains("/parts/") {
                            thread::sleep(Duration::from_millis(250));
                        }
                        let mut body = if path.ends_with("/definition.json.gz") {
                            definition.clone()
                        } else if path.ends_with("/parts/0") {
                            parts[0].clone()
                        } else if path.ends_with("/parts/1") {
                            parts[1].clone()
                        } else {
                            Vec::new()
                        };
                        if path.ends_with("/parts/0")
                            && (matches!(mode, Mode::BadChecksum)
                                || matches!(mode, Mode::RetryChecksum) && count == 1)
                        {
                            body[0] ^= 0xff;
                        }
                        let _ = write!(
                            stream,
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            body.len()
                        );
                        let _ = stream.write_all(&body);
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2))
                    }
                    Err(_) => break,
                }
            }
        });
        Self {
            url,
            requests,
            stop,
            worker: Some(worker),
        }
    }
    fn part_requests(&self) -> usize {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(p, _)| p.contains("/parts/"))
            .map(|(_, n)| n)
            .sum()
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.worker.take().unwrap().join().unwrap();
    }
}

fn archive(json: &[u8]) -> Vec<u8> {
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    writer
        .start_file(
            "modlist",
            zip::write::SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Stored),
        )
        .unwrap();
    writer.write_all(json).unwrap();
    writer.finish().unwrap().into_inner()
}
fn no_partial_files(root: &Path) {
    assert!(!std::fs::read_dir(root).unwrap().any(|e| e
        .unwrap()
        .file_name()
        .to_string_lossy()
        .ends_with(".part")));
}

#[tokio::test]
async fn verifies_cache_content_and_publishes_only_complete_files() {
    let temp = tempfile::tempdir().unwrap();
    let out = temp.path().join("Fixture.wabbajack");
    let bytes = archive(b"{}");
    let server = Server::new(&bytes, Mode::Good);
    let output = out.clone();
    WabbajackCdnDownloader::new()
        .unwrap()
        .download_with_progress(&server.url, &out, 0, move |_, _| {
            assert!(
                !output.exists(),
                "final filename exposed before verification"
            );
        })
        .await
        .unwrap();
    assert_eq!(std::fs::read(&out).unwrap(), bytes);
    let downloader = WabbajackCdnDownloader::new().unwrap();
    downloader
        .download(&server.url, &out, bytes.len() as u64)
        .await
        .unwrap();
    assert_eq!(
        server.part_requests(),
        2,
        "verified cache should not redownload parts"
    );
    let wrong = archive(b"[]");
    assert_eq!(wrong.len(), bytes.len());
    std::fs::write(&out, wrong).unwrap();
    downloader.download(&server.url, &out, 0).await.unwrap();
    assert_eq!(
        server.part_requests(),
        4,
        "a valid ZIP with the wrong hash must be replaced"
    );
    assert_eq!(std::fs::read(&out).unwrap(), bytes);
    no_partial_files(temp.path());
}

#[tokio::test]
async fn failed_chunk_keeps_previous_file_and_removes_partial() {
    let temp = tempfile::tempdir().unwrap();
    let out = temp.path().join("Fixture.wabbajack");
    std::fs::write(&out, b"existing file").unwrap();
    let server = Server::new(&archive(b"{}"), Mode::BadChecksum);
    let error = WabbajackCdnDownloader::new()
        .unwrap()
        .download(&server.url, &out, 0)
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("checksum mismatch"));
    assert_eq!(std::fs::read(&out).unwrap(), b"existing file");
    no_partial_files(temp.path());
}

#[tokio::test]
async fn retries_damaged_chunks_before_accepting_them() {
    let temp = tempfile::tempdir().unwrap();
    let out = temp.path().join("Fixture.wabbajack");
    let bytes = archive(b"{}");
    let server = Server::new(&bytes, Mode::RetryChecksum);
    WabbajackCdnDownloader::new()
        .unwrap()
        .download(&server.url, &out, 0)
        .await
        .unwrap();
    assert_eq!(std::fs::read(out).unwrap(), bytes);
    assert_eq!(server.part_requests(), 3);
}

#[tokio::test]
async fn rejects_inconsistent_whole_hash_and_part_layout() {
    for mode in [Mode::BadWholeHash, Mode::BadLayout] {
        let temp = tempfile::tempdir().unwrap();
        let out = temp.path().join("Fixture.wabbajack");
        let server = Server::new(&archive(b"{}"), mode);
        let error = WabbajackCdnDownloader::new()
            .unwrap()
            .download(&server.url, &out, 0)
            .await
            .unwrap_err();
        assert!(
            format!("{error:#}").contains(if matches!(mode, Mode::BadLayout) {
                "layout"
            } else {
                "checksum"
            })
        );
        if matches!(mode, Mode::BadLayout) {
            assert_eq!(server.part_requests(), 0);
        }
        assert!(!out.exists());
        no_partial_files(temp.path());
    }
}

#[tokio::test]
async fn matching_hash_does_not_make_a_broken_zip_valid() {
    let temp = tempfile::tempdir().unwrap();
    let out = temp.path().join("Fixture.wabbajack");
    let server = Server::new(b"not a ZIP archive", Mode::Good);
    let error = WabbajackCdnDownloader::new()
        .unwrap()
        .download(&server.url, &out, 0)
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("Invalid Wabbajack ZIP"));
    assert!(!out.exists());
    no_partial_files(temp.path());
}

#[test]
fn hosted_cli_repairs_preallocated_cache_instead_of_reusing_it() {
    let temp = tempfile::tempdir().unwrap();
    let cache = temp.path().join("cache/clf3/modlists");
    std::fs::create_dir_all(&cache).unwrap();
    let out = cache.join("Fixture.wabbajack");
    let bytes = archive(b"{}");
    std::fs::File::create(&out)
        .unwrap()
        .set_len(bytes.len() as u64)
        .unwrap();
    let server = Server::new(&bytes, Mode::Good);
    let mut child = Command::new(env!("CARGO_BIN_EXE_clf3"))
        .args(["install", &server.url])
        .arg(temp.path().join("downloads"))
        .arg(temp.path().join("install"))
        .args(["--jackify", "--hosted"])
        .current_dir(temp.path())
        .env("XDG_CACHE_HOME", temp.path().join("cache"))
        .env("XDG_CONFIG_HOME", temp.path().join("config"))
        .env("XDG_DATA_HOME", temp.path().join("data"))
        .env("XDG_STATE_HOME", temp.path().join("state"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut output = BufReader::new(child.stdout.take().unwrap());
    let mut line = String::new();
    output.read_line(&mut line).unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&line).unwrap()["type"],
        "hello"
    );
    let mut input = child.stdin.take().unwrap();
    input
        .write_all(b"{\"type\":\"hello_ack\",\"protocol_version\":1}\n")
        .unwrap();
    // '{}' intentionally fails semantic parsing after the repaired ZIP is opened.
    let result = child.wait_with_output().unwrap();
    let errors = String::from_utf8_lossy(&result.stderr);
    assert!(!result.status.success());
    assert!(errors.contains("Verified .wabbajack file"), "{errors}");
    assert!(errors.contains("Failed to parse modlist JSON"), "{errors}");
    assert!(
        !errors.contains("Error: Failed to read as ZIP archive"),
        "{errors}"
    );
    assert_eq!(std::fs::read(&out).unwrap(), bytes);
    assert_eq!(server.part_requests(), 2);
    no_partial_files(&cache);
}

#[tokio::test]
async fn cancellation_never_publishes_the_partial_file() {
    let temp = tempfile::tempdir().unwrap();
    let out = temp.path().join("Fixture.wabbajack");
    let server = Server::new(&archive(b"{}"), Mode::Slow);
    let url = server.url.clone();
    let target = out.clone();
    let task = tokio::spawn(async move {
        WabbajackCdnDownloader::new()
            .unwrap()
            .download(&url, &target, 0)
            .await
    });
    tokio::time::timeout(Duration::from_secs(3), async {
        while server.part_requests() == 0 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    assert!(!out.exists());
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(!out.exists());
    no_partial_files(temp.path());
}

#[tokio::test]
async fn gui_browser_replaces_same_size_incomplete_cache() {
    use clf3::modlist::browser::{DownloadMetadata, ModlistBrowser, ModlistLinks, ModlistMetadata};
    let temp = tempfile::tempdir().unwrap();
    let bytes = archive(b"{}");
    let server = Server::new(&bytes, Mode::Good);
    let out = temp.path().join("fixture.wabbajack");
    std::fs::File::create(&out)
        .unwrap()
        .set_len(bytes.len() as u64)
        .unwrap();
    let metadata = ModlistMetadata {
        title: "Fixture".into(),
        machine_name: "fixture".into(),
        links: Some(ModlistLinks {
            download: server.url.clone(),
            ..Default::default()
        }),
        download_metadata: Some(DownloadMetadata {
            size: bytes.len() as u64,
            hash: compute_bytes_hash(&bytes),
            ..Default::default()
        }),
        ..Default::default()
    };
    assert_eq!(
        ModlistBrowser::new()
            .unwrap()
            .download_modlist(&metadata, temp.path())
            .await
            .unwrap(),
        out
    );
    assert_eq!(std::fs::read(out).unwrap(), bytes);
    assert_eq!(server.part_requests(), 2);
}
