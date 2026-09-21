//! MEGA v2 transfers: bounded parallel ranges, including CloudRAID reconstruction.

use aes::Aes128;
use anyhow::{bail, ensure, Context, Result};
use ctr::cipher::{KeyIvInit, StreamCipher};
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use futures::stream::{self, FuturesUnordered};
use futures::{StreamExt, TryStreamExt};
use reqwest_012::{Client, StatusCode, Url};
use serde_json::{json, Value};
use std::pin::Pin;
use std::time::Duration;
use tokio_util::compat::TokioAsyncReadCompatExt;
use tracing::info;

const SECTOR: usize = 16;
const DATA_PARTS: usize = 5;
const PARTS: usize = 6;
const LINE: usize = SECTOR * DATA_PARTS;
const PART_BLOCK: u64 = 1024 * 1024;
const RAID_BLOCK: u64 = PART_BLOCK * DATA_PARTS as u64;
// Multiple ranges per shard keep a slow storage connection from limiting the
// whole file. Eight 5 MiB blocks cap buffered transfer data below 64 MiB.
const RAID_CONCURRENCY: usize = 8;
const DIRECT_BLOCK: u64 = 4 * 1024 * 1024;
const ATTEMPTS: u32 = 3;

// Deliberately no Debug: these URLs contain temporary download credentials.
enum TransferUrls {
    Direct(Url),
    Raid([Url; PARTS]),
}

fn download_request(handle: &str, download_id: &str) -> Value {
    let mut request = json!({"a": "g", "g": 1, "v": 2, "ssl": 2});
    request[if handle == download_id { "p" } else { "n" }] = json!(handle);
    request
}

fn parse_urls(value: &Value, expected_size: u64) -> Result<TransferUrls> {
    if let Some(code) = value
        .as_i64()
        .or_else(|| value.get("e").and_then(Value::as_i64))
    {
        if code != 0 {
            bail!("MEGA download API returned error {code}");
        }
    }
    ensure!(
        value["s"].as_u64() == Some(expected_size),
        "MEGA file size changed"
    );
    let parse = |value: &Value| -> Result<Url> {
        let url = Url::parse(value.as_str().context("Invalid MEGA transfer URL")?)
            .context("Invalid MEGA transfer URL")?;
        ensure!(
            matches!(url.scheme(), "http" | "https"),
            "Invalid MEGA transfer protocol"
        );
        Ok(url)
    };
    match &value["g"] {
        Value::String(_) => Ok(TransferUrls::Direct(parse(&value["g"])?)),
        Value::Array(urls) if urls.len() == PARTS => {
            let urls = urls.iter().map(parse).collect::<Result<Vec<_>>>()?;
            Ok(TransferUrls::Raid(
                urls.try_into()
                    .map_err(|_| anyhow::anyhow!("Invalid MEGA RAID parts"))?,
            ))
        }
        _ => bail!("Invalid MEGA v2 download response"),
    }
}

async fn resolve(client: &Client, node: &mega::Node) -> Result<TransferUrls> {
    let download_id = node
        .download_id()
        .context("MEGA file is not a public share")?;
    let request = download_request(node.handle(), download_id);
    for attempt in 0..ATTEMPTS {
        if attempt > 0 {
            tokio::time::sleep(Duration::from_secs(1 << (attempt - 1))).await;
        }
        let response = client
            .post("https://g.api.mega.co.nz/cs")
            .query(&[
                ("id", uuid::Uuid::new_v4().as_u128().to_string()),
                ("n", download_id.to_owned()),
            ])
            .json(&[&request])
            .timeout(Duration::from_secs(30))
            .send()
            .await;
        let response = match response {
            Ok(response) => response,
            Err(error) if attempt + 1 < ATTEMPTS && (error.is_timeout() || error.is_connect()) => {
                continue
            }
            Err(error) => return Err(error.without_url().into()),
        };
        if retryable(response.status()) && attempt + 1 < ATTEMPTS {
            continue;
        }
        let value: Value = response
            .error_for_status()
            .map_err(|e| e.without_url())?
            .json()
            .await
            .map_err(|e| e.without_url())?;
        let item = value
            .as_array()
            .and_then(|v| (v.len() == 1).then(|| &v[0]))
            .unwrap_or(&value);
        let code = item
            .as_i64()
            .or_else(|| item.get("e").and_then(Value::as_i64));
        if matches!(code, Some(-3 | -4 | -18)) && attempt + 1 < ATTEMPTS {
            continue;
        }
        return parse_urls(item, node.size());
    }
    bail!("MEGA download API retries exhausted")
}

fn retryable(status: StatusCode) -> bool {
    matches!(status.as_u16(), 408 | 429 | 500 | 502 | 503 | 504)
}

#[derive(Debug, thiserror::Error)]
#[error("MEGA transfer quota exceeded (HTTP 509)")]
struct TransferQuotaExceeded;

async fn fetch_range_once(client: &Client, base: &Url, start: u64, len: usize) -> Result<Vec<u8>> {
    if len == 0 {
        return Ok(Vec::new());
    }
    // MEGA uses an inclusive byte range in the URL path, not a Range header.
    let url = format!(
        "{}/{start}-{}",
        base.as_str().trim_end_matches('/'),
        start + len as u64 - 1
    );
    let response = client
        .get(url)
        .timeout(Duration::from_secs(60))
        .send()
        .await
        .map_err(|e| e.without_url())?;
    if response.status().as_u16() == 509 {
        return Err(TransferQuotaExceeded.into());
    }
    let response = response.error_for_status().map_err(|e| e.without_url())?;
    if let Some(length) = response.content_length() {
        ensure!(
            length == len as u64,
            "MEGA range has incorrect length: expected {len}, got {length}"
        );
    }
    let mut bytes = Vec::with_capacity(len);
    let mut body = response.bytes_stream();
    while let Some(piece) = body.next().await {
        let piece = piece.map_err(|e| e.without_url())?;
        ensure!(
            piece.len() <= len - bytes.len(),
            "MEGA range exceeded requested length"
        );
        bytes.extend_from_slice(&piece);
    }
    ensure!(
        bytes.len() == len,
        "MEGA range was truncated: expected {len}, got {}",
        bytes.len()
    );
    Ok(bytes)
}

async fn fetch_range(client: &Client, base: &Url, start: u64, len: usize) -> Result<Vec<u8>> {
    for attempt in 0..ATTEMPTS {
        match fetch_range_once(client, base, start, len).await {
            Ok(bytes) => return Ok(bytes),
            Err(error) => {
                let permanent = error.is::<TransferQuotaExceeded>()
                    || error
                        .downcast_ref::<reqwest_012::Error>()
                        .and_then(|e| e.status())
                        .is_some_and(|status| !retryable(status));
                if permanent || attempt + 1 == ATTEMPTS {
                    return Err(error);
                }
                tokio::time::sleep(Duration::from_secs(1 << attempt)).await;
            }
        }
    }
    unreachable!()
}

// CloudRAID stores five consecutive 16-byte data sectors per line. Part 0 is
// their XOR parity; parts 1..5 hold the data. Absent tail bytes are zero for XOR.
// Protocol reference: meganz/webclient, js/transfers/cloudraid.js.
fn part_size(part: usize, size: u64) -> u64 {
    let full_lines = size / LINE as u64;
    let tail = size % LINE as u64;
    full_lines * SECTOR as u64
        + tail
            .saturating_sub(part.saturating_sub(1) as u64 * SECTOR as u64)
            .min(SECTOR as u64)
}

fn reconstruct(parts: &[Option<Vec<u8>>; PARTS], len: usize) -> Result<Vec<u8>> {
    ensure!(
        parts.iter().filter(|p| p.is_none()).count() <= 1,
        "MEGA CloudRAID needs five parts"
    );
    for (part, bytes) in parts.iter().enumerate() {
        if let Some(bytes) = bytes {
            ensure!(
                bytes.len() as u64 == part_size(part, len as u64),
                "Incorrect MEGA CloudRAID part length"
            );
        }
    }
    let mut output = vec![0; len];
    for (sector, target) in output.chunks_mut(SECTOR).enumerate() {
        let part = sector % DATA_PARTS + 1;
        let offset = sector / DATA_PARTS * SECTOR;
        if let Some(bytes) = &parts[part] {
            target.copy_from_slice(&bytes[offset..offset + target.len()]);
        } else {
            for bytes in parts.iter().flatten() {
                for (index, byte) in target.iter_mut().enumerate() {
                    *byte ^= bytes.get(offset + index).copied().unwrap_or(0);
                }
            }
        }
    }
    Ok(output)
}

async fn raid_block(
    client: &Client,
    urls: &[Url; PARTS],
    start: u64,
    len: usize,
) -> Result<Vec<u8>> {
    ensure!(start % LINE as u64 == 0, "Unaligned MEGA CloudRAID range");
    let mut pending = FuturesUnordered::new();
    for (part, url) in urls.iter().enumerate() {
        pending.push(async move {
            (
                part,
                fetch_range(
                    client,
                    url,
                    start / DATA_PARTS as u64,
                    part_size(part, len as u64) as usize,
                )
                .await,
            )
        });
    }
    let mut parts = std::array::from_fn(|_| None);
    let mut received = 0;
    let mut failure = None;
    while let Some((part, result)) = pending.next().await {
        match result {
            Ok(bytes) => {
                parts[part] = Some(bytes);
                received += 1;
                if received == DATA_PARTS {
                    // Dropping the remaining future cancels the slowest request.
                    // Each block can tolerate a different unavailable part.
                    return reconstruct(&parts, len);
                }
            }
            Err(error) => {
                if error.is::<TransferQuotaExceeded>() {
                    return Err(error);
                }
                failure = Some(error);
            }
        }
        if received + pending.len() < DATA_PARTS {
            break;
        }
    }
    Err(failure.unwrap_or_else(|| anyhow::anyhow!("Insufficient MEGA CloudRAID parts")))
}

fn encrypted_reader(
    client: Client,
    urls: TransferUrls,
    size: u64,
) -> Pin<Box<dyn AsyncRead + Send>> {
    let (block_size, concurrency) = match &urls {
        TransferUrls::Direct(_) => (DIRECT_BLOCK, 4),
        TransferUrls::Raid(_) => (RAID_BLOCK, RAID_CONCURRENCY),
    };
    let urls = std::sync::Arc::new(urls);
    // Buffered preserves file order while fetching ahead. Memory and requests
    // stay bounded regardless of file size; no detached tasks survive a cancel.
    let blocks = stream::iter((0..size).step_by(block_size as usize))
        .map(move |start| {
            let client = client.clone();
            let urls = urls.clone();
            async move {
                let len = (size - start).min(block_size) as usize;
                match urls.as_ref() {
                    TransferUrls::Direct(url) => fetch_range(&client, url, start, len).await,
                    TransferUrls::Raid(urls) => raid_block(&client, urls, start, len).await,
                }
                .map_err(std::io::Error::other)
            }
        })
        .buffered(concurrency);
    Box::pin(blocks.into_async_read())
}

async fn decrypt_and_verify<W: AsyncWrite + Unpin>(
    mut reader: Pin<Box<dyn AsyncRead + Send>>,
    writer: &mut W,
    size: u64,
    key: &[u8; 16],
    iv: &[u8; 8],
    expected_mac: &[u8; 8],
) -> Result<()> {
    let mut counter = [0; 16];
    counter[..8].copy_from_slice(iv);
    let mut cipher = ctr::Ctr128BE::<Aes128>::new(key.into(), (&counter).into());
    // A bounded pipe feeds the crate's existing integrity checker concurrently.
    let (mac_reader, mac_writer) = tokio::io::duplex(256 * 1024);
    let transfer = async {
        let mut mac_writer = mac_writer.compat();
        let mut buffer = vec![0; 256 * 1024];
        let mut written = 0;
        loop {
            let count = reader.read(&mut buffer).await?;
            if count == 0 {
                break;
            }
            written += count as u64;
            ensure!(written <= size, "MEGA download exceeded expected size");
            cipher.apply_keystream(&mut buffer[..count]);
            writer.write_all(&buffer[..count]).await?;
            mac_writer.write_all(&buffer[..count]).await?;
        }
        ensure!(
            written == size,
            "MEGA download incomplete: got {written} bytes, expected {size}"
        );
        Ok::<_, anyhow::Error>(())
    };
    let verify = async {
        mega::compute_condensed_mac(mac_reader.compat(), size, key, iv)
            .await
            .map_err(anyhow::Error::from)
    };
    let (_, mac) = futures::try_join!(transfer, verify)?;
    ensure!(
        &mac == expected_mac,
        "MEGA file integrity check failed (condensed MAC mismatch)"
    );
    Ok(())
}

pub(super) async fn download_node<W: AsyncWrite + Unpin>(
    client: &Client,
    node: &mega::Node,
    writer: &mut W,
) -> Result<()> {
    let iv = node.aes_iv().context("MEGA file is missing its IV")?;
    let mac = node
        .condensed_mac()
        .context("MEGA file is missing its integrity tag")?;
    let urls = resolve(client, node).await?;
    match &urls {
        TransferUrls::Raid(_) => info!("Mega v2 CloudRAID: parallel transfer with parity recovery"),
        TransferUrls::Direct(_) => info!("Mega v2: parallel direct transfer"),
    }
    let reader = encrypted_reader(client.clone(), urls, node.size());
    decrypt_and_verify(reader, writer, node.size(), node.aes_key(), iv, mac).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode(data: &[u8]) -> [Vec<u8>; PARTS] {
        let mut parts: [Vec<u8>; PARTS] = std::array::from_fn(|_| Vec::new());
        for line in data.chunks(LINE) {
            let mut parity = [0; SECTOR];
            for (part, sector) in line.chunks(SECTOR).enumerate() {
                parts[part + 1].extend_from_slice(sector);
                for (i, byte) in sector.iter().enumerate() {
                    parity[i] ^= byte;
                }
            }
            parts[0].extend_from_slice(&parity[..line.len().min(SECTOR)]);
        }
        parts
    }

    #[test]
    fn recovers_every_missing_part_and_tail_length() {
        for size in 0..=4 * LINE {
            let data: Vec<_> = (0..size).map(|i| (i * 73 + i / 11) as u8).collect();
            let encoded = encode(&data);
            for missing in 0..PARTS {
                let mut parts = encoded.clone().map(Some);
                parts[missing] = None;
                assert_eq!(
                    reconstruct(&parts, size).unwrap(),
                    data,
                    "size {size}, missing {missing}"
                );
            }
        }
        let mut parts = encode(&[42; 101]).map(Some);
        parts[1] = None;
        parts[5] = None;
        assert!(reconstruct(&parts, 101).is_err());
        parts[5] = Some(vec![0]);
        assert!(reconstruct(&parts, 101).is_err());
    }

    #[test]
    fn public_file_and_folder_requests_and_response_shapes() {
        let file = download_request("file", "file");
        assert_eq!(file, json!({"a":"g","g":1,"v":2,"ssl":2,"p":"file"}));
        let folder = download_request("file", "folder");
        assert_eq!(folder["n"], "file");
        assert!(folder.get("p").is_none());
        assert!(matches!(
            parse_urls(&json!({"s":4,"g":"https://example.com/file"}), 4).unwrap(),
            TransferUrls::Direct(_)
        ));
        assert!(matches!(
            parse_urls(&json!({"s":4,"g":vec!["https://example.com/part";6]}), 4).unwrap(),
            TransferUrls::Raid(_)
        ));
        for invalid in [
            json!(-17),
            json!({"e":-17}),
            json!({"s":3,"g":"https://example.com/file"}),
            json!({"s":4,"g":[]}),
            json!({"s":4,"g":vec!["https://example.com/part";5]}),
        ] {
            assert!(parse_urls(&invalid, 4).is_err());
        }
    }

    #[tokio::test]
    async fn decrypts_and_rejects_corruption_or_truncation() {
        let data: Vec<_> = (0..2 * 1024 * 1024 + 31)
            .map(|i| (i * 31 + i / 27) as u8)
            .collect();
        let key = [0x17; 16];
        let iv = [0x39; 8];
        let mac = mega::compute_condensed_mac(
            futures::io::Cursor::new(&data),
            data.len() as u64,
            &key,
            &iv,
        )
        .await
        .unwrap();
        let mut counter = [0; 16];
        counter[..8].copy_from_slice(&iv);
        let mut encrypted = data.clone();
        ctr::Ctr128BE::<Aes128>::new((&key).into(), (&counter).into())
            .apply_keystream(&mut encrypted);
        for case in 0..3 {
            let mut bytes = encrypted.clone();
            if case == 1 {
                bytes[1025] ^= 1;
            }
            if case == 2 {
                bytes.pop();
            }
            let reader = Box::pin(futures::io::Cursor::new(bytes));
            let mut output = Vec::new();
            let result =
                decrypt_and_verify(reader, &mut output, data.len() as u64, &key, &iv, &mac).await;
            if case == 0 {
                result.unwrap();
                assert_eq!(output, data);
            } else {
                assert!(result.is_err());
            }
        }
    }

    // HTTP fixture: validates inclusive URL ranges, and can fail or indefinitely
    // stall one shard to prove parity recovery does not wait for all six.
    async fn server(
        data: Vec<Vec<u8>>,
        failed: Vec<usize>,
        stalled: Option<usize>,
    ) -> (Vec<Url>, tokio::task::JoinHandle<()>) {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let urls = (0..data.len())
            .map(|i| Url::parse(&format!("http://{addr}/{i}")).unwrap())
            .collect();
        let task = tokio::spawn(async move {
            let mut handlers = tokio::task::JoinSet::new();
            let data = std::sync::Arc::new(data);
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let data = data.clone();
                let failed = failed.clone();
                handlers.spawn(async move {
                    let mut request = Vec::new();
                    loop {
                        let mut byte = [0];
                        if socket.read_exact(&mut byte).await.is_err() { return; }
                        request.push(byte[0]);
                        if request.ends_with(b"\r\n\r\n") { break; }
                    }
                    let request = String::from_utf8(request).unwrap();
                    let path: Vec<_> = request.split_whitespace().nth(1).unwrap().trim_start_matches('/').split('/').collect();
                    let part: usize = path[0].parse().unwrap();
                    if stalled == Some(part) { std::future::pending::<()>().await; }
                    if failed.contains(&part) {
                        let _ = socket.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await;
                        return;
                    }
                    let (start, end) = path[1].split_once('-').unwrap();
                    let start: usize = start.parse().unwrap();
                    let end: usize = end.parse().unwrap();
                    let body = &data[part][start..=end];
                    let header = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len());
                    let _ = socket.write_all(header.as_bytes()).await;
                    let _ = socket.write_all(body).await;
                });
                while handlers.try_join_next().is_some() {}
            }
        });
        (urls, task)
    }

    #[tokio::test]
    async fn parallel_ranges_preserve_order_and_recover_stalled_or_failed_parts() {
        let data: Vec<_> = (0..RAID_BLOCK as usize + 139)
            .map(|i| (i * 17 + i / 113) as u8)
            .collect();
        let client = Client::new();
        for (failed, stalled) in [(vec![], Some(2)), (vec![4], None), (vec![1, 3], None)] {
            let (urls, task) = server(encode(&data).to_vec(), failed.clone(), stalled).await;
            let urls = TransferUrls::Raid(urls.try_into().unwrap());
            let mut reader = encrypted_reader(client.clone(), urls, data.len() as u64);
            let mut output = Vec::new();
            let result =
                tokio::time::timeout(Duration::from_secs(5), reader.read_to_end(&mut output)).await;
            task.abort();
            let result = result.expect("Waited for a stalled shard");
            if failed.len() == 2 {
                assert!(result.is_err());
            } else {
                result.unwrap();
                assert_eq!(output, data);
            }
        }
        let (urls, task) = server(vec![data.clone()], vec![], None).await;
        let mut reader = encrypted_reader(
            client,
            TransferUrls::Direct(urls[0].clone()),
            data.len() as u64,
        );
        let mut output = Vec::new();
        reader.read_to_end(&mut output).await.unwrap();
        task.abort();
        assert_eq!(output, data);
    }
}
