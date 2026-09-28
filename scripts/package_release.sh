#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

cargo_version="$(python3 -c 'import tomllib; print(tomllib.load(open("Cargo.toml", "rb"))["package"]["version"])')"
if [[ "${GITHUB_REF_TYPE:-}" == "tag" ]]; then
    tag_version="${GITHUB_REF_NAME#v}"
    if [[ "$tag_version" != "$cargo_version" ]]; then
        echo "Tag ${GITHUB_REF_NAME} does not match Cargo.toml version ${cargo_version}" >&2
        exit 1
    fi
fi

if [[ ! "$cargo_version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "Cargo package version is not a release version: ${cargo_version}" >&2
    exit 1
fi

binary_path="${CLF3_BINARY_PATH:-target/release/clf3}"
seven_zip_path="${CLF3_7ZIP_PATH:-release/7zz}"
if [[ ! -x "$binary_path" ]]; then
    echo "Missing release executable: ${binary_path}" >&2
    exit 1
fi
if [[ ! -x "$seven_zip_path" ]]; then
    echo "Missing 7-Zip executable: ${seven_zip_path}" >&2
    exit 1
fi

dist_dir="${CLF3_DIST_DIR:-dist}"
stage_dir="$(mktemp -d)"
trap 'rm -rf "$stage_dir"' EXIT
mkdir -p "$dist_dir"
cp "$binary_path" "$stage_dir/clf3"
cp "$seven_zip_path" "$stage_dir/7zz"
chmod +x "$stage_dir/clf3" "$stage_dir/7zz"

archive_base="clf3-linux-x86_64-${cargo_version}"
tar_archive="${archive_base}.tar.gz"
zip_archive="${archive_base}.zip"
compat_zip="clf3-linux-x64.zip"
tar --sort=name --mtime='@0' --owner=0 --group=0 --numeric-owner \
    -C "$stage_dir" -cf - clf3 7zz | gzip -n > "${dist_dir}/${tar_archive}"
python3 - "$stage_dir" "${dist_dir}/${zip_archive}" "${dist_dir}/${compat_zip}" <<'PY'
import sys
import zipfile

source_dir, *archive_paths = sys.argv[1:]
for archive_path in archive_paths:
    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name in ("clf3", "7zz"):
            archive.write(f"{source_dir}/{name}", arcname=name)
PY
(cd "$dist_dir" && sha256sum "$tar_archive" "$zip_archive" "$compat_zip" > SHA256SUMS)

printf 'Packaged %s, %s, and %s with SHA256SUMS in %s/\n' \
    "$tar_archive" "$zip_archive" "$compat_zip" "$dist_dir"
