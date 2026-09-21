#!/usr/bin/env python3
"""Install a selective Wabbajack overlay into an existing MO2 instance.

The shim preserves Wabbajack archive hashes and file mappings. It stages away
from the live instance, refuses to merge while MO2 is running, and backs up the
active profile before changing it.
"""

from __future__ import annotations

import argparse
import configparser
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


PLUGIN_SUFFIXES = {".esp", ".esm", ".esl"}


def parts(path: str) -> list[str]:
    return path.replace("/", "\\").split("\\")


def mod_folder(path: str) -> str | None:
    path_parts = parts(path)
    if len(path_parts) >= 2 and path_parts[0].casefold() == "mods":
        return path_parts[1]
    return None


def human_size(size: int) -> str:
    value = float(size)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024.0 or unit == "TiB":
            return f"{value:.1f} {unit}"
        value /= 1024.0
    raise AssertionError("unreachable")


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


@dataclass
class SourceManifest:
    data: dict[str, Any]
    archive_path: Path | None

    @classmethod
    def open(cls, path: Path) -> "SourceManifest":
        if zipfile.is_zipfile(path):
            with zipfile.ZipFile(path) as archive, archive.open("modlist") as manifest:
                return cls(json.load(manifest), path)
        return cls(load_json(path), None)

    def read_embedded(self, name: str) -> bytes:
        if self.archive_path is None:
            raise RuntimeError(
                f"embedded Wabbajack data {name!r} is unavailable in a plain JSON manifest"
            )
        with zipfile.ZipFile(self.archive_path) as archive:
            return archive.read(name)


@dataclass
class OverlayPlan:
    manifest: dict[str, Any]
    groups: dict[str, list[str]]
    embedded_ids: set[str]
    omitted_directives: list[str]

    @property
    def archive_bytes(self) -> int:
        return sum(int(archive.get("Size", 0)) for archive in self.manifest["Archives"])


def embedded_ids(value: Any) -> set[str]:
    found: set[str] = set()
    if isinstance(value, dict):
        for key, child in value.items():
            if key in {"SourceDataID", "PatchID"} and child:
                found.add(str(child))
            else:
                found.update(embedded_ids(child))
    elif isinstance(value, list):
        for child in value:
            found.update(embedded_ids(child))
    return found


def archive_hashes(value: Any) -> set[str]:
    found: set[str] = set()
    if isinstance(value, dict):
        for key, child in value.items():
            if key == "ArchiveHashPath" and isinstance(child, list) and child:
                found.add(str(child[0]))
            else:
                found.update(archive_hashes(child))
    elif isinstance(value, list):
        for child in value:
            found.update(archive_hashes(child))
    return found


def casefold_lookup(values: Iterable[str]) -> dict[str, str]:
    return {value.casefold(): value for value in values}


def source_mod_order(source: SourceManifest) -> list[str]:
    if source.archive_path is None:
        return []
    for item in source.data.get("Directives", []):
        target = item.get("To", "")
        source_id = item.get("SourceDataID")
        if source_id and re.search(r"^profiles[\\/].*[\\/]modlist\.txt$", target, re.I):
            content = source.read_embedded(str(source_id)).decode("utf-8-sig", errors="replace")
            return [
                line[1:].strip()
                for line in content.splitlines()
                if len(line) > 1 and line[0] in "+-" and not line[1:].endswith("_separator")
            ]
    return []


def sort_groups_by_source(groups: dict[str, list[str]], order: list[str]) -> None:
    rank = {name.casefold(): index for index, name in enumerate(order)}
    for folders in groups.values():
        folders.sort(key=lambda name: (rank.get(name.casefold(), 10**9), name.casefold()))


def build_plan(source: SourceManifest, recipe: dict[str, Any]) -> OverlayPlan:
    directives = source.data.get("Directives", [])
    present = {folder for item in directives if (folder := mod_folder(item.get("To", "")))}
    lookup = casefold_lookup(present)
    groups: dict[str, list[str]] = defaultdict(list)
    folder_group: dict[str, str] = {}

    for group in recipe.get("groups", []):
        separator = group["separator"]
        for requested in group.get("folders", []):
            actual = lookup.get(requested.casefold())
            if actual is None:
                print(f"warning: recipe folder is absent: {requested}", file=sys.stderr)
                continue
            folder_group.setdefault(actual.casefold(), separator)
            if actual not in groups[separator]:
                groups[separator].append(actual)

    outfit = recipe["outfits"]
    outfit_separator = outfit["separator"]
    patterns = [re.compile(pattern, re.IGNORECASE) for pattern in outfit["patterns"]]
    slider_set_folders = {
        mod_folder(item.get("To", ""))
        for item in directives
        if re.search(
            r"calientetools[\\/]bodyslide[\\/]slidersets[\\/].*\.osp$",
            item.get("To", ""),
            re.IGNORECASE,
        )
    }
    for folder in sorted(present, key=str.casefold):
        if folder.casefold() not in folder_group and folder in slider_set_folders:
            if any(pattern.search(folder) for pattern in patterns):
                folder_group[folder.casefold()] = outfit_separator
                groups[outfit_separator].append(folder)
    for requested in outfit.get("extra_folders", []):
        actual = lookup.get(requested.casefold())
        if actual is not None and actual.casefold() not in folder_group:
            folder_group[actual.casefold()] = outfit_separator
            groups[outfit_separator].append(actual)

    preset = recipe["presets"]
    preset_separator = preset["separator"]
    preset_re = re.compile(
        r"calientetools[\\/]bodyslide[\\/]sliderpresets[\\/].*\.xml$",
        re.IGNORECASE,
    )
    excluded = {name.casefold() for name in preset.get("exclude_folders", [])}
    preset_paths: set[str] = set()
    for item in directives:
        target = item.get("To", "")
        folder = mod_folder(target)
        if not folder or not preset_re.search(target):
            continue
        if folder.casefold() in excluded or folder.casefold() in folder_group:
            continue
        preset_paths.add(target.casefold())
        folder_group[folder.casefold()] = preset_separator
        if folder not in groups[preset_separator]:
            groups[preset_separator].append(folder)

    selected: list[dict[str, Any]] = []
    omitted: list[str] = []
    needed_embedded: set[str] = set()
    needed_archives: set[str] = set()
    supported = {
        "FromArchive",
        "PatchedFromArchive",
        "InlineFile",
        "RemappedInlineFile",
        "TransformedTexture",
        "CreateBSA",
    }
    for item in directives:
        target = item.get("To", "")
        folder = mod_folder(target)
        if folder is None:
            continue
        group = folder_group.get(folder.casefold())
        if group is None or (group == preset_separator and target.casefold() not in preset_paths):
            continue
        ids = embedded_ids(item)
        if ids and source.archive_path is None:
            # A plain extracted manifest is sufficient for planning, but not
            # for building. Record every unavailable inline/patch directive;
            # main() rejects build/stage/install with a plain source.
            omitted.append(target)
            continue
        kind = item.get("$type", "").split(",", 1)[0]
        if kind not in supported:
            omitted.append(target)
            continue
        selected.append(item)
        needed_embedded.update(ids)
        needed_archives.update(archive_hashes(item))

    state_overrides = recipe.get("archive_state_overrides", {})
    archives: list[dict[str, Any]] = []
    for item in source.data.get("Archives", []):
        if item.get("Hash") not in needed_archives:
            continue
        copied = dict(item)
        override = state_overrides.get(str(item.get("Name", "")))
        if override is not None:
            copied["State"] = override
        archives.append(copied)
    missing_hashes = needed_archives - {item.get("Hash") for item in archives}
    if missing_hashes:
        raise RuntimeError(f"manifest lacks {len(missing_hashes)} referenced archives")
    if source.archive_path is not None:
        with zipfile.ZipFile(source.archive_path) as archive:
            missing_embedded = needed_embedded - set(archive.namelist())
        if missing_embedded:
            raise RuntimeError(f"source lacks {len(missing_embedded)} embedded payloads")

    overlay = {
        key: value
        for key, value in source.data.items()
        if key not in {"Archives", "Directives", "Name", "Description"}
    }
    overlay.update(
        Name=recipe.get("name", "Selective Wabbajack Overlay"),
        Description=recipe.get("description", "Selective overlay generated by CLF3"),
        Archives=archives,
        Directives=selected,
    )
    sort_groups_by_source(groups, source_mod_order(source))
    return OverlayPlan(overlay, dict(groups), needed_embedded, omitted)


def write_overlay(source: SourceManifest, plan: OverlayPlan, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(output, "w", allowZip64=True) as archive:
        archive.writestr(
            "modlist",
            json.dumps(plan.manifest, ensure_ascii=False, separators=(",", ":")),
            compress_type=zipfile.ZIP_DEFLATED,
            compresslevel=6,
        )
        for item_id in sorted(plan.embedded_ids):
            archive.writestr(item_id, source.read_embedded(item_id), zipfile.ZIP_STORED)


def fluorine_running() -> bool:
    for child in Path("/proc").iterdir():
        if not child.name.isdigit():
            continue
        try:
            command = (child / "cmdline").read_bytes().replace(b"\0", b" ").decode(errors="ignore")
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            continue
        if "ModOrganizer-core" in command or "ModOrganizer.exe" in command:
            return True
    return False


def saved_credentials() -> tuple[str, str, str]:
    settings_path = Path.home() / ".config" / "clf3" / "settings.json"
    settings = load_json(settings_path)
    key = str(settings.get("nexus_api_key", "")).strip()
    if not key:
        raise RuntimeError(f"no Nexus API key is configured in {settings_path}")
    return (
        key,
        str(settings.get("loverslab_email", "")).strip(),
        str(settings.get("loverslab_password", "")),
    )


def find_profile(target: Path, requested: str | None) -> Path:
    if requested:
        profile = target / "profiles" / requested
    else:
        ini = configparser.ConfigParser(interpolation=None, strict=False)
        ini.optionxform = str
        ini.read(target / "ModOrganizer.ini", encoding="utf-8")
        raw = ini.get("General", "selected_profile", fallback="Default")
        match = re.fullmatch(r"@ByteArray\((.*)\)", raw)
        profile = target / "profiles" / (match.group(1) if match else raw)
    if not profile.is_dir():
        raise RuntimeError(f"MO2 profile does not exist: {profile}")
    return profile


def stage_overlay(args: argparse.Namespace, overlay_path: Path, stage: Path) -> None:
    # CLF3 is resumable: retain a partial stage and verified archives if an
    # upstream host fails or a genuinely manual archive must be supplied.
    stage.mkdir(parents=True, exist_ok=True)
    downloads = args.downloads or (args.target / "downloads")
    downloads.mkdir(parents=True, exist_ok=True)
    report_path = stage / ".clf3-overlay-report.json"
    with tempfile.TemporaryDirectory(prefix="clf3-overlay-config-") as config_home:
        env = os.environ.copy()
        env["XDG_CONFIG_HOME"] = config_home
        nexus_key, ll_email, ll_password = saved_credentials()
        env["NEXUS_API_KEY"] = nexus_key
        if ll_email and ll_password:
            env["LOVERSLAB_EMAIL"] = ll_email
            env["LOVERSLAB_PASSWORD"] = ll_password
        command = [
            str(args.clf3),
            "install",
            str(overlay_path),
            str(downloads),
            str(stage),
            "--game",
            str(args.game or (args.target / "Game Root")),
            "--concurrent",
            str(args.concurrent),
            "--install-workers",
            str(args.install_workers),
            "--sevenzip-workers",
            str(args.sevenzip_workers),
            "--report-json",
            str(report_path),
        ]
        subprocess.run(command, check=True, env=env)
    if not report_path.is_file():
        raise RuntimeError(f"CLF3 did not produce an install report: {report_path}")
    report = load_json(report_path)
    blockers = {
        "manual archives": int(report.get("archives_manual", 0)),
        "failed archives": int(report.get("archives_failed", 0)),
        "failed directives": int(report.get("directives_failed", 0)),
    }
    blockers = {label: count for label, count in blockers.items() if count}
    if blockers:
        detail = ", ".join(f"{count} {label}" for label, count in blockers.items())
        raise RuntimeError(
            f"overlay staging is incomplete ({detail}); fix the listed downloads and rerun stage"
        )


def backup(path: Path, stamp: str) -> None:
    if path.exists():
        shutil.copy2(path, path.with_name(f"{path.name}.overlay-backup-{stamp}"))


def read_lines(path: Path) -> list[str]:
    if not path.exists():
        return ["# This file was automatically generated by Mod Organizer."]
    return path.read_text(encoding="utf-8-sig").splitlines()


def write_lines(path: Path, lines: list[str]) -> None:
    path.write_text("\r\n".join(lines) + "\r\n", encoding="utf-8")


def source_plugin_order(source: SourceManifest) -> list[str]:
    if source.archive_path is None:
        return []
    for item in source.data.get("Directives", []):
        target = item.get("To", "")
        source_id = item.get("SourceDataID")
        if source_id and re.search(r"^profiles[\\/].*[\\/]plugins\.txt$", target, re.I):
            content = source.read_embedded(str(source_id)).decode("utf-8-sig", errors="replace")
            return [
                line.lstrip("*+").strip()
                for line in content.splitlines()
                if line.strip() and not line.startswith("#")
            ]
    return []


def merge_overlay(source: SourceManifest, plan: OverlayPlan, target: Path, stage: Path, profile_name: str | None) -> None:
    if fluorine_running():
        raise RuntimeError("Fluorine/Mod Organizer is running; close it before merging")
    staged_mods = stage / "mods"
    if not staged_mods.is_dir():
        raise RuntimeError(f"staged mods directory is missing: {staged_mods}")
    target_mods = target / "mods"
    folders = [folder for values in plan.groups.values() for folder in values]
    collisions = [folder for folder in folders if (target_mods / folder).exists()]
    if collisions:
        raise RuntimeError(
            f"refusing to overwrite {len(collisions)} existing mod folders: {', '.join(collisions[:8])}"
        )

    profile = find_profile(target, profile_name)
    stamp = time.strftime("%Y%m%d-%H%M%S")
    for filename in ("modlist.txt", "plugins.txt", "loadorder.txt"):
        backup(profile / filename, stamp)

    moved: list[str] = []
    for folder in folders:
        source_dir = staged_mods / folder
        if not source_dir.is_dir():
            continue
        destination = target_mods / folder
        shutil.move(str(source_dir), str(destination))
        meta = destination / "meta.ini"
        if not meta.exists():
            meta.write_text("[General]\nnotes=Installed by CLF3 selective overlay\n", encoding="utf-8")
        moved.append(folder)

    for separator in plan.groups:
        separator_dir = target_mods / separator
        separator_dir.mkdir(exist_ok=True)
        meta = separator_dir / "meta.ini"
        if not meta.exists():
            meta.write_text("[General]\n", encoding="utf-8")

    modlist_path = profile / "modlist.txt"
    current = read_lines(modlist_path)
    managed = {item.casefold() for item in moved} | {item.casefold() for item in plan.groups}
    remaining = [line for line in current if line.startswith("#") or line[1:].casefold() not in managed]
    header = [line for line in remaining if line.startswith("#")]
    remaining = [line for line in remaining if not line.startswith("#")]
    overlay_lines: list[str] = []
    for separator, group_folders in plan.groups.items():
        overlay_lines.extend(f"+{folder}" for folder in group_folders if folder in moved)
        overlay_lines.append(f"-{separator}")
    write_lines(modlist_path, header + overlay_lines + remaining)

    installed_plugins: dict[str, str] = {}
    for folder in moved:
        for path in (target_mods / folder).rglob("*"):
            if path.is_file() and path.suffix.casefold() in PLUGIN_SUFFIXES:
                installed_plugins.setdefault(path.name.casefold(), path.name)
    preferred = source_plugin_order(source)
    rank = {name.casefold(): index for index, name in enumerate(preferred)}
    plugin_names = sorted(installed_plugins.values(), key=lambda name: (rank.get(name.casefold(), 10**9), name.casefold()))

    plugins_path = profile / "plugins.txt"
    plugin_lines = read_lines(plugins_path)
    existing = {line.lstrip("*+").casefold() for line in plugin_lines if not line.startswith("#")}
    plugin_lines.extend(f"*{name}" for name in plugin_names if name.casefold() not in existing)
    write_lines(plugins_path, plugin_lines)

    loadorder_path = profile / "loadorder.txt"
    loadorder = read_lines(loadorder_path)
    existing = {line.casefold() for line in loadorder if not line.startswith("#")}
    loadorder.extend(name for name in plugin_names if name.casefold() not in existing)
    write_lines(loadorder_path, loadorder)
    print(f"Merged {len(moved)} mod folders into {target}")
    print(f"Enabled {len(plugin_names)} discovered plugins in profile {profile.name}")
    print(f"Profile backups use suffix overlay-backup-{stamp}")


def sort_overlay_mods(plan: OverlayPlan, target: Path, profile_name: str | None) -> None:
    if fluorine_running():
        raise RuntimeError("Fluorine/Mod Organizer is running; close it before sorting")
    profile = find_profile(target, profile_name)
    modlist_path = profile / "modlist.txt"
    current = read_lines(modlist_path)
    folders = [folder for values in plan.groups.values() for folder in values]
    managed = {item.casefold() for item in folders} | {item.casefold() for item in plan.groups}
    states: dict[str, str] = {}
    remaining: list[str] = []
    for line in current:
        if line.startswith("#"):
            remaining.append(line)
            continue
        if len(line) > 1 and line[0] in "+-" and line[1:].casefold() in managed:
            states[line[1:].casefold()] = line[0]
            continue
        remaining.append(line)

    header = [line for line in remaining if line.startswith("#")]
    remaining = [line for line in remaining if not line.startswith("#")]
    overlay_lines: list[str] = []
    # MO2 stores the left pane in reverse display order. Emit reversed groups
    # so the recipe's category order is what users see from top to bottom.
    for separator, group_folders in reversed(list(plan.groups.items())):
        overlay_lines.extend(
            f"{states.get(folder.casefold(), '+')}{folder}" for folder in group_folders
        )
        overlay_lines.append(f"-{separator}")

    stamp = time.strftime("%Y%m%d-%H%M%S")
    backup(modlist_path, stamp)
    write_lines(modlist_path, header + overlay_lines + remaining)
    print(f"Sorted {len(folders)} overlay mod folders in profile {profile.name}")
    print(f"Modlist backup uses suffix overlay-backup-{stamp}")


def print_plan(plan: OverlayPlan) -> None:
    print(f"Overlay: {plan.manifest['Name']} {plan.manifest.get('Version', '')}")
    print(f"Archives: {len(plan.manifest['Archives'])} ({human_size(plan.archive_bytes)})")
    print(f"Directives: {len(plan.manifest['Directives'])}")
    print(f"Embedded payloads: {len(plan.embedded_ids)}")
    for separator, folders in plan.groups.items():
        print(f"  {separator}: {len(folders)} mod folders")
    if plan.omitted_directives:
        print(f"Regenerated metadata directives: {len(plan.omitted_directives)}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("plan", "build", "stage", "merge", "sort", "install"))
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--recipe", type=Path, required=True)
    parser.add_argument("--target", type=Path, required=True)
    parser.add_argument("--overlay", type=Path)
    parser.add_argument("--stage", type=Path)
    parser.add_argument("--downloads", type=Path)
    parser.add_argument("--game", type=Path)
    parser.add_argument("--profile")
    parser.add_argument("--clf3", type=Path, default=Path(__file__).resolve().parents[1] / "target" / "release" / "clf3")
    parser.add_argument("--concurrent", type=int, default=8)
    parser.add_argument("--install-workers", type=int, default=8)
    parser.add_argument("--sevenzip-workers", type=int, default=4)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    for name in ("source", "recipe", "target", "clf3"):
        setattr(args, name, getattr(args, name).expanduser().resolve())
    source = SourceManifest.open(args.source)
    plan = build_plan(source, load_json(args.recipe))
    print_plan(plan)
    if args.action != "plan" and source.archive_path is None:
        raise RuntimeError(
            "build/stage/install requires the complete source .wabbajack; "
            "the extracted modlist is planning-only"
        )
    overlay = (args.overlay or args.source.parent / "merethic-body-overlay.wabbajack").expanduser().resolve()
    stage = (args.stage or args.target.parent / f".{args.target.name}.overlay-staging").expanduser().resolve()
    if args.action in {"build", "stage", "install"}:
        write_overlay(source, plan, overlay)
        print(f"Wrote overlay: {overlay}")
    if args.action in {"stage", "install"}:
        stage_overlay(args, overlay, stage)
    if args.action in {"merge", "install"}:
        merge_overlay(source, plan, args.target, stage, args.profile)
    if args.action == "sort":
        sort_overlay_mods(plan, args.target, args.profile)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (RuntimeError, subprocess.CalledProcessError, OSError, KeyError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1)
