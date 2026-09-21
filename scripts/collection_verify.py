#!/usr/bin/env python3
"""Read-only audit of a published instance against its collection journal."""
import argparse
import configparser
import hashlib
import json
from pathlib import Path, PurePosixPath


def digest(path, algorithm='sha256'):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, algorithm).hexdigest()


def resolve(root, relative):
    parts = PurePosixPath(relative)
    if parts.is_absolute() or '..' in parts.parts or not parts.parts:
        raise ValueError('Unsafe audit path')
    path = root
    for part in parts.parts:
        exact = path / part
        if exact.exists():
            path = exact
        else:
            matches = [p for p in path.iterdir() if p.name.casefold() == part.casefold()]
            if len(matches) != 1:
                raise FileNotFoundError(relative)
            path = matches[0]
        if path.is_symlink():
            raise ValueError('Linked audit path')
    return path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('instance', type=Path)
    parser.add_argument('--source-snapshot', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    root = args.instance.resolve()
    journal = json.loads((root / '.collection/installation.json').read_text())
    plan = journal['plan']
    errors = []
    report = {'collection': plan['name'], 'installed_members': len(journal['members']), 'verified_mod_files': 0, 'verified_mod_bytes': 0, 'verified_archives': 0, 'verified_root_winners': 0, 'source_game_files_unchanged': 0, 'game_launch_tested': False, 'errors': errors}
    root_winners = {}
    for index, mid in enumerate(journal['asset_order']):
        member = journal['members'][mid]
        directory = resolve(root, member['directory'])
        expected = {'meta.ini'}
        for file in member['files']:
            relative = file['staged_path'] + ('.mohidden' if file['excluded'] else '')
            expected.add(relative.casefold())
            try:
                path = resolve(directory, relative)
                if path.stat().st_size != file['size'] or digest(path) != file['sha256']:
                    errors.append('Payload differs: ' + member['directory'] + '/' + relative)
                else:
                    report['verified_mod_files'] += 1
                    report['verified_mod_bytes'] += file['size']
            except (OSError, ValueError):
                errors.append('Missing or linked payload: ' + member['directory'] + '/' + relative)
            if file['deployment_root'] == 'game' and not file['excluded']:
                root_winners[file['staged_path'][5:].casefold()] = file
        actual = {str(p.relative_to(directory)).casefold() for p in directory.rglob('*') if p.is_file()}
        for extra in sorted(actual - expected):
            errors.append('Unexpected payload: ' + member['directory'] + '/' + extra)
        if (index + 1) % 100 == 0:
            print(json.dumps({'mods_checked': index + 1, 'total': len(journal['members']), 'errors': len(errors)}), flush=True)
    for relative, file in root_winners.items():
        try:
            if digest(resolve(root / 'Stock Game', relative)) != file['sha256']:
                errors.append('Root winner differs: ' + relative)
            else:
                report['verified_root_winners'] += 1
        except (OSError, ValueError):
            errors.append('Missing root winner: ' + relative)
    mapping_path = root / '.collection/artifacts.json'
    mapping = json.loads(mapping_path.read_text()) if mapping_path.exists() else {}
    selected_artifacts = {m['artifact_id'] for m in plan['members'] if m['selected']}
    for index, artifact in enumerate(plan['artifacts']):
        if artifact['id'] not in selected_artifacts or artifact['source_type'] == 'bundle':
            continue
        try:
            path = Path(mapping[artifact['id']])
            if path.is_symlink() or not path.resolve().is_relative_to(root / 'downloads'):
                raise ValueError('Archive outside instance downloads')
            if artifact['expected_size'] is not None and path.stat().st_size != artifact['expected_size']:
                raise ValueError('Archive size mismatch')
            if digest(path, 'md5') != artifact['expected_md5']:
                raise ValueError('Archive MD5 mismatch')
            report['verified_archives'] += 1
        except (KeyError, OSError, ValueError):
            errors.append('Archive differs or is absent: ' + artifact['id'])
        if (index + 1) % 250 == 0:
            print(json.dumps({'archives_checked': index + 1, 'total': len(plan['artifacts']), 'errors': len(errors)}), flush=True)
    artifacts = {a['id']: a for a in plan['artifacts']}
    report['verified_mod_metadata'] = 0
    for member in plan['members']:
        if not member['selected']:
            continue
        try:
            artifact = artifacts[member['artifact_id']]
            directory = resolve(root, journal['members'][member['id']]['directory'])
            metadata = configparser.ConfigParser(interpolation=None, strict=False)
            metadata.read(directory / 'meta.ini')
            general = metadata['General']
            archive_name = json.loads(general['installationfile']) if general['installationfile'].startswith(chr(34)) else general['installationfile']
            version = json.loads(general['version']) if general['version'].startswith(chr(34)) else general['version']
            expected_archive = artifact['expected_md5'] if artifact['source_type'] == 'bundle' else Path(mapping[artifact['id']]).name
            if archive_name != expected_archive or version != member['version'] or int(general['modid']) != artifact['mod_id'] or int(metadata['installedFiles']['1\\fileid']) != artifact['file_id']:
                raise ValueError('Metadata identity mismatch')
            report['verified_mod_metadata'] += 1
        except (KeyError, ValueError, OSError, configparser.Error):
            errors.append('Mod metadata differs: ' + member['name'])
    profile = root / 'profiles/Default'
    all_mod_rows = [line for line in (profile / 'modlist.txt').read_text().splitlines() if line.startswith(('+', '-', '*'))]
    report['automatic_base_mod_rows'] = sum(line.startswith('*') for line in all_mod_rows)
    lines = [line for line in all_mod_rows if line.startswith(('+', '-'))]
    real = [line for line in lines if not line[1:].endswith('_separator')]
    expected = ['+' + Path(journal['members'][mid]['directory']).name for mid in reversed(journal['asset_order'])]
    report['mod_order_and_states_match'] = real == expected
    if real != expected:
        errors.append('Mod order or enabled states differ from the installation plan')
    expected_separators = ['-Mod Additions_separator', '-Collection Mods_separator']
    report['separators'] = [line[1:] for line in lines if line[1:].endswith('_separator')]
    if [line for line in lines if line[1:].endswith('_separator')] != expected_separators or lines[0] != expected_separators[0] or lines[-1] != expected_separators[-1]:
        errors.append('Collection/Additions separator placement differs')
    for name in report['separators']:
        if any((root / 'mods' / name).iterdir()):
            errors.append('Separator is not empty: ' + name)
    profile_settings = configparser.ConfigParser(interpolation=None)
    profile_settings.read(profile / 'settings.ini')
    for key in ('LocalSaves', 'LocalSettings'):
        if not profile_settings.getboolean('General', key, fallback=False):
            errors.append('Profile isolation disabled: ' + key)
    report['profile_local_saves_and_settings'] = all(profile_settings.getboolean('General', key, fallback=False) for key in ('LocalSaves', 'LocalSettings'))
    plugins = {}
    ordered = []
    modern = plan['domain'] in ('skyrimspecialedition', 'fallout4')
    timestamp_order = plan['domain'] in ('oblivion', 'fallout3', 'newvegas')
    if plan['domain'] not in ('skyrimspecialedition', 'fallout4', 'oblivion', 'fallout3', 'newvegas', 'skyrim'):
        errors.append('Unsupported game activation format in audit')
    loadorder = [line.casefold() for line in (profile / 'loadorder.txt').read_text().splitlines() if line and not line.startswith('#')]
    for line in (profile / 'plugins.txt').read_text(encoding='cp1252').splitlines():
        if not line or line.startswith('#'):
            continue
        name = line.lstrip('*').casefold()
        if not modern and line.startswith('*'):
            errors.append('Legacy activation file contains an asterisk: ' + name)
        plugins[name] = line.startswith('*') if modern else True
        ordered.append(name)
    missing_records = []
    for plugin in plan['plugins']:
        name = plugin['name'].casefold()
        if name not in loadorder:
            missing_records.append(plugin['name'])
            if plugin['enabled']:
                errors.append('Authored enabled plugin missing: ' + plugin['name'])
        elif plugins.get(name, False) != plugin['enabled']:
            errors.append('Plugin enabled state differs: ' + plugin['name'])
    base_plugins = {p.name.casefold() for p in (root / 'Stock Game/Data').iterdir() if p.is_file() and p.suffix.casefold() in ('.esm', '.esp', '.esl')}
    declared = {p['name'].casefold() for p in plan['plugins']}
    unlisted = sorted(set(plugins) - declared - base_plugins)
    report['unlisted_mod_plugins_disabled'] = unlisted
    for name in unlisted:
        if plugins[name]:
            errors.append('Unlisted collection plugin was enabled: ' + name)
    report['omitted_plugin_records'] = missing_records
    report['enabled_plugins_checked'] = sum(plugins.values())
    expected_activation_order = loadorder if modern else [name for name in loadorder if plugins.get(name, False)]
    if expected_activation_order != ordered or len(ordered) != len(set(ordered)) or len(loadorder) != len(set(loadorder)):
        errors.append('Plugin order files disagree or contain duplicates')
    report['plugin_activation_format'] = 'asterisk' if modern else 'enabled-only'
    if timestamp_order:
        winners = {p.name.casefold(): p for p in (root / 'Stock Game/Data').iterdir() if p.is_file() and p.suffix.casefold() in ('.esm', '.esp')}
        for mid in journal['asset_order']:
            member = journal['members'][mid]
            for file in member['files']:
                relative = file['staged_path']
                if file['deployment_root'] == 'data' and not file['excluded'] and '/' not in relative and Path(relative).suffix.casefold() in ('.esm', '.esp'):
                    winners[relative.casefold()] = resolve(root, member['directory'] + '/' + relative)
        report['plugin_timestamps_checked'] = 0
        for index, name in enumerate(loadorder):
            if name not in winners or int(winners[name].stat().st_mtime) != 1577836800 + index * 60:
                errors.append('Plugin timestamp order differs: ' + name)
            else:
                report['plugin_timestamps_checked'] += 1
    desired = {}
    for tweak in plan.get('ini_tweaks', []):
        for edit in tweak['edits']:
            desired[(tweak['target'], edit['section'].casefold(), edit['key'].casefold())] = edit['value']
    parsed = {}
    for target, section, key in desired:
        if target not in parsed:
            config = configparser.ConfigParser(interpolation=None, strict=False)
            config.read(profile / target)
            parsed[target] = {(s.casefold(), k.casefold()): v for s in config.sections() for k, v in config.items(s)}
        if parsed[target].get((section, key)) != desired[(target, section, key)]:
            errors.append('INI tweak differs: ' + target + '/' + section + '/' + key)
    report['profile_ini_keys_checked'] = len(desired)
    if args.source_snapshot:
        snapshot = json.loads(args.source_snapshot.read_text())
        source = Path(snapshot['root'])
        for file in snapshot['files']:
            try:
                path = resolve(source, file['path'])
                if path.stat().st_size != file['size'] or digest(path) != file['sha256']:
                    errors.append('Source game changed: ' + file['path'])
                else:
                    report['source_game_files_unchanged'] += 1
                stock = resolve(root / 'Stock Game', file['path'])
                if (path.stat().st_dev, path.stat().st_ino) == (stock.stat().st_dev, stock.stat().st_ino):
                    errors.append('Game copy is a shared inode: ' + file['path'])
            except (OSError, ValueError):
                errors.append('Source/stock game file absent or linked: ' + file['path'])
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report), flush=True)
    return 1 if errors else 0


if __name__ == '__main__':
    raise SystemExit(main())
