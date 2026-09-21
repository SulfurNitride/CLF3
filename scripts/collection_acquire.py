#!/usr/bin/env python3
"""Local Nexus acquisition broker for a pinned CLF3 plan.

Reads the saved CLF3 key only in this process. Authenticated requests never
redirect; archive downloads use a different, unauthenticated opener. Reports
contain identities and verified paths, never keys or signed URLs.
"""
import argparse
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


class AcquisitionError(Exception):
    pass


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'md5').hexdigest()


def atomic_json(path, value):
    tmp = path.with_suffix(path.suffix + '.tmp')
    with tmp.open('w') as stream:
        json.dump(value, stream, indent=2)
        stream.flush()
        os.fsync(stream.fileno())
    os.chmod(tmp, 0o600)
    tmp.replace(path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('plan', type=Path)
    parser.add_argument('--cache', required=True, type=Path)
    parser.add_argument('--workers', type=int, default=4)
    parser.add_argument('--manifest', type=Path, help='Private collection.json input for exact direct-download URLs')
    parser.add_argument('--settings', type=Path, default=Path(os.environ.get('XDG_CONFIG_HOME', str(Path.home() / '.config'))) / 'clf3/settings.json')
    args = parser.parse_args()
    os.umask(0o077)
    plan = json.loads(args.plan.read_text())
    needed = {m['artifact_id'] for m in plan['members'] if m['selected']}
    artifacts = [a for a in plan['artifacts'] if a['id'] in needed]
    direct_urls = {}
    if args.manifest:
        manifest = json.loads(args.manifest.read_text())
        for member in plan['members']:
            if not member['selected']:
                continue
            raw = manifest['mods'][member['source_index']]
            source = raw['source']
            if source['type'] != 'direct':
                continue
            domain = raw.get('domainName') or manifest['info']['domainName']
            identity = [source['type'], domain, source.get('modId', 0), source.get('fileId', 0), source.get('md5', '').lower(), source.get('fileSize'), source.get('url', ''), source.get('fileExpression', ''), source.get('updatePolicy', 'exact')]
            expected_id = 'artifact-' + hashlib.sha256(json.dumps(identity, ensure_ascii=False, separators=(',', ':')).encode()).hexdigest()
            if expected_id != member['artifact_id']:
                raise AcquisitionError('Direct source differs from the planned artifact')
            direct_urls[expected_id] = source['url']
    key = json.loads(args.settings.read_text()).get('nexus_api_key', '')
    if not key:
        raise AcquisitionError('No saved CLF3 Nexus key')
    args.cache.mkdir(parents=True, exist_ok=True)
    auth = urllib.request.build_opener(NoRedirect())

    def api(path):
        if not path.startswith('/v1/') or '?' in path:
            raise AcquisitionError('Invalid API path')
        request = urllib.request.Request('https://api.nexusmods.com' + path, headers={'apikey': key, 'User-Agent': 'CLF3/collections-local'})
        try:
            with auth.open(request, timeout=45) as response:
                return json.loads(response.read(2 * 1024 * 1024))
        except urllib.error.HTTPError as error:
            raise AcquisitionError(f'Nexus API HTTP {error.code}') from None
        except Exception:
            raise AcquisitionError('Nexus API request failed') from None

    account = api('/v1/users/validate.json')
    if not account.get('is_premium'):
        raise AcquisitionError('Unattended downloads require Nexus Premium; browser handoff is needed for this account')
    mapping, failures = {}, {}
    lock = threading.Lock()

    def acquire(artifact):
        identity = artifact['id']
        domain = artifact['domain']
        md5 = artifact['expected_md5'].lower()
        if artifact['source_type'] not in ('nexus', 'direct') or not re.fullmatch(r'[a-z0-9_-]+', domain) or not re.fullmatch(r'[a-f0-9]{32}', md5):
            raise AcquisitionError('Unsupported artifact identity')
        target = args.cache / (md5 + '.archive')
        expected_size = artifact.get('expected_size')
        if target.is_file() and not target.is_symlink() and (expected_size is None or target.stat().st_size == expected_size) and digest(target) == md5:
            return str(target.resolve())
        for attempt in range(4):
            try:
                if artifact['source_type'] == 'direct':
                    if identity not in direct_urls:
                        raise AcquisitionError('Direct downloads require the matching private manifest')
                    links = [{'URI': direct_urls[identity]}]
                else:
                    links = api(f'/v1/games/{domain}/mods/{int(artifact["mod_id"])}/files/{int(artifact["file_id"])}/download_link.json')
                if not isinstance(links, list) or not links:
                    raise AcquisitionError('No download mirror supplied')
                link = links[min(attempt, len(links) - 1)]['URI']
                parsed = urllib.parse.urlsplit(link)
                if parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password:
                    raise AcquisitionError('Invalid archive download URL')
                # Nexus may return spaces/non-ASCII names in the URL path.
                # Preserve existing escapes and the signed query verbatim.
                link = urllib.parse.urlunsplit(parsed._replace(path=urllib.parse.quote(parsed.path, safe='/%:@!$&\'()*+,;=-._~')))
                # Separate opener: the API key is never a default HTTP header.
                cdn = urllib.request.build_opener()
                request = urllib.request.Request(link, headers={'User-Agent': 'CLF3/collections-local'})
                temp = target.with_suffix('.' + uuid.uuid4().hex + '.part')
                hasher = hashlib.md5()
                count = 0
                with cdn.open(request, timeout=120) as response, temp.open('wb') as output:
                    while chunk := response.read(1024 * 1024):
                        count += len(chunk)
                        if count > (expected_size if expected_size is not None else 32 * 1024**3):
                            raise AcquisitionError('Archive exceeded expected size')
                        hasher.update(chunk)
                        output.write(chunk)
                    output.flush()
                    os.fsync(output.fileno())
                if (expected_size is not None and count != expected_size) or hasher.hexdigest() != md5:
                    raise AcquisitionError('Archive integrity mismatch')
                temp.replace(target)
                return str(target.resolve())
            except Exception as error:
                reason = str(error) if isinstance(error, AcquisitionError) else ('Archive HTTP ' + str(error.code) if isinstance(error, urllib.error.HTTPError) else 'Archive transfer failed: ' + type(error).__name__)
                if attempt == 3:
                    raise AcquisitionError(reason) from None
                time.sleep(2 ** attempt)

    def complete(artifact):
        try:
            path = acquire(artifact)
            with lock:
                mapping[artifact['id']] = path
                atomic_json(args.cache / 'artifacts.json', mapping)
                print(json.dumps({'downloaded': len(mapping), 'total': len(artifacts), 'failed': len(failures), 'mod_id': artifact['mod_id'], 'file_id': artifact['file_id']}), flush=True)
        except AcquisitionError as error:
            with lock:
                failures[artifact['id']] = str(error)
                atomic_json(args.cache / 'failures.json', failures)
                print(json.dumps({'download_failure': str(error), 'mod_id': artifact['mod_id'], 'file_id': artifact['file_id']}), flush=True)

    print(json.dumps({'acquiring': len(artifacts), 'credentials': 'saved CLF3 key; API host only'}), flush=True)
    # Largest first keeps the tail short; independent identities only.
    artifacts.sort(key=lambda a: a.get('expected_size') or 0, reverse=True)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, min(args.workers, 4))) as pool:
        list(pool.map(complete, artifacts))
    atomic_json(args.cache / 'failures.json', failures)
    print(json.dumps({'verified': len(mapping), 'total': len(artifacts), 'failed': len(failures)}), flush=True)
    return 1 if failures else 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except AcquisitionError as error:
        print(json.dumps({'error': str(error)}), flush=True)
        raise SystemExit(1)
