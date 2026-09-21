"""Exercise credential separation and pinned direct downloads without networking."""
import contextlib
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('collection_acquire', Path(__file__).resolve().parents[1] / 'scripts/collection_acquire.py')
broker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(broker)


class AcquisitionTest(unittest.TestCase):
    def fixture(self, root):
        payload = b'fixture archive bytes'
        source = {'type': 'direct', 'url': 'https://github.com/example/mod/releases/download/v1/pinned.7z', 'md5': hashlib.md5(payload).hexdigest(), 'fileSize': len(payload), 'updatePolicy': 'exact'}
        identity = ['direct', 'skyrimspecialedition', 0, 0, source['md5'], len(payload), source['url'], '', 'exact']
        aid = 'artifact-' + hashlib.sha256(json.dumps(identity, separators=(',', ':')).encode()).hexdigest()
        manifest = {'info': {'domainName': 'skyrimspecialedition'}, 'mods': [{'source': source}]}
        plan = {'members': [{'artifact_id': aid, 'selected': True, 'source_index': 0}], 'artifacts': [{'id': aid, 'source_type': 'direct', 'domain': 'skyrimspecialedition', 'mod_id': 0, 'file_id': 0, 'expected_md5': source['md5'], 'expected_size': len(payload)}]}
        for name, data in [('plan.json', plan), ('manifest.json', manifest), ('settings.json', {'nexus_api_key': 'FAKE_TEST_KEY'})]:
            (root / name).write_text(json.dumps(data))
        args = ['broker', str(root / 'plan.json'), '--manifest', str(root / 'manifest.json'), '--settings', str(root / 'settings.json'), '--cache', str(root / 'cache'), '--workers', '1']
        return payload, source, aid, args

    def test_direct_uses_an_unauthenticated_client_and_checks_pinned_bytes(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            payload, source, aid, args = self.fixture(root)
            requests = []

            class Opener:
                def __init__(self, authenticated):
                    self.authenticated = authenticated

                def open(self, request, timeout):
                    requests.append((self.authenticated, request.full_url, dict(request.header_items())))
                    if self.authenticated:
                        return io.BytesIO(b'{"is_premium":true}')
                    return io.BytesIO(payload)

            def opener(*handlers):
                return Opener(bool(handlers))

            output = io.StringIO()
            with patch('sys.argv', args), patch.object(broker.urllib.request, 'build_opener', opener), contextlib.redirect_stdout(output):
                self.assertEqual(broker.main(), 0)
            self.assertEqual(len(requests), 2)
            self.assertEqual(requests[0][1], 'https://api.nexusmods.com/v1/users/validate.json')
            self.assertIn('Apikey', requests[0][2])
            self.assertEqual(requests[1][1], source['url'])
            self.assertFalse(requests[1][0])
            self.assertNotIn('Apikey', requests[1][2])
            mapping = json.loads((root / 'cache/artifacts.json').read_text())
            self.assertEqual(Path(mapping[aid]).read_bytes(), payload)
            self.assertNotIn('FAKE_TEST_KEY', output.getvalue())
            self.assertNotIn(source['url'], output.getvalue())

    def test_changed_manifest_cannot_redirect_a_planned_direct_artifact(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _, _, _, args = self.fixture(root)
            manifest = json.loads((root / 'manifest.json').read_text())
            manifest['mods'][0]['source']['url'] = 'https://example.com/replacement.7z'
            (root / 'manifest.json').write_text(json.dumps(manifest))
            with patch('sys.argv', args), patch.object(broker.urllib.request, 'build_opener') as opener:
                with self.assertRaisesRegex(broker.AcquisitionError, 'differs from the planned artifact'):
                    broker.main()
                opener.assert_not_called()


if __name__ == '__main__':
    unittest.main()
