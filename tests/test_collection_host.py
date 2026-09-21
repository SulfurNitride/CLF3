"""Process-level Collections protocol checks. Run after cargo build."""
import json
from pathlib import Path
import select
import subprocess
import tempfile
import unittest

BINARY = Path(__file__).resolve().parents[1] / "target/debug/clf3"
SOURCE = "https://www.nexusmods.com/games/skyrimspecialedition/collections/qfftpq/revisions/12"


class CollectionHost(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.package = self.root / "package"
        self.package.mkdir()
        (self.package / "collection.json").write_text(json.dumps({
            "info": {"name": "Protocol fixture", "domainName": "skyrimspecialedition"},
            "mods": [],
        }))
        self.child = subprocess.Popen(
            [str(BINARY), "collection", "hosted-plan", SOURCE],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            cwd=self.root, text=True, bufsize=1,
        )

    def tearDown(self):
        if self.child.poll() is None:
            self.child.kill()
        self.child.wait(timeout=5)
        for pipe in (self.child.stdin, self.child.stdout, self.child.stderr):
            pipe.close()
        self.temp.cleanup()

    def receive(self):
        self.assertTrue(select.select([self.child.stdout], [], [], 5)[0], "protocol timeout")
        return json.loads(self.child.stdout.readline())

    def send(self, command):
        self.child.stdin.write(json.dumps(command) + "\n")
        self.child.stdin.flush()

    def handshake(self):
        hello = self.receive()
        self.assertIn("collection_plan_v1", hello["required_capabilities"])
        self.send({"type": "hello_ack", "protocol_version": 1, "capabilities": ["collection_plan_v1"]})
        request = self.receive()
        self.assertEqual(request["type"], "collection_revision_required")
        self.assertEqual(request["locator"]["revision"], 12)
        return request

    def test_pinned_package_produces_plan_and_no_install_event(self):
        request = self.handshake()
        self.send({"type": "collection_package_result", "job_id": request["job_id"],
                   "request_id": request["request_id"], "locator": request["locator"],
                   "schema_id": 1, "package_path": str(self.package)})
        event = self.receive()
        self.assertEqual(event["type"], "collection_plan_ready")
        self.assertFalse(event["installation_available"])
        self.assertEqual(event["plan"]["name"], "Protocol fixture")
        self.assertEqual(self.child.wait(timeout=5), 0)
        self.assertEqual(list(self.root.iterdir()), [self.package])

    def test_cancel_while_package_authorization_pending(self):
        request = self.handshake()
        self.send({"type": "cancel", "job_id": request["job_id"]})
        self.assertEqual(self.receive()["type"], "collection_cancelled")
        self.assertEqual(self.child.wait(timeout=5), 0)

    def test_credential_payload_is_rejected_without_echo(self):
        request = self.handshake()
        self.send({"type": "collection_package_result", "job_id": request["job_id"],
                   "request_id": request["request_id"], "locator": request["locator"],
                   "schema_id": 1, "package_path": str(self.package), "api_key": "fixture-secret-never-echo"})
        event = self.receive()
        self.assertEqual(event["type"], "collection_failed")
        self.assertNotIn("fixture-secret-never-echo", json.dumps(event))
        self.assertNotEqual(self.child.wait(timeout=5), 0)
        self.assertNotIn("fixture-secret-never-echo", self.child.stderr.read())

    def test_old_host_fails_before_acquisition(self):
        self.receive()
        self.send({"type": "hello_ack", "protocol_version": 1, "capabilities": []})
        self.assertEqual(self.receive()["type"], "collection_failed")
        self.assertNotEqual(self.child.wait(timeout=5), 0)


if __name__ == "__main__":
    unittest.main()
