import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "python"))
from pragma_change import DatasetRecorder

CONFIG = dict(
    format="request_events",
    endpoint_id="33333333-3333-4333-8333-333333333333",
    pipeline_version_id="11111111-1111-4111-8111-111111111111",
    schema_hash="a" * 64,
    allowed_body_fields=["amount"],
    max_records=1,
)
EVENT = dict(
    endpoint_id=CONFIG["endpoint_id"],
    timestamp_ms=1000,
    method="POST",
    path="/orders",
    body={"amount": 2, "secret": "private"},
    body_size=800,
)


class RecordingTests(unittest.TestCase):
    def test_disk_boundaries_and_checksum(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root) / "recording"
            with DatasetRecorder(directory, CONFIG) as writer:
                self.assertFalse(
                    json.loads((directory / "manifest.json").read_text())["finalized"]
                )
                for event in [
                    dict(EVENT, path="/orders?token=private"),
                    dict(EVENT, body={"amount": {"nested": "secret"}}),
                ]:
                    with self.assertRaises(ValueError):
                        writer.record(event)
                writer.record(EVENT)
                with self.assertRaises(ValueError):
                    writer.record(EVENT)
            manifest = json.loads((directory / "manifest.json").read_text())
            content = (directory / "records.jsonl").read_bytes()
            self.assertTrue(manifest["finalized"])
            self.assertEqual(
                manifest["content_sha256"], hashlib.sha256(content).hexdigest()
            )
            self.assertEqual(json.loads(content)["body_size"], 800)
            self.assertNotIn(b"private", content)
            with self.assertRaises(FileExistsError):
                DatasetRecorder(directory, CONFIG)
            with self.assertRaises(ValueError):
                writer.record(EVENT)

    def test_partial_write_cannot_finalize(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root) / "recording"
            writer = DatasetRecorder(directory, CONFIG)
            with patch(
                "pragma_change.recording.os.write", side_effect=OSError("disk full")
            ):
                with self.assertRaises(OSError):
                    writer.record(EVENT)
            with self.assertRaises(ValueError):
                writer.close()
            self.assertFalse(
                json.loads((directory / "manifest.json").read_text())["finalized"]
            )
