#!/usr/bin/env python3
"""Write real files with all SDKs; verify their checksums and shared Rust import contract."""

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
EVENT = json.loads((ROOT / "contract/event.json").read_text())
REQUEST = dict(
    timestamp_ms=1700000000000,
    endpoint_id="33333333-3333-4333-8333-333333333333",
    method="POST",
    path="/orders",
    headers={"X-Client-ID": "trusted-user", "Authorization": "MUST_NOT_SAVE"},
    query={"page": "1", "token": "MUST_NOT_SAVE"},
    body={"amount": 12.5, "password": "MUST_NOT_SAVE"},
    body_size=999,
    client_ip="192.0.2.1",
)
node = "import fs from 'node:fs';import {DatasetRecorder} from './node/index.js';const r=new DatasetRecorder(process.argv[1],JSON.parse(fs.readFileSync(process.argv[2])));for(const line of fs.readFileSync(process.argv[3],'utf8').trimEnd().split('\\n'))r.record(JSON.parse(line));r.close();"
python = "import json,sys;sys.path.insert(0,'python');from pragma_change import DatasetRecorder\nwith DatasetRecorder(sys.argv[1],json.load(open(sys.argv[2]))) as r:\n for line in open(sys.argv[3]):r.record(json.loads(line))"


def run(command):
    subprocess.run(
        command, cwd=ROOT, check=True, timeout=180, stdout=subprocess.DEVNULL
    )


with tempfile.TemporaryDirectory(prefix="pragma-recordings-") as tmp:
    root = Path(tmp)
    for kind, event, settings in [
        (
            "behavior_events",
            EVENT,
            dict(
                application_id=EVENT["application_id"],
                allowed_attributes=["duration_ms"],
            ),
        ),
        (
            "request_events",
            REQUEST,
            dict(
                endpoint_id=REQUEST["endpoint_id"],
                pipeline_version_id=EVENT["event_id"],
                schema_hash="a" * 64,
                allowed_headers=["X-Client-ID"],
                allowed_query=["page"],
                allowed_body_fields=["amount"],
            ),
        ),
    ]:
        config = root / (kind + ".json")
        config.write_text(json.dumps(dict(format=kind, **settings)))
        source = root / (kind + ".jsonl")
        source.write_text(json.dumps(event) + "\n")
        expected = (
            dict(EVENT, attributes={"duration_ms": 25})
            if kind == "behavior_events"
            else dict(
                REQUEST,
                headers={"x-client-id": "trusted-user"},
                query={"page": "1"},
                body={"amount": 12.5},
                client_ip="",
            )
        )
        for name in ["Node", "Python", "CSharp", "Rust"]:
            out = root / (kind + "-" + name)
            args = [str(out), str(config), str(source)]
            commands = {
                "Node": ["node", "--input-type=module", "-e", node, *args],
                "Python": [sys.executable, "-c", python, *args],
                "CSharp": [
                    os.environ.get("DOTNET", "dotnet"),
                    "run",
                    "--project",
                    "contract/csharp/Contract.csproj",
                    "--",
                    "--record",
                    *args,
                ],
                "Rust": [
                    "cargo",
                    "run",
                    "-q",
                    "-p",
                    "pragmachange",
                    "--example",
                    "recording",
                    "--",
                    *args,
                ],
            }
            run(commands[name])
            content = (out / "records.jsonl").read_bytes()
            manifest = json.loads((out / "manifest.json").read_text())
            assert manifest["finalized"] and manifest["row_count"] == 1
            assert manifest["byte_count"] == len(content)
            assert manifest["content_sha256"] == hashlib.sha256(content).hexdigest()
            assert json.loads(content) == expected, (name, kind, json.loads(content))
            assert b"MUST_NOT" not in content
            if os.name == "posix":
                assert out.stat().st_mode & 0o777 == 0o700
                for file in out.iterdir():
                    assert file.stat().st_mode & 0o777 == 0o600
            run(
                [
                    "cargo",
                    "run",
                    "-q",
                    "-p",
                    "pragmachange",
                    "--example",
                    "recording",
                    "--",
                    "--verify",
                    str(out),
                ]
            )
            print(
                name,
                kind,
                "file parity, privacy, permissions and import contract passed",
                flush=True,
            )
