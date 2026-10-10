"""Private, bounded JSONL recordings. No network or feature extraction is required."""

import hashlib
import json
import os
from pathlib import Path
import re
import threading
import time
from uuid import UUID, uuid4

DEFAULTS = dict(
    application_id=None,
    pipeline_version_id=None,
    schema_hash=None,
    endpoint_id=None,
    allowed_headers=[],
    allowed_query=[],
    allowed_body_fields=[],
    allowed_attributes=[],
    include_client_ip=False,
    max_bytes=4194304,
    max_records=100000,
)


def _uuid(value):
    try:
        return str(UUID(value))
    except (ValueError, TypeError, AttributeError):
        raise ValueError("valid UUID required") from None


def _string(value):
    if not isinstance(value, str) or len(value.encode()) > 512:
        raise ValueError("selected strings must be at most 512 bytes")
    return value


def _selected(value, fields, strings=False):
    result = {}
    for key, item in (value or {}).items():
        if key not in fields:
            continue
        if strings:
            _string(item)
        elif not isinstance(item, (str, int, float, bool)) or item is None:
            raise ValueError("selected body fields must be scalars")
        if isinstance(item, str):
            _string(item)
        result[key] = item
    return result


class DatasetRecorder:
    def __init__(self, directory, config):
        self.config = json.loads(json.dumps({**DEFAULTS, **config}))
        c = self.config
        if set(c) - set(DEFAULTS) - {"format"}:
            raise ValueError("unknown recording option")
        for key, limit in [("max_bytes", 4194304), ("max_records", 100000)]:
            if type(c[key]) is not int or not 1 <= c[key] <= limit:
                raise ValueError("invalid recording limit")
        for key in (
            "allowed_headers",
            "allowed_query",
            "allowed_body_fields",
            "allowed_attributes",
        ):
            if (
                not isinstance(c[key], list)
                or len(c[key]) > 32
                or any(
                    not isinstance(v, str)
                    or not v
                    or len(v.encode()) > 64
                    or re.search(r"[\x00-\x1f\x7f]", v)
                    for v in c[key]
                )
            ):
                raise ValueError("invalid allowlist")
        if type(c["include_client_ip"]) is not bool:
            raise ValueError("include_client_ip must be a boolean")
        if c.get("format") == "behavior_events":
            _uuid(c["application_id"])
        elif c.get("format") == "request_events":
            _uuid(c["endpoint_id"])
            _uuid(c["pipeline_version_id"])
            if not isinstance(c["schema_hash"], str) or not re.fullmatch(
                "[0-9a-fA-F]{64}", c["schema_hash"]
            ):
                raise ValueError("saved pipeline schema_hash required")
        else:
            raise ValueError("unsupported recording format")
        self.directory = Path(directory)
        self.directory.mkdir(mode=0o700)
        self.fd = os.open(
            self.directory / "records.jsonl",
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
        self.rows = self.bytes = 0
        self.hash = hashlib.sha256()
        self.id = str(uuid4())
        self.closed = self.failed = False
        self.lock = threading.Lock()
        try:
            self._manifest(False)
        except BaseException:
            os.close(self.fd)
            raise

    def _sanitize(self, input):
        c = self.config
        if c.get("format") == "behavior_events":
            event = dict(
                version=1,
                event_id=str(uuid4()),
                application_id=c["application_id"],
                resource=None,
                workflow_id=None,
                correlation_id=None,
                timestamp_ms=int(time.time() * 1000),
                outcome="succeeded",
                cohort="",
                sequence_start=False,
                sequence_number=None,
            )
            event.update(input)
            event["attributes"] = _selected(
                input.get("attributes"), c["allowed_attributes"]
            )
            allowed = {
                "version",
                "event_id",
                "application_id",
                "actor",
                "action",
                "resource",
                "session_id",
                "workflow_id",
                "correlation_id",
                "timestamp_ms",
                "outcome",
                "cohort",
                "sequence_start",
                "sequence_number",
                "attributes",
            }
            if (
                set(event) - allowed
                or event["version"] != 1
                or event["application_id"] != c["application_id"]
                or event["outcome"] not in ("attempted", "succeeded", "failed")
                or any(
                    not isinstance(event.get(k), str)
                    or not event[k]
                    or len(event[k].encode()) > 256
                    or re.search(r"[\x00-\x1f\x7f]", event[k])
                    for k in ("actor", "action", "session_id")
                )
                or not re.fullmatch(r"[a-zA-Z0-9._:-]+", event["action"])
            ):
                raise ValueError("invalid behavior event")
            _uuid(event["event_id"])
            if (
                type(event["timestamp_ms"]) is not int
                or not 0 <= event["timestamp_ms"] <= 9007199254740991
            ):
                raise ValueError("invalid timestamp")
            if (
                not isinstance(event["cohort"], str)
                or len(event["cohort"].encode()) > 128
                or type(event["sequence_start"]) is not bool
                or (
                    event["sequence_number"] is not None
                    and (
                        type(event["sequence_number"]) is not int
                        or not 0 <= event["sequence_number"] <= 9007199254740991
                    )
                )
                or any(
                    event[k] is not None
                    and (not isinstance(event[k], str) or len(event[k].encode()) > 256)
                    for k in ("resource", "workflow_id")
                )
                or any(
                    len(
                        json.dumps(
                            v,
                            ensure_ascii=False,
                            separators=(",", ":"),
                            allow_nan=False,
                        ).encode()
                    )
                    > 512
                    for v in event["attributes"].values()
                )
            ):
                raise ValueError("invalid behavior context")
            if event["correlation_id"] is not None:
                _uuid(event["correlation_id"])
            return event
        if (
            input.get("endpoint_id") != c["endpoint_id"]
            or not re.fullmatch("[A-Z]{1,16}", input.get("method", ""))
            or not isinstance(input.get("path"), str)
            or not input["path"].startswith("/")
            or re.search(r"[?#\x00-\x1f\x7f]", input["path"])
            or len(input["path"].encode()) > 2048
        ):
            raise ValueError("invalid request metadata")
        for k in ("timestamp_ms", "body_size"):
            if type(input.get(k)) is not int or not 0 <= input[k] <= 9007199254740991:
                raise ValueError("original body_size and timestamp_ms required")
        headers = {
            k.lower(): _string(v)
            for k, v in input.get("headers", {}).items()
            if k.lower() in [a.lower() for a in c["allowed_headers"]]
        }
        return dict(
            timestamp_ms=input["timestamp_ms"],
            endpoint_id=c["endpoint_id"],
            method=input["method"],
            path=input["path"],
            headers=headers,
            query=_selected(input.get("query"), c["allowed_query"], True),
            body=_selected(input.get("body"), c["allowed_body_fields"]),
            client_ip=(
                _string(input.get("client_ip", "")) if c["include_client_ip"] else ""
            ),
            body_size=input["body_size"],
        )

    def record(self, event):
        with self.lock:
            if self.closed or self.failed:
                raise ValueError("recorder is closed or failed")
            data = (
                json.dumps(
                    self._sanitize(event),
                    ensure_ascii=False,
                    separators=(",", ":"),
                    allow_nan=False,
                )
                + "\n"
            ).encode()
            if (
                len(data) > 16384
                or self.rows >= self.config["max_records"]
                or self.bytes + len(data) > self.config["max_bytes"]
            ):
                raise ValueError(
                    "recording limit reached; close and start another directory"
                )
            try:
                offset = 0
                while offset < len(data):
                    offset += os.write(self.fd, data[offset:])
                os.fsync(self.fd)
            except OSError:
                self.failed = True
                raise
            self.hash.update(data)
            self.rows += 1
            self.bytes += len(data)

    def _manifest(self, finalized):
        value = dict(
            format_version=1,
            recording_id=self.id,
            config=self.config,
            pipeline_version_id=self.config["pipeline_version_id"],
            schema_hash=self.config["schema_hash"],
            row_count=self.rows,
            byte_count=self.bytes,
            content_sha256=self.hash.hexdigest(),
            finalized=finalized,
        )
        temporary = self.directory / ".manifest.tmp"
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w") as f:
            json.dump(value, f, indent=2, allow_nan=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temporary, self.directory / "manifest.json")
        return value

    def close(self):
        with self.lock:
            if self.closed:
                return
            self.closed = True
            try:
                if self.failed:
                    raise ValueError(
                        "recording write failed; manifest remains unfinished"
                    )
                return self._manifest(True)
            finally:
                os.close(self.fd)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
