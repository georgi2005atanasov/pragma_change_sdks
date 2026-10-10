import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatasetRecorder } from "./index.js";
const config = {
  format: "request_events",
  endpoint_id: "33333333-3333-4333-8333-333333333333",
  pipeline_version_id: "11111111-1111-4111-8111-111111111111",
  schema_hash: "a".repeat(64),
  allowed_body_fields: ["amount"],
  max_records: 1,
};
const event = {
  endpoint_id: config.endpoint_id,
  timestamp_ms: 1000,
  method: "POST",
  path: "/orders",
  body: { amount: 2, secret: "private" },
  body_size: 800,
};
test("recordings survive rejected rows, have limits, and never overwrite files", () => {
  const root = mkdtempSync(join(tmpdir(), "pragma-node-"));
  try {
    const dir = join(root, "dataset"),
      r = new DatasetRecorder(dir, config);
    assert.equal(
      JSON.parse(readFileSync(join(dir, "manifest.json"))).finalized,
      false,
    );
    assert.throws(() =>
      r.record({ ...event, body: { amount: { nested: "secret" } } }),
    );
    assert.throws(() => r.record({ ...event, path: "/orders?token=secret" }));
    r.record(event);
    assert.throws(() => r.record(event));
    const manifest = r.close();
    assert.equal(manifest.row_count, 1);
    assert.equal(
      JSON.parse(readFileSync(join(dir, "records.jsonl"))).body_size,
      800,
    );
    assert.equal(
      readFileSync(join(dir, "records.jsonl"), "utf8").includes("private"),
      false,
    );
    assert.throws(() => r.record(event));
    assert.throws(() => new DatasetRecorder(dir, config));
    const small = new DatasetRecorder(join(root, "small"), {
      ...config,
      max_bytes: 1,
    });
    assert.throws(() => small.record(event));
    assert.equal(small.close().row_count, 0);
  } finally {
    rmSync(root, { recursive: true });
  }
});
