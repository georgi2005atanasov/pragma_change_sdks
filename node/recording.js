import {
  mkdirSync,
  openSync,
  writeSync,
  fsyncSync,
  closeSync,
  renameSync,
} from "node:fs";
import { createHash, randomUUID } from "node:crypto";
import { join } from "node:path";
const uuid = (v) =>
  typeof v === "string" &&
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(v);
const defaults = {
  application_id: null,
  pipeline_version_id: null,
  schema_hash: null,
  endpoint_id: null,
  allowed_headers: [],
  allowed_query: [],
  allowed_body_fields: [],
  allowed_attributes: [],
  include_client_ip: false,
  max_bytes: 4194304,
  max_records: 100000,
};
const text = (v) => {
  if (typeof v !== "string" || Buffer.byteLength(v) > 512)
    throw Error("Selected values must be strings of at most 512 bytes");
  return v;
};
function selected(value, fields, strings = false) {
  const out = {};
  for (const [k, v] of Object.entries(value || {}))
    if (fields.includes(k)) {
      if (strings) text(v);
      else if (
        !["string", "number", "boolean"].includes(typeof v) ||
        (typeof v === "number" && !Number.isFinite(v))
      )
        throw Error("Selected body fields must be scalars");
      if (typeof v === "string") text(v);
      Object.defineProperty(out, k, { value: v, enumerable: true });
    }
  return out;
}
/** Explicit disk recording, independent of streaming. One writer per new directory. */
export class DatasetRecorder {
  constructor(directory, config) {
    this.config = { ...defaults, ...structuredClone(config) };
    const c = this.config;
    if (
      Object.keys(c).some((k) => !Object.hasOwn(defaults, k) && k !== "format")
    )
      throw Error("Unknown recording option");
    if (
      !Number.isInteger(c.max_bytes) ||
      c.max_bytes < 1 ||
      c.max_bytes > 4194304 ||
      !Number.isInteger(c.max_records) ||
      c.max_records < 1 ||
      c.max_records > 100000
    )
      throw Error("Invalid recording limits");
    for (const field of [
      "allowed_headers",
      "allowed_query",
      "allowed_body_fields",
      "allowed_attributes",
    ])
      if (
        !Array.isArray(c[field]) ||
        c[field].length > 32 ||
        c[field].some(
          (v) =>
            typeof v !== "string" ||
            !v.length ||
            Buffer.byteLength(v) > 64 ||
            /[\x00-\x1f\x7f]/.test(v),
        )
      )
        throw Error("Invalid allowlist");
    if (typeof c.include_client_ip !== "boolean")
      throw Error("include_client_ip must be a boolean");
    if (c.format === "behavior_events") {
      if (!uuid(c.application_id)) throw Error("application_id required");
    } else if (c.format === "request_events") {
      if (
        !uuid(c.pipeline_version_id) ||
        !uuid(c.endpoint_id) ||
        !/^[a-f0-9]{64}$/i.test(c.schema_hash || "")
      )
        throw Error("Use the saved pipeline version, endpoint and schema hash");
    } else throw Error("Unsupported recording format");
    mkdirSync(directory, { mode: 0o700 });
    this.directory = directory;
    this.rows = 0;
    this.bytes = 0;
    this.hash = createHash("sha256");
    this.closed = false;
    this.failed = false;
    this.id = randomUUID();
    this.fd = openSync(join(directory, "records.jsonl"), "wx", 0o600);
    try {
      this.manifest(false);
    } catch (e) {
      closeSync(this.fd);
      throw e;
    }
  }
  sanitize(input) {
    const c = this.config;
    if (c.format === "behavior_events") {
      const value = {
        version: 1,
        event_id: randomUUID(),
        application_id: c.application_id,
        resource: null,
        workflow_id: null,
        correlation_id: null,
        timestamp_ms: Date.now(),
        outcome: "succeeded",
        cohort: "",
        sequence_start: false,
        sequence_number: null,
        ...input,
        attributes: selected(input.attributes, c.allowed_attributes),
      };
      const fields = [
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
      ];
      if (
        Object.keys(value).some((k) => !fields.includes(k)) ||
        value.version !== 1 ||
        value.application_id !== c.application_id ||
        !uuid(value.event_id) ||
        !["attempted", "succeeded", "failed"].includes(value.outcome) ||
        !["actor", "action", "session_id"].every(
          (k) =>
            typeof value[k] === "string" &&
            value[k].length &&
            Buffer.byteLength(value[k]) <= 256 &&
            !/[\x00-\x1f\x7f]/.test(value[k]),
        ) ||
        !/^[a-z0-9._:-]+$/i.test(value.action) ||
        !Number.isSafeInteger(value.timestamp_ms) ||
        value.timestamp_ms < 0
      )
        throw Error("Invalid behavior event");
      if (
        typeof value.cohort !== "string" ||
        Buffer.byteLength(value.cohort) > 128 ||
        typeof value.sequence_start !== "boolean" ||
        (value.sequence_number !== null &&
          (!Number.isSafeInteger(value.sequence_number) ||
            value.sequence_number < 0)) ||
        (value.correlation_id !== null && !uuid(value.correlation_id)) ||
        ["resource", "workflow_id"].some(
          (k) =>
            value[k] !== null &&
            (typeof value[k] !== "string" || Buffer.byteLength(value[k]) > 256),
        ) ||
        Object.values(value.attributes).some(
          (v) => Buffer.byteLength(JSON.stringify(v)) > 512,
        )
      )
        throw Error("Invalid behavior context");
      return value;
    }
    if (
      input.endpoint_id !== c.endpoint_id ||
      !Number.isSafeInteger(input.timestamp_ms) ||
      input.timestamp_ms < 0 ||
      !/^[A-Z]{1,16}$/.test(input.method) ||
      typeof input.path !== "string" ||
      !input.path.startsWith("/") ||
      /[?#\x00-\x1f\x7f]/.test(input.path) ||
      Buffer.byteLength(input.path) > 2048 ||
      !Number.isSafeInteger(input.body_size) ||
      input.body_size < 0
    )
      throw Error(
        "Invalid request metadata; supply original body_size and omit query from path",
      );
    const headers = {};
    for (const [k, v] of Object.entries(input.headers || {}))
      if (c.allowed_headers.some((a) => a.toLowerCase() === k.toLowerCase()))
        Object.defineProperty(headers, k.toLowerCase(), {
          value: text(v),
          enumerable: true,
        });
    return {
      timestamp_ms: input.timestamp_ms,
      endpoint_id: c.endpoint_id,
      method: input.method,
      path: input.path,
      headers,
      query: selected(input.query, c.allowed_query, true),
      body: selected(input.body, c.allowed_body_fields),
      client_ip: c.include_client_ip ? text(input.client_ip || "") : "",
      body_size: input.body_size,
    };
  }
  record(input) {
    if (this.closed || this.failed) throw Error("Recorder is closed or failed");
    const line = Buffer.from(JSON.stringify(this.sanitize(input)) + "\n");
    if (
      line.length > 16384 ||
      this.rows >= this.config.max_records ||
      this.bytes + line.length > this.config.max_bytes
    )
      throw Error("Recording limit reached; close and start another directory");
    try {
      let offset = 0;
      while (offset < line.length)
        offset += writeSync(this.fd, line, offset, line.length - offset);
      fsyncSync(this.fd);
    } catch (e) {
      this.failed = true;
      throw e;
    }
    this.hash.update(line);
    this.rows++;
    this.bytes += line.length;
  }
  manifest(finalized) {
    const value = {
      format_version: 1,
      recording_id: this.id,
      config: this.config,
      pipeline_version_id: this.config.pipeline_version_id,
      schema_hash: this.config.schema_hash,
      row_count: this.rows,
      byte_count: this.bytes,
      content_sha256: this.hash.copy().digest("hex"),
      finalized,
    };
    const temporary = join(this.directory, ".manifest.tmp");
    const fd = openSync(temporary, "wx", 0o600);
    try {
      const data = Buffer.from(JSON.stringify(value, null, 2) + "\n");
      let offset = 0;
      while (offset < data.length)
        offset += writeSync(fd, data, offset, data.length - offset);
      fsyncSync(fd);
    } finally {
      closeSync(fd);
    }
    renameSync(temporary, join(this.directory, "manifest.json"));
    return value;
  }
  close() {
    if (this.closed) return;
    this.closed = true;
    try {
      if (this.failed)
        throw Error("Recording write failed; manifest remains unfinished");
      return this.manifest(true);
    } finally {
      closeSync(this.fd);
    }
  }
}
