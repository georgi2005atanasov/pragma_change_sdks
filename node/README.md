# Node.js / TypeScript reference client

Requires Node.js 20+. From your application folder, install the checked-out sources:

```bash
npm install /absolute/path/to/pragma_change_sdks/node
```

Use `import { PragmaChange, DatasetRecorder } from '@pragmachange/sdk'`. The examples below run from the reference repository root. TypeScript declarations ship with the source package.

## Record locally

Download your saved pipeline templates and create `recording-config.json` following [the recording guide](../docs/recording.md#request-recorder-configuration).


Import from `@pragmachange/sdk` after installing the local package. From this repository root, the equivalent is:

```javascript
import { readFileSync } from 'node:fs';
import { DatasetRecorder } from './node/index.js';
const config = JSON.parse(readFileSync('recording-config.json', 'utf8'));
const recorder = new DatasetRecorder('recordings/orders-node', config);
try {
  recorder.record({
    timestamp_ms: Date.now(), endpoint_id: config.endpoint_id,
    method: 'POST', path: '/orders', body_size: 30,
    headers: { 'X-Client-ID': 'example-user' }, body: { amount: 12 },
  });
} finally {
  recorder.close();
}
```

Use a `.mjs` file or an ES module project. `DatasetRecorder.record` writes synchronously; do not run high-volume disk synchronization on a latency-sensitive Node request loop. Put collection in an application-owned worker when necessary and define its own bounded queue/failure policy. The current recorder does not provide a durable background spool.


## Live behavior integration

```javascript
const client = new PragmaChange({
  url: process.env.PRAGMA_URL,
  token: process.env.PRAGMA_TOKEN,
  applicationId: process.env.PRAGMA_APP,
  allowedAttributes: ['duration_ms'],
});
const attempt = client.event({
  actor: 'server-authenticated-user', session_id: 'verified-session',
  workflow_id: 'workflow-1', action: 'document.export', outcome: 'attempted',
});
const decision = await client.evaluate(attempt);
// Enforce decision.action before any side effect.
// After a confirmed result, record a NEW event correlated to attempt.event_id.
await client.close(); // bounded best-effort drain on application shutdown
```

Keep one client for the application's lifetime. `record(event)` queues an event, `flush()` attempts delivery, and `health()` reports queued/sent/dropped counts.

## Upload and application decisions

Close/dispose the file recorder before uploading `records.jsonl` and `manifest.json`. Request recordings go to **Datasets**; behavior recordings go to **Behavior → Import an offline recording**. Training and activation remain explicit steps. Rust requires `close()`; dropping it does not finalize the manifest.

Apply normal authorization first. Only perform the operation after handling the returned action: `block` stops it, `check` requires your verification flow, and `allow` permits continuation of an already-authorized operation. Transport failure produces `check`. In shadow mode, inspect recommendations separately from the returned enforcement action.

Record actual outcomes with a new event ID and the attempt's ID as `correlation_id`. Reuse event IDs only when retrying the same event. Local recordings and live streams are independent and should not be imported twice as overlapping training data.

See [recording limits and privacy](../docs/recording.md#limits-and-privacy) and the [repository guide](../README.md). These are adaptable reference implementations, not a required integration layer.
