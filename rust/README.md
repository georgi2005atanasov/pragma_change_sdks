# Rust reference client

The examples are validated with Rust 1.98. In your application's `Cargo.toml`:

```toml
[dependencies]
pragmachange = { path = "/absolute/path/to/pragma_change_sdks/rust" }
serde_json = "1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

The bundled contract crate means this does not depend on a product checkout. Tokio is needed for the live client; the recorder is synchronous.

## Record locally

Create `recording-config.json` following [the recording guide](../docs/recording.md#request-recorder-configuration). The snippet below belongs inside a function returning a compatible `Result`.


The recorder is synchronous and needs no Tokio runtime; the HTTP client still does. Add `serde_json` to your application's dependencies when using this example:

```rust
use pragmachange::{DatasetRecorder, recording::RecordingConfig};
use serde_json::json;
let config: RecordingConfig = serde_json::from_slice(
    &std::fs::read("recording-config.json")?
)?;
let endpoint = config.endpoint_id.unwrap();
let mut recorder = DatasetRecorder::create("recordings/orders-rust", config)?;
recorder.record(json!({
    "timestamp_ms":1700000000000_u64, "endpoint_id":endpoint,
    "method":"POST", "path":"/orders", "body_size":30,
    "headers":{"X-Client-ID":"example-user"}, "body":{"amount":12}
}))?;
recorder.close()?; // mandatory: Drop does not finalize
```

Or use the runnable example from the repository root:

```bash
cargo run -p pragmachange --example recording -- \
  recordings/orders-rust recording-config.json request-events.jsonl
cargo run -p pragmachange --example recording -- --verify recordings/orders-rust
```


## Live behavior integration

```rust
use pragmachange::{Client, Event};
// Inside an async function returning Result:
let client = Client::new(
    std::env::var("PRAGMA_URL")?, std::env::var("PRAGMA_TOKEN")?,
    ["duration_ms".to_owned()].into(),
)?;
let attempt: Event = serde_json::from_slice(&std::fs::read("attempt.json")?)?;
let decision = client.evaluate(attempt.clone()).await;
// Enforce decision.action before any side effect.
// Record a new, correlated outcome event after the real operation.
client.close().await;
```

Construct `attempt.json` using the [v1 shape](../contract/event.json) with your registered application ID, fresh event ID, current timestamp, and authenticated identities. The checked fixture is only for tests. `Client::new` returns a string error; adapt it to your application's error type. `record`, `flush`, and `health` are asynchronous. Keep one client for the application's lifetime.

## Upload and application decisions

Close/dispose the file recorder before uploading `records.jsonl` and `manifest.json`. Request recordings go to **Datasets**; behavior recordings go to **Behavior → Import an offline recording**. Training and activation remain explicit steps. Rust requires `close()`; dropping it does not finalize the manifest.

Apply normal authorization first. Only perform the operation after handling the returned action: `block` stops it, `check` requires your verification flow, and `allow` permits continuation of an already-authorized operation. Transport failure produces `check`. In shadow mode, inspect recommendations separately from the returned enforcement action.

Record actual outcomes with a new event ID and the attempt's ID as `correlation_id`. Reuse event IDs only when retrying the same event. Local recordings and live streams are independent and should not be imported twice as overlapping training data.

See [recording limits and privacy](../docs/recording.md#limits-and-privacy) and the [repository guide](../README.md). These are adaptable reference implementations, not a required integration layer.
