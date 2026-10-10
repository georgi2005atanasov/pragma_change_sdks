# Python reference client

Requires Python 3.10+. In your application's virtual environment:

```bash
python3 -m pip install /absolute/path/to/pragma_change_sdks/python
```

Import `Client` and `DatasetRecorder` from `pragma_change`. The examples below run from the reference repository root.

## Record locally

First create `recording-config.json` from your saved pipeline templates using [the recording guide](../docs/recording.md#request-recorder-configuration).


Run from the repository root after creating `recording-config.json` above. This creates synthetic example data solely for testing the import flow:

```bash
mkdir -p recordings
PYTHONPATH=python python3 - <<'PY'
import json
from pathlib import Path
from pragma_change import DatasetRecorder
config = json.loads(Path('recording-config.json').read_text())
with DatasetRecorder('recordings/orders-python', config) as output:
    for index in range(40):
        output.record({
            'timestamp_ms': 1700000000000 + index * 100,
            'endpoint_id': config['endpoint_id'],
            'method': 'POST', 'path': '/orders',
            'headers': {'X-Client-ID': 'example-user'},
            'body': {'amount': 10 + index % 5},
            'body_size': 30,
        })
PY
```

For the example one-second window this produces 30 complete vectors after warm-up. In **Datasets**, choose `records.jsonl`, choose its **SDK recording manifest**, give the dataset a name, preview, and accept. Then train from that dataset. The server checks the exact file bytes and bindings and retains the recording manifest alongside the source. Legacy JSONL/CSV imports without a recording manifest remain supported.


## Live behavior integration

```python
import os
from pragma_change import Client
client = Client(os.environ['PRAGMA_URL'], os.environ['PRAGMA_TOKEN'],
                os.environ['PRAGMA_APP'], ['duration_ms'])
try:
    attempt = client.event(actor='server-authenticated-user',
        session_id='verified-session', workflow_id='workflow-1',
        action='document.export', outcome='attempted')
    decision = client.evaluate(**attempt)
    # Enforce decision['action'] before any side effect.
    # Record a new, correlated outcome event after the real operation.
finally:
    client.close()
```

Keep one client for the application's lifetime. `record(**event)` queues, `flush()` attempts delivery, and `health()` reports queue and dropped counts. The example identities are placeholders for verified server context.

## Upload and application decisions

Close/dispose the file recorder before uploading `records.jsonl` and `manifest.json`. Request recordings go to **Datasets**; behavior recordings go to **Behavior → Import an offline recording**. Training and activation remain explicit steps. Rust requires `close()`; dropping it does not finalize the manifest.

Apply normal authorization first. Only perform the operation after handling the returned action: `block` stops it, `check` requires your verification flow, and `allow` permits continuation of an already-authorized operation. Transport failure produces `check`. In shadow mode, inspect recommendations separately from the returned enforcement action.

Record actual outcomes with a new event ID and the attempt's ID as `correlation_id`. Reuse event IDs only when retrying the same event. Local recordings and live streams are independent and should not be imported twice as overlapping training data.

See [recording limits and privacy](../docs/recording.md#limits-and-privacy) and the [repository guide](../README.md). These are adaptable reference implementations, not a required integration layer.
