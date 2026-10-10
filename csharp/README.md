# C# / .NET reference client

Requires .NET 8. From your application directory:

```bash
dotnet add reference /absolute/path/to/pragma_change_sdks/csharp/PragmaChange.csproj
```

This is one C# implementation for .NET. Keep the namespace `PragmaChange`; there is no separate .NET client.

## Record locally

Create `recording-config.json` from your saved pipeline templates using [the recording guide](../docs/recording.md#request-recorder-configuration). Run these examples from the reference repository root.


Reference `csharp/PragmaChange.csproj`. The recorder takes the same JSON configuration and either a `JsonObject` request or a typed `BehaviorEvent`:

```csharp
using System.Text.Json.Nodes;
using PragmaChange;
var config = JsonNode.Parse(File.ReadAllText("recording-config.json"))!.AsObject();
using var recorder = new DatasetRecorder("recordings/orders-csharp", config);
recorder.Record(new JsonObject {
    ["timestamp_ms"] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
    ["endpoint_id"] = config["endpoint_id"]!.GetValue<string>(),
    ["method"] = "POST", ["path"] = "/orders", ["body_size"] = 30,
    ["headers"] = new JsonObject { ["X-Client-ID"] = "example-user" },
    ["body"] = new JsonObject { ["amount"] = 12 }
}); // Dispose finalizes manifest.json
```

The checked-in C# contract runner can also turn existing JSONL into a filtered recording:

```bash
dotnet run --project contract/csharp/Contract.csproj -- \
  --record recordings/orders-csharp recording-config.json request-events.jsonl
```


## Live behavior integration

```csharp
await using var client = new Client(
    new Uri(Environment.GetEnvironmentVariable("PRAGMA_URL")!),
    Environment.GetEnvironmentVariable("PRAGMA_TOKEN")!, new[] { "duration_ms" });
var attempt = new BehaviorEvent {
    ApplicationId = Guid.Parse(Environment.GetEnvironmentVariable("PRAGMA_APP")!),
    Actor = "server-authenticated-user", SessionId = "verified-session",
    WorkflowId = "workflow-1", Action = "document.export", Outcome = "attempted"
};
var decision = await client.EvaluateAsync(attempt);
// Enforce decision.Action before any side effect.
// Record a new, correlated outcome event after the real operation.
```

Use a base URL ending in `/`, such as `http://127.0.0.1:8090/`. `Record`, `FlushAsync`, and `Health` expose collection and counters; `DisposeAsync` drains on shutdown. Keep the client for the application lifetime.

## Upload and application decisions

Close/dispose the file recorder before uploading `records.jsonl` and `manifest.json`. Request recordings go to **Datasets**; behavior recordings go to **Behavior → Import an offline recording**. Training and activation remain explicit steps. Rust requires `close()`; dropping it does not finalize the manifest.

Apply normal authorization first. Only perform the operation after handling the returned action: `block` stops it, `check` requires your verification flow, and `allow` permits continuation of an already-authorized operation. Transport failure produces `check`. In shadow mode, inspect recommendations separately from the returned enforcement action.

Record actual outcomes with a new event ID and the attempt's ID as `correlation_id`. Reuse event IDs only when retrying the same event. Local recordings and live streams are independent and should not be imported twice as overlapping training data.

See [recording limits and privacy](../docs/recording.md#limits-and-privacy) and the [repository guide](../README.md). These are adaptable reference implementations, not a required integration layer.
