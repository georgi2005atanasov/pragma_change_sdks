# PragmaChange reference clients

Small **reference implementations** for integrating an application with [PragmaChange](https://github.com/georgi2005atanasov/pragma_change). Study them, adapt them, or write your own client. These examples are not finished SDK products and are not published to package registries.

The examples demonstrate two independent paths:

- **Record files locally:** write request or behavior JSONL plus a finalized manifest, then upload through the management portal later. No running service or ingestion token is needed during collection.
- **Integrate live behavior:** send authenticated application events to the behavior service and evaluate actions before their side effects. Your application handles the returned decision.

You can generate compatible datasets without using any of this code. Ordinary request JSONL and feature CSV imports do not require the reference recorder's manifest. See [formats and recording](docs/recording.md).

## Start here

```bash
git clone https://github.com/georgi2005atanasov/pragma_change_sdks.git
cd pragma_change_sdks
```

Access to the repositories is currently restricted. This checkout builds independently; the product checkout is only needed for running the server or comparing contract updates.

| Example | Runtime | Guide |
| --- | --- | --- |
| Node.js / TypeScript | Node.js 20+; ESM and TypeScript declarations | [Node.js](node/README.md) |
| Python | Python 3.10+ | [Python](python/README.md) |
| C# / .NET | .NET 8 | [C#](csharp/README.md) |
| Rust | Rust 1.98 used for validation; Tokio for live clients | [Rust](rust/README.md) |

The existing package names remain `@pragmachange/sdk`, `pragmachange` (Python import `pragma_change`), `PragmaChange`, and `pragmachange` respectively. Install from the local sources; registry installation is not part of this workflow.

## Record, upload, train

1. Save an endpoint pipeline in PragmaChange and download its templates, or register a behavior application.
2. Configure the explicit field allowlists and real pipeline/application bindings.
3. Record representative data into a **new directory**. Close/dispose the recorder to finalize `records.jsonl` and `manifest.json`.
4. Upload request files in **Datasets**, or behavior files in **Behavior → Import an offline recording**.
5. Review validation, then train. Activation/deployment is a separate step.

[Complete recording examples](docs/recording.md) explain checksums, original body sizes, workflow boundaries, privacy, failure behavior, and limits. A few synthetic rows demonstrate file generation; they do not establish a useful training baseline.

## Live application behavior

Create an application in the behavior service and keep its ingestion token on your server. Resolve actor/session identities from your authenticated context. Authorize the operation normally, call evaluation before the side effect, enforce `check`/`block`, and record the actual outcome afterward with a new event ID correlated to the attempt.

New applications default to shadow mode. An `allow` does not grant authorization. Network failures return `check`; your application must decide how to verify before proceeding. Collection is bounded and best effort: inspect dropped counters and close the client during shutdown. Offline recording and live streaming do not automatically enable each other.

## Verify the examples

```bash
python3 scripts/check-contract.py
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace
python3 -m unittest discover -s scripts/tests
node --test node/test.mjs node/recording.test.mjs
dotnet build csharp/PragmaChange.csproj
python3 scripts/test-sdk-parity.py
python3 scripts/test-sdk-recordings.py
```

The parity tests use disposable loopback listeners and temporary files. They do not need a database or a running PragmaChange service. Set `DOTNET` to an executable path if needed.

## Contract maintenance

`contract/rust` is a snapshot of the product's `crates/behavior-contract` version 0.1.0. It contains portable event, decision, and recording contracts—not the service. `contract/snapshot.json` records every source hash.

When updating both repositories, compare them explicitly:

```bash
python3 scripts/check-contract.py --main-repo ../pragma_change
```

The product contract remains canonical. Review protocol changes, copy its complete crate into `contract/rust`, update the recorded hashes/version, and run all parity tests together. Do not silently change bindings or recording hashes to accept incompatible data. The standalone CI checks the bundled snapshot; comparison with a newer private product checkout is an explicit maintainer step.
