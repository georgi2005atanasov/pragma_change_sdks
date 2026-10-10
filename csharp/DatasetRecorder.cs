using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
namespace PragmaChange;

/// <summary>Bounded offline JSONL output. Use a fresh directory and Dispose to finalize.</summary>
public sealed class DatasetRecorder : IDisposable
{
    private readonly string directory;
    private readonly JsonObject config;
    private readonly FileStream file;
    private readonly IncrementalHash hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
    private readonly string id = Guid.NewGuid().ToString();
    private readonly object gate = new();
    private long rows, bytes;
    private bool closed, failed;
    private static readonly JsonSerializerOptions Options = new(Client.Json) { WriteIndented = false };
    public DatasetRecorder(string directory, JsonObject configuration)
    {
        config = JsonNode.Parse("""{"application_id":null,"pipeline_version_id":null,"schema_hash":null,"endpoint_id":null,"allowed_headers":[],"allowed_query":[],"allowed_body_fields":[],"allowed_attributes":[],"include_client_ip":false,"max_bytes":4194304,"max_records":100000}""")!.AsObject();
        foreach (var (key, value) in configuration)
        {
            if (!config.ContainsKey(key) && key != "format") throw new ArgumentException("Unknown recording option");
            config[key] = value?.DeepClone();
        }
        if (Limit("max_bytes") is < 1 or > 4194304 || Limit("max_records") is < 1 or > 100000) throw new ArgumentException("Invalid recording limits");
        foreach (var field in new[] { "allowed_headers", "allowed_query", "allowed_body_fields", "allowed_attributes" })
        {
            var list = config[field]!.AsArray();
            if (list.Count > 32 || list.Any(v => v is null || string.IsNullOrEmpty(v.GetValue<string>()) || Encoding.UTF8.GetByteCount(v.GetValue<string>()) > 64 || v.GetValue<string>().Any(char.IsControl))) throw new ArgumentException("Invalid allowlist");
        }
        if (Text(config, "format") == "behavior_events") Guid.Parse(Text(config, "application_id"));
        else if (Text(config, "format") == "request_events")
        {
            Guid.Parse(Text(config, "endpoint_id")); Guid.Parse(Text(config, "pipeline_version_id"));
            if (!Regex.IsMatch(Text(config, "schema_hash"), "\\A[0-9a-fA-F]{64}\\z")) throw new ArgumentException("Saved pipeline schema hash required");
        }
        else throw new ArgumentException("Unsupported recording format");
        this.directory = Path.GetFullPath(directory);
        if (Directory.Exists(this.directory)) throw new IOException("Use a new recording directory");
        if (OperatingSystem.IsWindows()) Directory.CreateDirectory(this.directory);
        else Directory.CreateDirectory(this.directory, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
        file = PrivateFile(Path.Combine(this.directory, "records.jsonl"));
        try { SaveManifest(false); } catch { file.Dispose(); hash.Dispose(); throw; }
    }
    private static FileStream PrivateFile(string path)
    {
        var options = new FileStreamOptions { Mode = FileMode.CreateNew, Access = FileAccess.Write, Share = FileShare.Read };
        if (!OperatingSystem.IsWindows()) options.UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite;
        return new FileStream(path, options);
    }
    private long Limit(string key) => config[key]!.GetValue<long>();
    private static string Text(JsonObject value, string key) => value[key]?.GetValue<string>() ?? throw new ArgumentException($"{key} required");
    private HashSet<string> Allowed(string field) => new(config[field]!.AsArray().Select(v => v!.GetValue<string>()));
    private static JsonNode StringValue(JsonNode? value)
    {
        if (value is not JsonValue scalar || !scalar.TryGetValue<string>(out var text) || Encoding.UTF8.GetByteCount(text) > 512) throw new ArgumentException("Selected strings must be at most 512 bytes");
        return JsonValue.Create(text)!;
    }
    private JsonObject Select(JsonNode? input, string field, bool strings = false)
    {
        var result = new JsonObject(); var allowed = Allowed(field);
        if (input is JsonObject values) foreach (var (key, value) in values) if (allowed.Contains(key))
                {
                    if (strings) result[key] = StringValue(value);
                    else
                    {
                        if (value is not JsonValue v) throw new ArgumentException("Selected fields must be scalars");
                        if (v.GetValueKind() == JsonValueKind.String) StringValue(value);
                        result[key] = value.DeepClone();
                    }
                }
        return result;
    }
    private JsonObject Sanitize(JsonObject input)
    {
        if (Text(config, "format") == "behavior_events")
        {
            var value = input.Deserialize<BehaviorEvent>(Client.Json) ?? throw new ArgumentException("Invalid behavior event");
            if (value.Version != 1 || value.ApplicationId != Guid.Parse(Text(config, "application_id")) || value.TimestampMs < 0 || !new[] { "attempted", "succeeded", "failed" }.Contains(value.Outcome) || new[] { value.Actor, value.Action, value.SessionId }.Any(v => string.IsNullOrEmpty(v) || Encoding.UTF8.GetByteCount(v) > 256 || v.Any(char.IsControl)) || !Regex.IsMatch(value.Action, "\\A[a-zA-Z0-9._:-]+\\z")) throw new ArgumentException("Invalid behavior event");
            if (value.Cohort is null || Encoding.UTF8.GetByteCount(value.Cohort) > 128 || new[] { value.Resource, value.WorkflowId }.Any(v => v is not null && Encoding.UTF8.GetByteCount(v) > 256)) throw new ArgumentException("Invalid behavior context");
            var result = JsonSerializer.SerializeToNode(value, Client.Json)!.AsObject();
            var attributes = Select(input["attributes"], "allowed_attributes");
            if (attributes.Any(v => Encoding.UTF8.GetByteCount(v.Value!.ToJsonString(Options)) > 512)) throw new ArgumentException("Attribute value exceeds 512 bytes");
            result["attributes"] = attributes; return result;
        }
        var timestamp = input["timestamp_ms"]!.GetValue<long>(); var size = input["body_size"]!.GetValue<long>();
        var method = Text(input, "method"); var path = Text(input, "path");
        if (Guid.Parse(Text(input, "endpoint_id")) != Guid.Parse(Text(config, "endpoint_id")) || timestamp < 0 || size < 0 || !Regex.IsMatch(method, "\\A[A-Z]{1,16}\\z") || !path.StartsWith('/') || Encoding.UTF8.GetByteCount(path) > 2048 || path.IndexOfAny(['?', '#']) >= 0 || path.Any(char.IsControl)) throw new ArgumentException("Invalid request metadata");
        var headers = new JsonObject(); var allowed = new HashSet<string>(Allowed("allowed_headers"), StringComparer.OrdinalIgnoreCase);
        if (input["headers"] is JsonObject values) foreach (var (key, value) in values) if (allowed.Contains(key)) headers[key.ToLowerInvariant()] = StringValue(value);
        return new JsonObject { ["timestamp_ms"] = timestamp, ["endpoint_id"] = Text(config, "endpoint_id"), ["method"] = method, ["path"] = path, ["headers"] = headers, ["query"] = Select(input["query"], "allowed_query", true), ["body"] = Select(input["body"], "allowed_body_fields"), ["body_size"] = size, ["client_ip"] = config["include_client_ip"]!.GetValue<bool>() ? StringValue(input["client_ip"] ?? JsonValue.Create("")) : JsonValue.Create("") };
    }
    public void Record(BehaviorEvent value) => Record(JsonSerializer.SerializeToNode(value, Client.Json)!.AsObject());
    public void Record(JsonObject input)
    {
        lock (gate)
        {
            if (closed || failed) throw new IOException("Recorder is closed or failed");
            var data = Encoding.UTF8.GetBytes(Sanitize(input).ToJsonString(Options) + "\n");
            if (data.Length > 16384 || rows >= Limit("max_records") || bytes + data.Length > Limit("max_bytes")) throw new IOException("Recording limit reached; close and start another directory");
            try { file.Write(data); file.Flush(true); } catch { failed = true; throw; }
            hash.AppendData(data); rows++; bytes += data.Length;
        }
    }
    private void SaveManifest(bool finalized)
    {
        var manifest = new JsonObject { ["format_version"] = 1, ["recording_id"] = id, ["config"] = config.DeepClone(), ["pipeline_version_id"] = config["pipeline_version_id"]?.DeepClone(), ["schema_hash"] = config["schema_hash"]?.DeepClone(), ["row_count"] = rows, ["byte_count"] = bytes, ["content_sha256"] = Convert.ToHexString(hash.GetCurrentHash()).ToLowerInvariant(), ["finalized"] = finalized };
        var temporary = Path.Combine(directory, ".manifest.tmp");
        using (var stream = PrivateFile(temporary)) { stream.Write(Encoding.UTF8.GetBytes(manifest.ToJsonString(Options))); stream.Flush(true); }
        File.Move(temporary, Path.Combine(directory, "manifest.json"), true);
    }
    public void Dispose()
    {
        lock (gate)
        {
            if (closed) return; closed = true;
            try { if (failed) throw new IOException("Write failed; manifest remains unfinished"); SaveManifest(true); }
            finally { file.Dispose(); hash.Dispose(); }
        }
    }
}
