using System.Collections.Concurrent;
using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Http;
namespace PragmaChange;

public record BehaviorEvent {
    public int Version { get; init; } = 1;
    public Guid EventId { get; init; } = Guid.NewGuid();
    public Guid ApplicationId { get; init; }
    public required string Actor { get; init; }
    public required string Action { get; init; }
    public required string SessionId { get; init; }
    public string? Resource { get; init; }
    public string? WorkflowId { get; init; }
    public Guid? CorrelationId { get; init; }
    public long TimestampMs { get; init; } = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    public string Outcome { get; init; } = "succeeded";
    public string Cohort { get; init; } = "";
    public bool SequenceStart { get; init; }
    public ulong? SequenceNumber { get; init; }
    public Dictionary<string, object> Attributes { get; init; } = new();
}
public record Decision(string Action, string RecommendedAction, bool Shadow, double? Surprise, Guid? ModelId, string[] Reasons, double? SequenceMeanSurprise = null, string? Baseline = null, ulong EvidenceCount = 0);
public sealed class Client : IAsyncDisposable {
    public static readonly JsonSerializerOptions Json = new() { PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower, PropertyNameCaseInsensitive = true };
    private readonly HttpClient http;
    private readonly HashSet<string> allowed;
    private readonly Queue<(BehaviorEvent Event,int Attempts)> queue = new();
    private readonly object gate = new();
    private readonly SemaphoreSlim flushing = new(1);
    private readonly CancellationTokenSource stop = new();
    private readonly Task worker;
    private long sent, dropped;
    private bool closed;
    public Client(Uri url, string token, IEnumerable<string>? allowedAttributes = null) {
        http = new HttpClient(new HttpClientHandler { AllowAutoRedirect = false }) { BaseAddress = url, Timeout = TimeSpan.FromSeconds(2) };
        http.DefaultRequestHeaders.Authorization = new("Bearer", token);
        allowed = new(allowedAttributes ?? []);
        worker = Task.Run(async () => { try { using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1)); while(await timer.WaitForNextTickAsync(stop.Token)) await FlushAsync(); } catch(OperationCanceledException) {} });
    }
    private BehaviorEvent Sanitize(BehaviorEvent value) => value with { Attributes = value.Attributes.Where(x => allowed.Contains(x.Key)).ToDictionary(x => x.Key,x => x.Value) };
    public bool Record(BehaviorEvent value) {
        value=Sanitize(value);
        if(string.IsNullOrEmpty(value.Actor)||string.IsNullOrEmpty(value.Action)||string.IsNullOrEmpty(value.SessionId)||JsonSerializer.SerializeToUtf8Bytes(value,Json).Length>16384) throw new ArgumentException("Invalid or oversized event");
        lock(gate) { if(closed||queue.Count>=1000) { dropped++; return false; } queue.Enqueue((value,0)); return true; }
    }
    public async Task FlushAsync() {
        await flushing.WaitAsync();
        try {
            List<(BehaviorEvent Event,int Attempts)> batch = new();
            lock(gate) { while(batch.Count<100&&queue.TryDequeue(out var item)) batch.Add(item); }
            if(batch.Count==0) return;
            bool success=false, permanent=false;
            try { using var response=await http.PostAsJsonAsync("v1/events",new { events=batch.Select(x=>x.Event) },Json);success=response.IsSuccessStatusCode;permanent=(int)response.StatusCode>=400&&(int)response.StatusCode<500&&response.StatusCode!=HttpStatusCode.TooManyRequests; } catch(HttpRequestException) {} catch(TaskCanceledException) {}
            lock(gate) {
                if(success) {sent+=batch.Count;return;}
                var existing=queue.ToArray();queue.Clear();
                foreach(var item in batch) {if(permanent||item.Attempts>=4)dropped++;else queue.Enqueue((item.Event,item.Attempts+1));}
                foreach(var item in existing) {if(queue.Count>=1000)dropped++;else queue.Enqueue(item);}
            }
        } finally {flushing.Release();}
    }
    public async Task<Decision> EvaluateAsync(BehaviorEvent value) {
        try { using var response=await http.PostAsJsonAsync("v1/evaluate",Sanitize(value with {Outcome="attempted"}),Json);if(response.IsSuccessStatusCode) {var decision=await response.Content.ReadFromJsonAsync<Decision>(Json);if(decision?.Action is "allow" or "check" or "block")return decision;}} catch(HttpRequestException) {} catch(TaskCanceledException) {} catch(JsonException) {}
        return new("check","check",false,null,null,["behavior_service_unavailable"]);
    }
    public object Health() {lock(gate)return new {queued=queue.Count,sent,dropped};}
    public Func<HttpContext,Task> Middleware(RequestDelegate next,Func<HttpContext,BehaviorEvent?> resolve) => async context => {
        var started=System.Diagnostics.Stopwatch.StartNew();
        bool failed=false;
        try {await next(context);} catch {failed=true;throw;} finally {
            try {var value=resolve(context);if(value is not null)Record(value with {Outcome=!failed&&context.Response.StatusCode<400?"succeeded":"failed",Attributes=new(value.Attributes) { ["status_code"]=context.Response.StatusCode,["duration_ms"]=started.Elapsed.TotalMilliseconds }});}catch{lock(gate)dropped++;}
        }
    };
    public async ValueTask DisposeAsync() {
        lock(gate) {if(closed)return;closed=true;}
        stop.Cancel();await worker;
        var deadline=System.Diagnostics.Stopwatch.StartNew();
        while(deadline.Elapsed<TimeSpan.FromSeconds(5)) {
            lock(gate) {if(queue.Count==0)break;}
            await FlushAsync();
        }
        lock(gate) {dropped+=queue.Count;queue.Clear();}
        http.Dispose();stop.Dispose();
    }
}
