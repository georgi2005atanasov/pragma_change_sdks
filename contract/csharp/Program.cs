using System.Text.Json;
using PragmaChange;
if(args[0]=="--record") {
    using var recorder=new DatasetRecorder(args[1],System.Text.Json.Nodes.JsonNode.Parse(File.ReadAllText(args[2]))!.AsObject());
    foreach(var line in File.ReadLines(args[3])) recorder.Record(System.Text.Json.Nodes.JsonNode.Parse(line)!.AsObject());
    return;
}
var value=JsonSerializer.Deserialize<BehaviorEvent>(File.ReadAllText(args[1]),Client.Json)!;
await using var client=new Client(new Uri(args[0]+"/"),"contract-test",new[]{"duration_ms"});
client.Record(value);
await client.FlushAsync();
