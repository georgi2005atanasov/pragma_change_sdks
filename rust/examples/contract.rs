use pragmachange::{Client, Event};
#[tokio::main]
async fn main() {
    let args: Vec<_> = std::env::args().collect();
    let event: Event = serde_json::from_slice(&std::fs::read(&args[2]).unwrap()).unwrap();
    let client = Client::new(
        args[1].clone(),
        "contract-test".into(),
        ["duration_ms".into()].into(),
    )
    .unwrap();
    client.record(event).await.unwrap();
    client.close().await;
}
