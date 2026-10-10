use pragmachange::{
    DatasetRecorder,
    recording::{RecordingConfig, RecordingManifest},
};
use serde_json::json;
use std::fs;
use uuid::Uuid;
#[test]
fn bounded_disk_output_requires_explicit_finalization() {
    let root = std::env::temp_dir().join(format!("pragma-rust-{}", Uuid::new_v4()));
    fs::create_dir(&root).unwrap();
    let dir = root.join("complete");
    let config:RecordingConfig=serde_json::from_value(json!({"format":"request_events","endpoint_id":Uuid::nil(),"pipeline_version_id":Uuid::nil(),"schema_hash":"a".repeat(64),"max_records":1,"allowed_body_fields":["amount"]})).unwrap();
    let event = json!({"endpoint_id":Uuid::nil(),"timestamp_ms":1000,"method":"POST","path":"/orders","body_size":900,"body":{"amount":2,"secret":"private"}});
    let mut writer = DatasetRecorder::create(&dir, config.clone()).unwrap();
    assert!(DatasetRecorder::create(&dir, config.clone()).is_err());
    writer.record(event.clone()).unwrap();
    assert!(writer.record(event.clone()).is_err());
    let manifest = writer.close().unwrap();
    let content = fs::read_to_string(dir.join("records.jsonl")).unwrap();
    manifest.validate_content(&content).unwrap();
    assert!(!content.contains("private"));
    assert!(manifest.validate_content(&(content + " ")).is_err());
    let unfinished = root.join("unfinished");
    let mut writer = DatasetRecorder::create(&unfinished, config).unwrap();
    writer.record(event).unwrap();
    drop(writer);
    let manifest: RecordingManifest =
        serde_json::from_slice(&fs::read(unfinished.join("manifest.json")).unwrap()).unwrap();
    assert!(!manifest.finalized);
    assert!(
        manifest
            .validate_content(&fs::read_to_string(unfinished.join("records.jsonl")).unwrap())
            .is_err()
    );
    fs::remove_dir_all(root).unwrap();
}
