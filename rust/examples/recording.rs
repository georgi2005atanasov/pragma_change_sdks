//! cargo run -p pragmachange --example recording -- OUTPUT CONFIG_JSON RECORDS_JSONL
use pragmachange::{
    DatasetRecorder,
    recording::{RecordingConfig, RecordingManifest},
};
use std::{env, fs};
fn main() {
    let args: Vec<_> = env::args().collect();
    if args[1] == "--verify" {
        let manifest: RecordingManifest =
            serde_json::from_slice(&fs::read(format!("{}/manifest.json", args[2])).unwrap())
                .unwrap();
        let content = fs::read_to_string(format!("{}/records.jsonl", args[2])).unwrap();
        manifest.validate_content(&content).unwrap();
        for line in content.lines() {
            let value: serde_json::Value = serde_json::from_str(line).unwrap();
            assert_eq!(manifest.config.sanitize(value.clone()).unwrap(), value);
        }
    } else {
        let config: RecordingConfig = serde_json::from_slice(&fs::read(&args[2]).unwrap()).unwrap();
        let mut writer = DatasetRecorder::create(&args[1], config).unwrap();
        for line in fs::read_to_string(&args[3]).unwrap().lines() {
            writer.record(serde_json::from_str(line).unwrap()).unwrap();
        }
        writer.close().unwrap();
    }
}
