//! Server-side SDK. Construct identities from authenticated application context.
pub use pragma_behavior_contract::{Decision, Event, Outcome};
use serde_json::json;
use std::{
    collections::{BTreeSet, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::Mutex;
struct Inner {
    url: String,
    token: String,
    http: reqwest::Client,
    queue: Mutex<VecDeque<(Event, u8)>>,
    flush: Mutex<()>,
    allowed: BTreeSet<String>,
    dropped: AtomicU64,
    sent: AtomicU64,
    closed: AtomicBool,
}
#[derive(Clone)]
pub struct Client(Arc<Inner>);
impl Client {
    pub fn new(
        url: String,
        token: String,
        allowed: BTreeSet<String>,
    ) -> Result<Self, reqwest::Error> {
        let client = Self(Arc::new(Inner {
            url: url.trim_end_matches('/').into(),
            token,
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(2))
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            queue: Mutex::new(VecDeque::new()),
            flush: Mutex::new(()),
            allowed,
            dropped: AtomicU64::new(0),
            sent: AtomicU64::new(0),
            closed: AtomicBool::new(false),
        }));
        let weak = Arc::downgrade(&client.0);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let Some(inner) = weak.upgrade() else {
                    break;
                };
                if inner.closed.load(Ordering::Relaxed) {
                    break;
                }
                Client(inner).flush().await;
            }
        });
        Ok(client)
    }
    pub async fn record(&self, mut event: Event) -> Result<bool, String> {
        event.validate()?;
        event
            .attributes
            .retain(|key, _| self.0.allowed.contains(key));
        let mut queue = self.0.queue.lock().await;
        if self.0.closed.load(Ordering::Relaxed) || queue.len() >= 1000 {
            self.0.dropped.fetch_add(1, Ordering::Relaxed);
            return Ok(false);
        }
        queue.push_back((event, 0));
        Ok(true)
    }
    pub async fn flush(&self) {
        let _flush = self.0.flush.lock().await;
        let batch: Vec<_> = {
            let mut q = self.0.queue.lock().await;
            let n = q.len().min(100);
            q.drain(..n).collect()
        };
        if batch.is_empty() {
            return;
        }
        let events: Vec<_> = batch.iter().map(|(event, _)| event).collect();
        let response = self
            .0
            .http
            .post(format!("{}/v1/events", self.0.url))
            .bearer_auth(&self.0.token)
            .json(&json!({"events":events}))
            .send()
            .await;
        if response.as_ref().is_ok_and(|r| r.status().is_success()) {
            self.0.sent.fetch_add(batch.len() as u64, Ordering::Relaxed);
            return;
        }
        let permanent = response
            .as_ref()
            .is_ok_and(|r| r.status().is_client_error() && r.status().as_u16() != 429);
        let mut q = self.0.queue.lock().await;
        for (event, attempt) in batch.into_iter().rev() {
            if permanent || attempt >= 4 || q.len() >= 1000 {
                self.0.dropped.fetch_add(1, Ordering::Relaxed);
            } else {
                q.push_front((event, attempt + 1));
            }
        }
    }
    pub async fn evaluate(&self, mut event: Event) -> Decision {
        event.outcome = Outcome::Attempted;
        event
            .attributes
            .retain(|key, _| self.0.allowed.contains(key));
        if event.validate().is_ok()
            && let Ok(response) = self
                .0
                .http
                .post(format!("{}/v1/evaluate", self.0.url))
                .bearer_auth(&self.0.token)
                .json(&event)
                .send()
                .await
            && response.status().is_success()
            && let Ok(decision) = response.json::<Decision>().await
        {
            return decision;
        }
        Decision {
            action: "check".into(),
            recommended_action: "check".into(),
            shadow: false,
            model_id: None,
            surprise: None,
            sequence_mean_surprise: None,
            baseline: None,
            evidence_count: 0,
            reasons: vec!["behavior_service_unavailable".into()],
        }
    }
    /// Drain pending events for at most five seconds plus one in-flight request.
    pub async fn close(&self) {
        self.0.closed.store(true, Ordering::Relaxed);
        let started = std::time::Instant::now();
        while !self.0.queue.lock().await.is_empty() && started.elapsed() < Duration::from_secs(5) {
            self.flush().await;
        }
        let mut queue = self.0.queue.lock().await;
        self.0
            .dropped
            .fetch_add(queue.len() as u64, Ordering::Relaxed);
        queue.clear();
    }
    pub async fn health(&self) -> serde_json::Value {
        json!({"queued":self.0.queue.lock().await.len(),"sent":self.0.sent.load(Ordering::Relaxed),"dropped":self.0.dropped.load(Ordering::Relaxed)})
    }
    /// Wrap an application operation to record its confirmed outcome and duration.
    pub async fn observe<F, T, E>(&self, mut event: Event, operation: F) -> Result<T, E>
    where
        F: std::future::Future<Output = Result<T, E>>,
    {
        let started = std::time::Instant::now();
        let result = operation.await;
        event.outcome = if result.is_ok() {
            Outcome::Succeeded
        } else {
            Outcome::Failed
        };
        event.attributes.insert(
            "duration_ms".into(),
            json!(started.elapsed().as_millis() as u64),
        );
        let _ = self.record(event).await;
        result
    }
}

pub mod recording;
pub use recording::DatasetRecorder;
