//! Executes one opaque job against the Riot API with the node's own key:
//! rate-limited, retrying per class of error, adopting server-declared
//! limit windows from response headers. The node never interprets what it
//! fetches.

use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crawler_proto::{Job, JobOutcome, JobResult};

use crate::ratelimit::{LimiterRegistry, parse_limit_header};

fn header<'a>(r: &'a reqwest::Response, name: &str) -> Option<&'a str> {
    r.headers().get(name).and_then(|v| v.to_str().ok())
}

pub struct Executor {
    http: reqwest::Client,
    api_key: RwLock<String>,
    pub limiters: LimiterRegistry,
    /// Set when Riot rejects the key (401/403); the work loop stops
    /// pulling jobs until the key is updated.
    pub key_bad: AtomicBool,
}

impl Executor {
    pub fn new(api_key: String) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("http client");
        Self {
            http,
            api_key: RwLock::new(api_key),
            limiters: LimiterRegistry::default(),
            key_bad: AtomicBool::new(false),
        }
    }

    pub fn set_api_key(&self, key: String) {
        *self.api_key.write().unwrap() = key;
        self.key_bad.store(false, Ordering::Relaxed);
    }

    /// Runs one job. `on_send` fires once, right before the first wire
    /// attempt — i.e. the moment the job stops queueing on local limiters
    /// and becomes network activity (frontends flip the visualization from
    /// "waiting" to "working" on it).
    pub async fn execute(&self, job: &Job, on_send: impl Fn() + Send) -> JobResult {
        let url = format!("https://{}.api.riotgames.com{}", job.host, job.path);
        // Method limiter first: waiting on a tight method budget must not
        // consume app-budget slots that other endpoints could use.
        let method_limiter = self.limiters.method(&job.host, &job.method);
        let app_limiter = self.limiters.app(&job.host);
        let mut attempts = 0u32;
        let mut announced = false;
        let fail = |e: String| JobResult {
            id: job.id,
            outcome: JobOutcome::Failed,
            body: None,
            error: Some(e),
        };
        loop {
            attempts += 1;
            method_limiter.acquire().await;
            app_limiter.acquire().await;
            if !announced {
                announced = true;
                on_send();
            }
            let api_key = self.api_key.read().unwrap().clone();
            let resp = self.http.get(&url).header("X-Riot-Token", api_key).send().await;
            match resp {
                Ok(r) => {
                    // Adopt the server-declared limit windows (a production
                    // key's budgets apply with no config change).
                    if let Some(h) = header(&r, "X-App-Rate-Limit") {
                        app_limiter.update_specs(parse_limit_header(h)).await;
                    }
                    if let Some(h) = header(&r, "X-Method-Rate-Limit") {
                        method_limiter.update_specs(parse_limit_header(h)).await;
                    }
                    let status = r.status();
                    if status.is_success() {
                        return match r.text().await {
                            Ok(body) => JobResult {
                                id: job.id,
                                outcome: JobOutcome::Ok,
                                body: Some(body),
                                error: None,
                            },
                            Err(e) => fail(format!("body read: {e}")),
                        };
                    }
                    match status.as_u16() {
                        404 => {
                            return JobResult {
                                id: job.id,
                                outcome: JobOutcome::NotFound,
                                body: None,
                                error: None,
                            };
                        }
                        429 => {
                            let retry_after = header(&r, "Retry-After")
                                .and_then(|v| v.parse::<u64>().ok())
                                .unwrap_or(10);
                            // Scope the cooldown to the layer that tripped.
                            if header(&r, "X-Rate-Limit-Type") == Some("method") {
                                method_limiter.cooldown(retry_after).await;
                            } else {
                                app_limiter.cooldown(retry_after).await;
                            }
                            // 429 doesn't count against attempts; the budget is the fix.
                            attempts -= 1;
                        }
                        401 | 403 => {
                            self.key_bad.store(true, Ordering::Relaxed);
                            return JobResult {
                                id: job.id,
                                outcome: JobOutcome::KeyRejected,
                                body: None,
                                error: Some(format!("riot returned {status}")),
                            };
                        }
                        500..=599 => {
                            tracing::warn!(url, status = status.as_u16(), "server error, retrying");
                            tokio::time::sleep(Duration::from_secs(5 * attempts as u64)).await;
                            if attempts >= 4 {
                                return fail(format!("{status} after {attempts} attempts"));
                            }
                        }
                        _ => return fail(format!("unexpected status {status}")),
                    }
                }
                Err(e) => {
                    tracing::warn!(url, error = %e, "request error, retrying");
                    tokio::time::sleep(Duration::from_secs(5 * attempts as u64)).await;
                    if attempts >= 4 {
                        return fail(format!("request error: {e}"));
                    }
                }
            }
        }
    }
}
