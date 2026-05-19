use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::config::RateLimitConfig;

pub fn apply(config: &RateLimitConfig) -> Arc<RateLimiter> {
    Arc::new(RateLimiter::from_config(config))
}

pub struct RateLimiter {
    enabled: bool,
    bytes_per_window: u64,
    window: Duration,
    state: Mutex<HashMap<String, WindowState>>,
}

struct WindowState {
    bytes: u64,
    window_start: Instant,
}

impl RateLimiter {
    fn from_config(config: &RateLimitConfig) -> Self {
        Self {
            enabled: config.enabled,
            bytes_per_window: config.bytes_per_window,
            window: config.window(),
            state: Mutex::new(HashMap::new()),
        }
    }

    pub fn is_allowed(&self, identity: &str) -> bool {
        if !self.enabled {
            return true;
        }
        let state = self.state.lock().expect("rate limiter lock poisoned");
        match state.get(identity) {
            None => true,
            Some(ws) => {
                ws.window_start.elapsed() >= self.window || ws.bytes < self.bytes_per_window
            }
        }
    }

    pub fn record_bytes(&self, identity: &str, bytes: u64) {
        if !self.enabled {
            return;
        }
        let mut state = self.state.lock().expect("rate limiter lock poisoned");
        let now = Instant::now();
        let entry = state
            .entry(identity.to_owned())
            .or_insert_with(|| WindowState { bytes: 0, window_start: now });
        if entry.window_start.elapsed() >= self.window {
            entry.bytes = bytes;
            entry.window_start = now;
        } else {
            entry.bytes = entry.bytes.saturating_add(bytes);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enabled_limiter(limit: u64) -> RateLimiter {
        RateLimiter::from_config(&RateLimitConfig {
            enabled: true,
            bytes_per_window: limit,
            window_secs: Some(60),
        })
    }

    fn disabled_limiter() -> RateLimiter {
        RateLimiter::from_config(&RateLimitConfig::default())
    }

    #[test]
    fn disabled_always_allows() {
        let l = disabled_limiter();
        l.record_bytes("agent-alpha", u64::MAX);
        assert!(l.is_allowed("agent-alpha"));
    }

    #[test]
    fn new_identity_is_allowed() {
        assert!(enabled_limiter(1024).is_allowed("agent-alpha"));
    }

    #[test]
    fn identity_below_limit_is_allowed() {
        let l = enabled_limiter(1024);
        l.record_bytes("agent-alpha", 512);
        assert!(l.is_allowed("agent-alpha"));
    }

    #[test]
    fn identity_at_limit_is_denied() {
        let l = enabled_limiter(1024);
        l.record_bytes("agent-alpha", 1024);
        assert!(!l.is_allowed("agent-alpha"));
    }

    #[test]
    fn multiple_tunnels_accumulate() {
        let l = enabled_limiter(1024);
        l.record_bytes("agent-alpha", 600);
        assert!(l.is_allowed("agent-alpha"));
        l.record_bytes("agent-alpha", 600);
        assert!(!l.is_allowed("agent-alpha"));
    }

    #[test]
    fn different_identities_are_independent() {
        let l = enabled_limiter(1024);
        l.record_bytes("agent-alpha", 2048);
        assert!(!l.is_allowed("agent-alpha"));
        assert!(l.is_allowed("agent-beta"));
    }

    #[test]
    fn expired_window_resets_on_next_record() {
        let l = RateLimiter::from_config(&RateLimitConfig {
            enabled: true,
            bytes_per_window: 1024,
            window_secs: Some(0),
        });
        l.record_bytes("agent-alpha", 2048);
        std::thread::sleep(Duration::from_millis(1));
        assert!(l.is_allowed("agent-alpha"));
    }
}
