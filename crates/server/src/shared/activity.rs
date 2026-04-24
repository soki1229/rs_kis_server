use chrono::{DateTime, Utc};
use std::collections::VecDeque;
use std::sync::{Arc, RwLock};

const MAX_LOG_ENTRIES: usize = 100;
const MAX_EVAL_ENTRIES: usize = 50;

#[derive(Debug, Clone)]
pub struct LogEntry {
    pub ts: DateTime<Utc>,
    pub market: String,
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct EvalEntry {
    pub ts: DateTime<Utc>,
    pub market: String,
    pub symbol: String,
    pub setup_score: i32,
    pub action: String,
    pub reason: String,
}

#[derive(Debug, Clone)]
pub struct PhaseInfo {
    pub phase: String,
    pub since: DateTime<Utc>,
}

#[derive(Debug)]
pub struct ActivityLogInner {
    pub started_at: DateTime<Utc>,
    pub kr_phase: PhaseInfo,
    pub us_phase: PhaseInfo,
    pub kr_watchlist_count: usize,
    pub us_watchlist_count: usize,
    pub kr_watchlist_symbols: Vec<String>,
    pub us_watchlist_symbols: Vec<String>,
    pub kr_ticks_received: u64,
    pub us_ticks_received: u64,
    pub kr_evals_total: u64,
    pub us_evals_total: u64,
    pub kr_orders_sent: u64,
    pub us_orders_sent: u64,
    pub logs: VecDeque<LogEntry>,
    pub evals: VecDeque<EvalEntry>,
    pub kr_last_tick_at: Option<DateTime<Utc>>,
    pub us_last_tick_at: Option<DateTime<Utc>>,
}

#[derive(Clone)]
pub struct ActivityLog(pub Arc<RwLock<ActivityLogInner>>);

impl Default for ActivityLog {
    fn default() -> Self {
        Self::new()
    }
}

impl ActivityLog {
    pub fn new() -> Self {
        let now = Utc::now();
        Self(Arc::new(RwLock::new(ActivityLogInner {
            started_at: now,
            kr_phase: PhaseInfo {
                phase: "Initializing".into(),
                since: now,
            },
            us_phase: PhaseInfo {
                phase: "Initializing".into(),
                since: now,
            },
            kr_watchlist_count: 0,
            us_watchlist_count: 0,
            kr_watchlist_symbols: vec![],
            us_watchlist_symbols: vec![],
            kr_ticks_received: 0,
            us_ticks_received: 0,
            kr_evals_total: 0,
            us_evals_total: 0,
            kr_orders_sent: 0,
            us_orders_sent: 0,
            logs: VecDeque::with_capacity(MAX_LOG_ENTRIES),
            evals: VecDeque::with_capacity(MAX_EVAL_ENTRIES),
            kr_last_tick_at: None,
            us_last_tick_at: None,
        })))
    }

    pub fn set_phase(&self, market: &str, phase: &str) {
        let mut inner = self.0.write().unwrap();
        let info = PhaseInfo {
            phase: phase.to_string(),
            since: Utc::now(),
        };
        match market {
            "KR" => inner.kr_phase = info,
            "US" => inner.us_phase = info,
            _ => {}
        }
        Self::push_log_inner(&mut inner, market, &format!("→ {}", phase));
    }

    #[allow(dead_code)]
    pub fn set_watchlist(&self, market: &str, symbols: &[String]) {
        let mut inner = self.0.write().unwrap();
        match market {
            "KR" => {
                inner.kr_watchlist_count = symbols.len();
                inner.kr_watchlist_symbols = symbols.iter().take(20).cloned().collect();
            }
            "US" => {
                inner.us_watchlist_count = symbols.len();
                inner.us_watchlist_symbols = symbols.iter().take(20).cloned().collect();
            }
            _ => {}
        }
    }

    #[allow(dead_code)]
    pub fn record_tick(&self, market: &str) {
        let mut inner = self.0.write().unwrap();
        let now = Utc::now();
        match market {
            "KR" => {
                inner.kr_ticks_received += 1;
                inner.kr_last_tick_at = Some(now);
            }
            "US" => {
                inner.us_ticks_received += 1;
                inner.us_last_tick_at = Some(now);
            }
            _ => {}
        }
        tracing::info!(target: "kis_server::activity", "Tick recorded for market: {}", market);
    }

    #[allow(dead_code)]
    pub fn record_eval(
        &self,
        market: &str,
        symbol: &str,
        setup_score: i32,
        action: &str,
        reason: &str,
    ) {
        let mut inner = self.0.write().unwrap();
        match market {
            "KR" => inner.kr_evals_total += 1,
            "US" => inner.us_evals_total += 1,
            _ => {}
        }
        if inner.evals.len() >= MAX_EVAL_ENTRIES {
            inner.evals.pop_front();
        }
        inner.evals.push_back(EvalEntry {
            ts: Utc::now(),
            market: market.to_string(),
            symbol: symbol.to_string(),
            setup_score,
            action: action.to_string(),
            reason: reason.to_string(),
        });
    }

    #[allow(dead_code)]
    pub fn record_order(&self, market: &str) {
        let mut inner = self.0.write().unwrap();
        match market {
            "KR" => inner.kr_orders_sent += 1,
            "US" => inner.us_orders_sent += 1,
            _ => {}
        }
    }

    #[allow(dead_code)]
    pub fn log(&self, market: &str, message: &str) {
        let mut inner = self.0.write().unwrap();
        Self::push_log_inner(&mut inner, market, message);
    }

    fn push_log_inner(inner: &mut ActivityLogInner, market: &str, message: &str) {
        if inner.logs.len() >= MAX_LOG_ENTRIES {
            inner.logs.pop_front();
        }
        inner.logs.push_back(LogEntry {
            ts: Utc::now(),
            market: market.to_string(),
            message: message.to_string(),
        });
    }

    pub fn format_status(&self) -> String {
        let inner = self.0.read().unwrap();
        let now = Utc::now();
        let uptime = now - inner.started_at;
        let uptime_str = format_duration(uptime);

        let kr_phase_dur = format_duration(now - inner.kr_phase.since);
        let us_phase_dur = format_duration(now - inner.us_phase.since);

        let kr_last_tick = inner
            .kr_last_tick_at
            .map(|t| format!("{}s ago", (now - t).num_seconds()))
            .unwrap_or_else(|| "never".into());
        let us_last_tick = inner
            .us_last_tick_at
            .map(|t| format!("{}s ago", (now - t).num_seconds()))
            .unwrap_or_else(|| "never".into());

        format!(
            "📊 *봇 상태 요약*\n\
             ⏱ Uptime: {uptime}\n\n\
             🇰🇷 *KR*\n\
             • Phase: {kr_phase} ({kr_dur})\n\
             • Watchlist: {kr_wl}종목\n\
             • Ticks: {kr_ticks} (last: {kr_lt})\n\
             • Evals: {kr_evals} / Orders: {kr_orders}\n\n\
             🇺🇸 *US*\n\
             • Phase: {us_phase} ({us_dur})\n\
             • Watchlist: {us_wl}종목\n\
             • Ticks: {us_ticks} (last: {us_lt})\n\
             • Evals: {us_evals} / Orders: {us_orders}",
            uptime = uptime_str,
            kr_phase = inner.kr_phase.phase,
            kr_dur = kr_phase_dur,
            kr_wl = inner.kr_watchlist_count,
            kr_ticks = inner.kr_ticks_received,
            kr_lt = kr_last_tick,
            kr_evals = inner.kr_evals_total,
            kr_orders = inner.kr_orders_sent,
            us_phase = inner.us_phase.phase,
            us_dur = us_phase_dur,
            us_wl = inner.us_watchlist_count,
            us_ticks = inner.us_ticks_received,
            us_lt = us_last_tick,
            us_evals = inner.us_evals_total,
            us_orders = inner.us_orders_sent,
        )
    }

    pub fn format_watchlist_for_market(&self, market: &str) -> String {
        let inner = self.0.read().unwrap();
        let (total, symbols) = match market {
            "KR" => (inner.kr_watchlist_count, &inner.kr_watchlist_symbols),
            "US" => (inner.us_watchlist_count, &inner.us_watchlist_symbols),
            _ => return format!("[{market}] 알 수 없는 마켓"),
        };
        if symbols.is_empty() {
            return format!("📋 *[{market}] 워치리스트*\n아직 준비 전");
        }
        let mut msg = format!("📋 *[{market}] 워치리스트* ({total}종목)\n\n");
        for sym in symbols.iter() {
            msg.push_str(&format!("`{sym}`  "));
        }
        if total > symbols.len() {
            msg.push_str(&format!("\n_... 외 {}종목_", total - symbols.len()));
        }
        msg
    }

    pub fn format_signals_for_market(&self, market: &str, count: usize) -> String {
        let inner = self.0.read().unwrap();
        let filtered: Vec<&EvalEntry> = inner
            .evals
            .iter()
            .filter(|e| e.market.eq_ignore_ascii_case(market))
            .collect();

        if filtered.is_empty() {
            return format!("[{market}] 오늘 시그널 없음");
        }

        let mut msg = format!("📈 *[{market}] 최근 시그널*\n\n");
        for eval in filtered.iter().rev().take(count) {
            let ts = eval.ts.format("%H:%M:%S");
            msg.push_str(&format!(
                "• `{}` {} score={} → {} {}\n",
                ts, eval.symbol, eval.setup_score, eval.action, eval.reason
            ));
        }
        msg
    }

    pub fn format_log(&self, count: usize) -> String {
        let inner = self.0.read().unwrap();
        if inner.logs.is_empty() && inner.evals.is_empty() {
            return "📋 최근 활동 없음".to_string();
        }

        let mut msg = String::from("📋 *최근 활동 로그*\n\n");

        // Recent log entries
        if !inner.logs.is_empty() {
            msg.push_str("*이벤트:*\n");
            for entry in inner.logs.iter().rev().take(count) {
                let ts = entry.ts.format("%H:%M:%S");
                msg.push_str(&format!(
                    "• `{}` [{}] {}\n",
                    ts, entry.market, entry.message
                ));
            }
        }

        // Recent evaluations
        if !inner.evals.is_empty() {
            msg.push_str("\n*최근 평가:*\n");
            for eval in inner.evals.iter().rev().take(count.min(10)) {
                let ts = eval.ts.format("%H:%M:%S");
                msg.push_str(&format!(
                    "• `{}` [{}] {} score={} → {} {}\n",
                    ts, eval.market, eval.symbol, eval.setup_score, eval.action, eval.reason
                ));
            }
        }

        // Watchlist info
        let kr_syms = &inner.kr_watchlist_symbols;
        let us_syms = &inner.us_watchlist_symbols;
        if !kr_syms.is_empty() || !us_syms.is_empty() {
            msg.push_str("\n*워치리스트:*\n");
            if !kr_syms.is_empty() {
                msg.push_str(&format!(
                    "• KR ({}): {}\n",
                    inner.kr_watchlist_count,
                    kr_syms.join(", ")
                ));
            }
            if !us_syms.is_empty() {
                msg.push_str(&format!(
                    "• US ({}): {}\n",
                    inner.us_watchlist_count,
                    us_syms.join(", ")
                ));
            }
        }

        msg
    }
}

fn format_duration(d: chrono::Duration) -> String {
    let total_secs = d.num_seconds();
    let hours = total_secs / 3600;
    let mins = (total_secs % 3600) / 60;
    let secs = total_secs % 60;
    if hours > 0 {
        format!("{}h {}m", hours, mins)
    } else if mins > 0 {
        format!("{}m {}s", mins, secs)
    } else {
        format!("{}s", secs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn activity_log_basic_operations() {
        let log = ActivityLog::new();
        log.set_phase("KR", "장 대기");
        log.set_watchlist("KR", &["005930".to_string(), "000660".to_string()]);
        log.record_tick("KR");
        log.record_eval("KR", "005930", 72, "skip", "score < threshold");
        log.log("KR", "test message");

        let inner = log.0.read().unwrap();
        assert_eq!(inner.kr_phase.phase, "장 대기");
        assert_eq!(inner.kr_watchlist_count, 2);
        assert_eq!(inner.kr_ticks_received, 1);
        assert_eq!(inner.kr_evals_total, 1);
        assert_eq!(inner.logs.len(), 2); // phase change + test message
        assert_eq!(inner.evals.len(), 1);
    }

    #[test]
    fn format_status_contains_sections() {
        let log = ActivityLog::new();
        log.set_phase("KR", "Trading");
        let status = log.format_status();
        assert!(status.contains("KR"));
        assert!(status.contains("US"));
        assert!(status.contains("Uptime"));
    }

    #[test]
    fn format_log_empty_shows_no_activity() {
        let log = ActivityLog::new();
        let msg = log.format_log(10);
        assert!(msg.contains("최근 활동 없음"));
    }

    #[test]
    fn log_entries_respect_max_capacity() {
        let log = ActivityLog::new();
        for i in 0..150 {
            log.log("KR", &format!("msg {}", i));
        }
        let inner = log.0.read().unwrap();
        assert!(inner.logs.len() <= MAX_LOG_ENTRIES);
    }
}
