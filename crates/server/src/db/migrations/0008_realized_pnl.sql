-- realized_pnl_log: 포지션 청산 기록 (실현 손익).
CREATE TABLE IF NOT EXISTS realized_pnl_log (
    id          TEXT PRIMARY KEY,
    symbol      TEXT NOT NULL,
    market      TEXT NOT NULL DEFAULT 'KR',  -- 'KR' | 'US'
    qty         INTEGER NOT NULL,
    entry_price TEXT NOT NULL,
    exit_price  TEXT NOT NULL,
    pnl         TEXT NOT NULL,               -- (exit_price - entry_price) * qty
    pnl_pct     REAL NOT NULL,               -- % 기준
    exited_at   TEXT NOT NULL                -- RFC3339 UTC (날짜 집계: date(exited_at, '+9 hours'))
);

-- portfolio_config: 기준 예수금 등 포트폴리오 설정.
CREATE TABLE IF NOT EXISTS portfolio_config (
    key         TEXT PRIMARY KEY,
    value       TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);
