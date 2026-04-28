-- signal_log: rule_direction 컬럼에 CHECK 제약 추가.
-- 허용값: 'Long' | 'Short' | NULL (신호 미달 시)
-- 이전 바이너리가 '1'/'2'를 저장하는 버그가 있었기 때문에 명시적으로 제약.
-- SQLite는 ALTER TABLE로 CHECK 추가 불가 → 기존 데이터를 유지하며 재생성.
CREATE TABLE IF NOT EXISTS signal_log_new (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol           TEXT NOT NULL,
    setup_score      INTEGER NOT NULL,
    rule_direction   TEXT CHECK(rule_direction IN ('Long', 'Short') OR rule_direction IS NULL),
    rule_strength    REAL,
    llm_verdict      TEXT,
    llm_block_reason TEXT,
    action           TEXT NOT NULL,
    expire_reason    TEXT,
    regime           TEXT NOT NULL,
    logged_at        TEXT NOT NULL
);

INSERT OR IGNORE INTO signal_log_new
    SELECT id, symbol, setup_score,
           CASE
               WHEN rule_direction IN ('Long', 'Short') THEN rule_direction
               ELSE NULL
           END,
           rule_strength, llm_verdict, llm_block_reason,
           action, expire_reason, regime, logged_at
    FROM signal_log;

DROP TABLE signal_log;
ALTER TABLE signal_log_new RENAME TO signal_log;
