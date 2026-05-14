-- 청산 사유 기록: StopLoss / TrailingStop / TargetHit / HoldingExpired / Liquidation / Fatal
ALTER TABLE realized_pnl_log ADD COLUMN exit_reason TEXT NOT NULL DEFAULT 'Unknown';
