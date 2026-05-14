-- orders: max_holding_days (전략에서 설정한 목표 보유 일수)
ALTER TABLE orders ADD COLUMN max_holding_days INTEGER NOT NULL DEFAULT 5;

-- positions: 보유 목표 기간 구분 (swing vs position 투자)
ALTER TABLE positions ADD COLUMN holding_type TEXT NOT NULL DEFAULT 'swing';
ALTER TABLE positions ADD COLUMN max_holding_days INTEGER NOT NULL DEFAULT 5;
