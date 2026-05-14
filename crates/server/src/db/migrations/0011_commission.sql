-- realized_pnl_log: 수수료 및 순손익 추가
-- commission: KR 왕복 ~0.21% (거래세+수수료), US 환전 스프레드 양방향
-- net_pnl: pnl - commission (실제 계좌 영향 손익)
ALTER TABLE realized_pnl_log ADD COLUMN commission TEXT NOT NULL DEFAULT '0';
ALTER TABLE realized_pnl_log ADD COLUMN net_pnl TEXT NOT NULL DEFAULT '0';
