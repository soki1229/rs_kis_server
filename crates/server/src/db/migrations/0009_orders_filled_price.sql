-- orders: 실제 체결 가격 컬럼 추가
ALTER TABLE orders ADD COLUMN filled_price TEXT;
