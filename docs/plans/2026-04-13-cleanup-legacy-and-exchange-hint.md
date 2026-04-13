# 구현 계획: 레거시 파이프라인 제거 + US 거래소 코드 개선

작성일: 2026-04-13  
대상 레포: `rs_kis_server`

---

## 배경 및 목적

리팩토링 Phase 1-3(Generic Trading Framework) 완료 후 남은 두 가지 후속 작업이다.

1. **Task A**: `pipeline/execution.rs` + `pipeline/position.rs` 레거시 파일 제거
2. **Task B**: `seed_symbols`의 US 거래소 코드 하드코딩(`"NASD"`) 개선

두 작업은 독립적이므로 순서 무관하게 진행 가능하다. 단, **Task A를 먼저** 처리하는 것을 권장한다 (파일 삭제 → 빌드 검증 → Task B 진행).

---

## Task A: 레거시 파이프라인 파일 제거

### 현황 파악

`run.rs`는 이미 Generic 파이프라인만 사용한다.

```
run.rs
  └─ run_generic_execution_task   (generic_execution.rs)  ✅ 현재 실행 경로
  └─ run_generic_position_task    (generic_position.rs)   ✅ 현재 실행 경로
  
  ❌ run_execution_task            (execution.rs)          — 호출되지 않음
  ❌ run_kr_execution_task         (execution.rs)          — 호출되지 않음
  ❌ run_position_task             (position.rs)           — 호출되지 않음
```

**삭제 대상 파일:**
- `crates/server/src/pipeline/execution.rs` (약 600줄)
- `crates/server/src/pipeline/position.rs` (약 700줄)

**연쇄 변경 필요:**
- `crates/server/src/pipeline/mod.rs`: `pub mod execution;` + `pub mod position;` 제거
- `pipeline/position.rs`의 `market_close_utc` 함수가 외부에서 사용되는지 확인 필요

### 사전 조사 (구현 전 확인할 것)

```bash
# 1. 외부에서 execution / position 모듈의 심볼이 참조되는지 확인
grep -r "pipeline::execution\|pipeline::position\|use.*execution\|use.*position" \
  crates/server/src/ --include="*.rs" | grep -v "^Binary\|^target"

# 2. market_close_utc 사용처 확인 (position.rs에서만 정의됨)
grep -r "market_close_utc" crates/server/src/ --include="*.rs"

# 3. run_execution_task / run_kr_execution_task 참조 확인
grep -r "run_execution_task\|run_kr_execution_task\|run_position_task" \
  crates/server/src/ --include="*.rs"
```

예상 결과: `mod.rs`의 `pub mod` 선언 외에는 참조 없음.  
만약 다른 참조가 나오면 해당 코드도 제거 대상에 포함한다.

### 구현 절차

**Step 1: mod.rs에서 모듈 선언 제거**

파일: `crates/server/src/pipeline/mod.rs`

제거 대상 라인:
```rust
pub mod execution;   // ← 제거
pub mod position;    // ← 제거
```

`pub mod generic_execution;`, `pub mod generic_position;`는 유지.

**Step 2: 레거시 파일 삭제**

```bash
rm crates/server/src/pipeline/execution.rs
rm crates/server/src/pipeline/position.rs
```

**Step 3: 빌드 확인**

```bash
cargo build 2>&1
```

오류가 없으면 Step 4로 진행. 오류 시 메시지를 기반으로 잔존 참조를 추가 제거한다.

**Step 4: 테스트 + clippy**

```bash
cargo test
cargo clippy -- -D warnings
cargo fmt --check
```

`position.rs`의 `fill_creates_position_in_db` 테스트가 사라지지만,  
동등한 커버리지가 `generic_position.rs` 내부 테스트에 이미 존재한다.

**Step 5: 커밋**

```
refactor: remove deprecated execution.rs and position.rs pipeline files

Legacy run_execution_task and run_position_task were superseded by
run_generic_execution_task / run_generic_position_task in Phase 1-3
refactoring. run.rs has not called them since commit a88a7d0.
```

---

## Task B: US 거래소 코드 하드코딩 개선

### 현황 파악

**문제 위치:** `crates/server/src/pipeline/signal.rs`, `seed_symbols()` 함수

```rust
// signal.rs:356-359 (현재 코드)
let default_exch = if market_id.is_kr() {
    "J".to_string()
} else {
    "NASD".to_string()   // ← 모든 US 종목을 NASD(NASDAQ)로 가정
};
```

이 값이 `exch_map` → `symbol_exchange` → `SignalContext.exchange_code` → `OrderRequest.exchange_code` 순으로 전파되어 최종적으로 주문 시 거래소를 결정한다.

**결과:** NYSE 상장 종목(예: JPM, IBM, BA)도 NASDAQ으로 주문이 나간다.

**US 어댑터의 현재 거래소 결정 로직 (`market/us.rs`):**

```rust
// exchange_from_hint: metadata.exchange_hint 기반 (UnifiedOrderRequest 경로)
fn exchange_from_hint(hint: Option<&str>, symbol: &str) -> Exchange {
    match hint {
        Some("NYSE")             => Exchange::NYSE,
        Some("AMEX")             => Exchange::AMEX,
        Some("NASD") | Some("NASDAQ") => Exchange::NASD,
        _ => {
            // 힌트 없으면 티커 길이로 추정: ≤3자 → NYSE, 나머지 → NASD
            if symbol.len() <= 3 { Exchange::NYSE } else { Exchange::NASD }
        }
    }
}

// exchange_code_to_exchange: metadata.exchange_code 기반 (기존 OrderRequest 경로)
fn exchange_code_to_exchange(code: Option<&str>) -> Exchange {
    match code {
        Some("NYS") | Some("NYSE") => Exchange::NYSE,
        Some("AMS") | Some("AMEX") => Exchange::AMEX,
        _ => Exchange::NASD,
    }
}
```

`seed_symbols`가 반환하는 `"NASD"` 문자열은 `exchange_code_to_exchange`를 거쳐 항상 `NASD`로 매핑된다. 티커 길이 휴리스틱이 있어도 `exchange_code` 경로에서는 활용되지 않는다.

### 해결 방안 비교

| 방안 | 정확도 | 구현 복잡도 | API 호출 추가 | 권장 |
|------|--------|-------------|--------------|------|
| **A: 티커 길이 휴리스틱** | 중 (≤3→NYSE, 나머지→NASD) | 낮음 | 없음 | ✅ 1차 구현 |
| **B: MarketAdapter::exchange_hint_for** | 높음 | 중간 | 종목별 1회 | 2차 개선 |
| **C: KIS API 시세 조회 시 코드 추출** | 높음 | 높음 | 대량 추가 | 미채택 |

**권장: 방안 A를 즉시 적용, 방안 B를 선택적으로 추가**

### 방안 A: 티커 길이 휴리스틱 적용 (즉시 구현)

**근거:** 현재 `exchange_from_hint`에 이미 동일 로직(`symbol.len() <= 3 → NYSE`)이 있다. `seed_symbols`에서도 같은 기준을 적용하면 별도 API 호출 없이 정확도가 크게 향상된다.

| 티커 | 실제 거래소 | 현재 코드 | 방안 A |
|------|------------|----------|--------|
| JPM  | NYSE | NASD ❌ | NYSE ✅ |
| IBM  | NYSE | NASD ❌ | NYSE ✅ |
| AAPL | NASD | NASD ✅ | NASD ✅ |
| MSFT | NASD | NASD ✅ | NASD ✅ |
| TSLA | NASD | NASD ✅ | NASD ✅ |

**구현 위치:** `crates/server/src/pipeline/signal.rs`, `seed_symbols()` 함수

```rust
// 변경 전
let default_exch = if market_id.is_kr() {
    "J".to_string()
} else {
    "NASD".to_string()
};

// 변경 후
// KR: 기본값 "J" (KOSPI). KOSDAQ 종목은 별도 추론 필요 (현재 범위 외)
// US: 티커 길이 휴리스틱 — ≤3자는 NYSE, 나머지는 NASD (exchange_from_hint와 동일 기준)
// 향후 MarketAdapter::exchange_hint_for(symbol) 도입 시 이 로직을 대체한다.
fn default_us_exchange(symbol: &str) -> &'static str {
    if symbol.len() <= 3 { "NYSE" } else { "NASD" }
}

let default_exch: String = if market_id.is_kr() {
    "J".to_string()
} else {
    default_us_exchange(sym).to_string()
};
```

> **주의:** `default_exch`가 각 심볼마다 결정되어야 하므로, `seed_symbols` 루프 바깥의 `let default_exch = ...` 선언을 루프 안으로 이동하거나 인라인 함수로 분리해야 한다.

**현재 루프 구조 (signal.rs:362–390):**

```rust
// 현재: default_exch를 루프 밖에서 한번만 결정
let default_exch = if market_id.is_kr() { "J" } else { "NASD" };
for sym in wl {
    // ...
    exch_map.insert(sym.clone(), default_exch.clone());
}
```

**변경 후:**

```rust
// 변경: 루프 안에서 sym별로 결정
for sym in wl {
    let exch = if market_id.is_kr() {
        "J"
    } else if sym.len() <= 3 {
        "NYSE"
    } else {
        "NASD"
    };
    // ...
    exch_map.insert(sym.clone(), exch.to_string());
}
```

**테스트 업데이트 필요:**

`signal.rs` 테스트 `test_seed_symbols_writes_to_db_us`에서:
```rust
// 변경 전 (AAPL은 4글자 → NASD 유지이므로 테스트 통과)
assert_eq!(exch_map.get("AAPL").unwrap(), "NASD");  // 유지

// JPM 케이스 추가 (3글자 → NYSE)
// 테스트에 JPM 심볼을 추가하거나 별도 테스트 케이스 작성
```

**구현 절차:**

1. `seed_symbols` 루프 내 `exch` 결정 로직 수정
2. `test_seed_symbols_writes_to_db_us` 테스트: AAPL 케이스 유지 + 3글자 종목(예: JPM) 케이스 추가
3. `cargo test` + `cargo clippy -- -D warnings`

**커밋 메시지:**
```
fix: use ticker-length heuristic for US exchange code in seed_symbols

Hardcoded "NASD" caused NYSE-listed symbols (JPM, IBM, BA, etc.) to be
ordered on NASDAQ. Now symbols ≤3 chars → "NYSE", longer → "NASD",
matching the heuristic already used in UsMarketBase::exchange_from_hint.
```

---

### 방안 B: MarketAdapter::exchange_hint_for 추가 (선택적 2차 개선)

방안 A 적용 후 더 높은 정확도가 필요한 경우에 구현한다.

**핵심 아이디어:** `MarketAdapter` trait에 `exchange_hint_for(symbol) -> String` 메서드를 추가하고, US 어댑터에서 KIS API 또는 정적 테이블로 구현한다.

**trait 변경 위치:** `crates/server/src/market/adapter.rs`

```rust
// MarketAdapter trait에 추가
/// Return the exchange code string for a symbol.
/// KR: "J" (KOSPI) or "Q" (KOSDAQ)
/// US: "NYSE" | "NASD" | "AMEX"
/// Default implementation uses ticker-length heuristic for US, "J" for KR.
fn exchange_hint_for(&self, symbol: &str) -> String {
    if self.market_id().is_kr() {
        "J".to_string()
    } else if symbol.len() <= 3 {
        "NYSE".to_string()
    } else {
        "NASD".to_string()
    }
}
```

**KR 어댑터 선택적 오버라이드 (`market/kr.rs`):**
```rust
// KOSDAQ 종목 판별이 필요하면 오버라이드
fn exchange_hint_for(&self, symbol: &str) -> String {
    // KOSDAQ 종목번호 패턴: 시작이 '2', '3', '4', '6'으로 시작하는 6자리
    // (단순 휴리스틱 — KIS API 조회 방식은 미래 개선 사항)
    if symbol.starts_with('2') || symbol.starts_with('3') {
        "Q".to_string() // KOSDAQ
    } else {
        "J".to_string() // KOSPI
    }
}
```

**seed_symbols 변경:**
```rust
// adapter.exchange_hint_for(sym)으로 교체
exch_map.insert(sym.clone(), adapter.exchange_hint_for(sym));
```

**방안 B 구현 조건:**
- KR KOSDAQ 종목 거래가 필요해진 경우
- US NYSE/AMEX 종목이 워치리스트에 많아진 경우
- 방안 A 휴리스틱으로 커버 안 되는 오류가 실거래에서 발견된 경우

---

## 리뷰 요청 시 체크리스트

구현 완료 후 리뷰 요청 시 아래 항목을 함께 제출한다.

### Task A 체크리스트

- [ ] `cargo build` 통과 (레거시 파일 삭제 후)
- [ ] `cargo test` 통과 — 84개 이상 유지 (position.rs 내 테스트 1개 제거되므로 83개 이상)
- [ ] `cargo clippy -- -D warnings` 클린
- [ ] `cargo fmt --check` 클린
- [ ] `mod.rs`에 `pub mod execution;` / `pub mod position;` 잔존하지 않음
- [ ] `market_close_utc` 함수가 generic_position.rs 등 다른 곳에서 필요한 경우 이전 완료
- [ ] 커밋 메시지에 superseded 커밋 해시(a88a7d0) 명시

### Task B 체크리스트

- [ ] `cargo build` 통과
- [ ] `cargo test` 통과
- [ ] `cargo clippy -- -D warnings` 클린
- [ ] `seed_symbols` 루프에서 `default_exch`를 심볼별로 결정하는 구조로 변경됨
- [ ] US 3글자 이하 종목 → `"NYSE"`, 나머지 → `"NASD"` 반환 확인
- [ ] 테스트: AAPL(`"NASD"`) 케이스 유지 + 3글자 종목 케이스 추가
- [ ] 기존 `test_seed_symbols_writes_to_db_us` assertion 깨지지 않음

---

## 현재 코드 위치 요약 (빠른 참조)

| 항목 | 파일 | 위치 |
|------|------|------|
| 레거시 실행 태스크 | `pipeline/execution.rs` | 전체 파일 (~600줄) |
| 레거시 포지션 태스크 | `pipeline/position.rs` | 전체 파일 (~700줄) |
| 모듈 선언 | `pipeline/mod.rs` | 1-5행 |
| NASD 하드코딩 | `pipeline/signal.rs` | `seed_symbols()` L356-359 |
| 티커 길이 휴리스틱 (참조) | `market/us.rs` | `exchange_from_hint()` L31-43 |
| MarketAdapter trait | `market/adapter.rs` | 전체 파일 |
