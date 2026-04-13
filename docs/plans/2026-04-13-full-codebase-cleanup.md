# rs_kis_server 코드베이스 완전 정리 계획

작성일: 2026-04-13

---

## 전체 구조

5개 Phase로 구성하며, 각 Phase는 독립적으로 커밋·검증 가능하다.  
리스크가 낮은 순서로 배열했으므로 순서대로 진행한다.

| Phase | 내용 | 리스크 | rs_kis_pilot 변경 필요 |
|-------|------|--------|----------------------|
| 1 | `pipeline/regime.rs` 레거시 삭제 | 낮음 | 없음 |
| 2 | `lib.rs` 모듈 가시성 정리 | 낮음 | 없음 |
| 3 | `qualification/setup_score.rs` 제거 | 낮음 | 없음 |
| 4 | `TradeSignal.quantity` 필드 제거 | 중간 | **있음** |
| 5 | 문서 및 어노테이션 정리 | 낮음 | 없음 |

---

## Phase 1: `pipeline/regime.rs` 레거시 삭제

### 현황

`run_regime_task` / `run_kr_regime_task` 두 함수가 `run.rs`에서 전혀 호출되지 않는다.  
`run.rs`는 `pipeline::run_generic_regime_task()`(generic_regime.rs)만 사용한다.  
파일 상단에 이미 DEPRECATED 주석이 없지만, 동일 패턴으로 이미 삭제된 `execution.rs`/`position.rs`와 완전히 같은 상태다.

```
pipeline/
  regime.rs          ← 삭제 대상 (run_regime_task, run_kr_regime_task 미호출)
  generic_regime.rs  ← 현재 실행 경로 (유지)
```

### 사전 확인

```bash
# run.rs 외에 레거시 함수를 참조하는 코드가 없는지 확인
grep -rn "run_regime_task\|run_kr_regime_task\|pipeline::regime::" \
  crates/server/src/ --include="*.rs" | grep -v "regime\.rs"
# 기대 결과: 출력 없음
```

### 구현

**Step 1**: `pipeline/mod.rs`에서 선언 제거

```rust
// 제거할 라인
pub mod regime;
```

**Step 2**: 파일 삭제

```bash
rm crates/server/src/pipeline/regime.rs
```

**Step 3**: 검증

```bash
cargo build
cargo test
cargo clippy -- -D warnings
```

기존 84(현재 73)개 테스트에서 `regime.rs` 내부 테스트가 추가로 제거된다.

### 커밋 메시지

```
refactor: remove deprecated pipeline/regime.rs

run_regime_task and run_kr_regime_task have not been called by run.rs
since the generic pipeline refactoring (a88a7d0). All regime scheduling
is now handled by run_generic_regime_task in generic_regime.rs.

Same pattern as the earlier removal of execution.rs and position.rs.
```

---

## Phase 2: `lib.rs` 모듈 가시성 정리

### 현황

`rs_kis_server`가 라이브러리 크레이트로 사용될 때 `rs_kis_pilot`이 실제로 필요한 공개 API는 4가지뿐이다:

```rust
kis_server::run()
kis_server::StrategyBundle
kis_server::ServerConfig
kis_server::MarketAdapter (+ 4개 어댑터)
```

그러나 현재 `lib.rs`의 많은 모듈이 `pub mod`로 선언되어 있어 내부 구현이 외부에 노출된다.

### 현재 상태 → 목표 상태

```rust
// 현재 lib.rs
pub mod auth;          // → pub(crate)
pub mod config;        // 유지 (ServerConfig pub use)
pub mod control;       // → pub(crate)
pub mod db;            // → pub(crate)
pub mod error;         // → pub(crate)  ※ 아래 주의사항 참고
pub mod execution;     // → pub(crate)
pub mod market;        // 유지 (MarketAdapter 등 pub use)
pub mod monitoring;    // → pub(crate)
pub mod notion;        // → pub(crate)
pub mod pipeline;      // → pub(crate)
pub mod position;      // → pub(crate)
pub(crate) mod qualification;  // 이미 pub(crate) ✅
pub mod regime;        // → pub(crate)
pub mod run;           // 유지 (run() pub use)
pub mod run_generic;   // → pub(crate)
pub mod shared;        // → pub(crate)
pub mod state;         // → pub(crate)
pub mod strategy;      // 유지 (StrategyBundle + 모든 trait 공개 필요)
pub mod types;         // → pub(crate)  ※ 아래 주의사항 참고
```

### 주의사항

**`error.rs`**: `BotError`가 `rs_kis_pilot`에서 직접 사용되는지 확인 필요.

```bash
grep -rn "kis_server::error\|BotError" \
  /Users/soki/dev/ind/KIS/rs_kis_pilot/crates/pilot/src/ --include="*.rs"
```

- 사용 없음 → `pub(crate)`로 변경
- 사용 있음 → `pub mod error;` 유지, `pub use error::BotError;` 추가

**`types.rs`**: `MarketRegime`, `Position`, `Side` 등이 `rs_kis_pilot`에서 사용되는지 확인.

```bash
grep -rn "kis_server::types\|MarketRegime\|kis_server::Side" \
  /Users/soki/dev/ind/KIS/rs_kis_pilot/crates/pilot/src/ --include="*.rs"
```

- `MarketRegime`은 `strategy.rs`를 통해 이미 노출됨 — `types`를 통한 직접 경로가 필요 없으면 `pub(crate)`
- `Side` 등 주문 타입은 `market/types.rs`(UnifiedSide)로 이미 대체됨

### 구현

**lib.rs 수정**: 확인된 결과에 따라 `pub mod` → `pub(crate) mod` 변경

```rust
// 최종 lib.rs (예상)
pub(crate) mod auth;
pub mod config;
pub(crate) mod control;
pub(crate) mod db;
pub(crate) mod error;        // BotError 미사용 시
pub(crate) mod execution;
pub mod market;
pub(crate) mod monitoring;
pub(crate) mod notion;
pub(crate) mod pipeline;
pub(crate) mod position;
pub(crate) mod qualification;
pub(crate) mod regime;
pub mod run;
pub(crate) mod run_generic;
pub(crate) mod shared;
pub(crate) mod state;
pub mod strategy;
pub(crate) mod types;        // MarketRegime 미노출 시
```

### 검증

```bash
# rs_kis_pilot이 컴파일되는지 반드시 확인
cd /Users/soki/dev/ind/KIS/rs_kis_pilot && cargo build
```

컴파일 오류가 나면 해당 타입을 `pub use`로 재노출하거나 모듈을 `pub`으로 되돌린다.

### 커밋 메시지

```
refactor: restrict internal module visibility to pub(crate)

Only modules that form the public API of the library crate remain pub:
- config (ServerConfig)
- market (MarketAdapter + adapters)
- run (run())
- strategy (StrategyBundle + all strategy traits)

All other modules are internal implementation details.
```

---

## Phase 3: `qualification/setup_score.rs` 제거

### 현황

`crates/server/src/qualification/setup_score.rs`의 `SetupScoreInput`과 `calculate_setup_score`는
서버 코드베이스 어디에서도 호출되지 않는다.

```bash
# 확인 명령
grep -rn "calculate_setup_score\|SetupScoreInput" \
  crates/server/src/ --include="*.rs" | grep -v "setup_score\.rs"
# 기대 결과: 출력 없음
```

이 로직은 Phase 2 리팩토링 이전에 파이프라인 내부에서 직접 호출되었으나,  
현재는 `QualificationStrategy` trait 위임으로 대체되어 rs_kis_pilot의 `qualification/setup_score.rs`가 해당 역할을 담당한다.

`lib.rs`에서 `pub(crate) mod qualification`이므로 외부 공개 API에도 없다.

### 구현

**선택 A (권장): 파일 전체 삭제**

```bash
rm crates/server/src/qualification/setup_score.rs
```

`qualification/mod.rs`에서 `pub mod setup_score;` 선언도 함께 제거.

`qualification/` 디렉토리에 다른 파일이 없다면 디렉토리와 `mod.rs` 전체 삭제:

```bash
# 확인
ls crates/server/src/qualification/

# qualification 디렉토리가 비면
rm -r crates/server/src/qualification/
# + lib.rs에서 pub(crate) mod qualification; 제거
```

**선택 B: 유지하되 의도 명시**

나중에 서버 레벨 setup_score 재활용 가능성이 있다면:

```rust
// #[allow(dead_code)] 대신 명시적 주석
/// Setup score calculation — reserved for future server-side use.
/// Currently the pipeline delegates qualification to the injected
/// QualificationStrategy trait (implemented in rs_kis_pilot).
/// See: strategy.rs::QualificationStrategy::qualify()
pub struct SetupScoreInput { ... }
```

### 커밋 메시지

```
refactor: remove unused qualification/setup_score from server crate

The setup score logic was moved to rs_kis_pilot's QualificationStrategy
implementation when Phase 2 refactoring delegated all qualification
decisions to injected strategy traits. The server-side copy has been
dead code (marked #[allow(dead_code)]) since that refactoring.
```

---

## Phase 4: `TradeSignal.quantity` 필드 제거

### 현황

`strategy.rs:75`에 `pub quantity: Decimal` 필드가 있다.  
**이 필드는 파이프라인에서 전혀 사용되지 않는다.**

```
TradeSignal.atr      → OrderRequest.atr → 손절/익절가 계산  ✅ 사용됨
TradeSignal.strength → OrderRequest.strength               ✅ 사용됨
TradeSignal.quantity → ?????                               ❌ 파이프라인 미사용
```

실제 주문 수량은 `RiskStrategy.size(signal, portfolio)` 반환값이 결정한다 (`signal.rs:195`).  
`quantity`는 ATR 대리값으로 쓰이다가 `atr` 필드 도입으로 역할이 종료되었다.

**현재 사용 현황:**
- `signal.rs:504`, `signal.rs:674` — 테스트 픽스처에서만 `quantity: dec!(10)` 설정
- `rs_kis_pilot/signal/mod.rs` — `quantity: Decimal::ZERO`로 설정 (사용하지 않음을 인지)
- `rs_kis_pilot/tests/test_qualification.rs` — `quantity: dec!(0)` 설정

### 영향 범위

**rs_kis_server 변경 필요:**
- `strategy.rs`: `TradeSignal` struct에서 `quantity` 필드 제거
- `pipeline/signal.rs`: 테스트 픽스처 2곳에서 `quantity: ...` 제거
- (다른 곳에서 `quantity` 미사용)

**rs_kis_pilot 변경 필요 (동시 진행):**
- `crates/pilot/src/signal/mod.rs`: `TradeSignal` 생성자에서 `quantity: Decimal::ZERO` 제거
- `crates/pilot/tests/test_qualification.rs`: 모든 `quantity: ...` 제거

### 구현

**rs_kis_server `strategy.rs`:**
```rust
// 제거
pub struct TradeSignal {
    pub symbol: String,
    pub direction: Direction,
    pub strength: f64,
    pub llm_verdict: Option<LlmVerdict>,
    pub entry_price: Decimal,
    // pub quantity: Decimal,  ← 제거
    pub atr: Decimal,
    pub setup_score: Option<i32>,
    pub regime: Option<MarketRegime>,
}
```

**rs_kis_pilot 빌드 오류 처리:**
```bash
# rs_kis_pilot에서 cargo build로 오류 위치 확인 후 quantity 필드 제거
cd /Users/soki/dev/ind/KIS/rs_kis_pilot
cargo build 2>&1 | grep "quantity"
```

### 커밋 메시지 (rs_kis_server)

```
refactor: remove unused TradeSignal.quantity field

This field was originally used as an ATR proxy before the dedicated
`atr: Decimal` field was added (b55967d). The pipeline never reads
quantity — order size comes from RiskStrategy.size() exclusively.
```

### 커밋 메시지 (rs_kis_pilot)

```
fix: remove TradeSignal.quantity field following server update

Field removed from TradeSignal in rs_kis_server. Order sizing is
determined entirely by RiskStrategy.size(), not by this field.
```

---

## Phase 5: 문서 및 어노테이션 정리

### 5-A: `CLAUDE.md` 업데이트

**문제**: `CLAUDE.md`가 삭제된 `dual_client.rs`를 여전히 참조한다.

```markdown
# 현재 (잘못된 내용)
  dual_client.rs     ← KR/US 듀얼 토큰 클라이언트 래퍼

# 교체할 내용
  run_generic.rs     ← MarketAdapters 조합 (KrReal/KrVts/UsReal/UsVts 생성)
```

또한 Phase 1-4 완료 후 파일 목록과 아키텍처 섹션을 실제 상태와 맞춰 업데이트한다.

### 5-B: `config.rs` `#[allow(dead_code)]` 제거

**문제**: `MarketConfig`와 여러 config 구조체에 `#[allow(dead_code)]`가 달려있으나,
이 필드들은 TOML 역직렬화(`serde::Deserialize`)에서 사용된다. dead code가 아니다.

```rust
// 변경 전
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct MarketConfig { ... }

// 변경 후 — allow(dead_code) 제거 (serde가 사용하므로 dead code 경고 없음)
#[derive(Debug, Clone, Deserialize)]
pub struct MarketConfig { ... }
```

**영향 범위**: `config.rs`의 모든 `#[allow(dead_code)]` 어노테이션.  
제거 후 `cargo clippy -- -D warnings`로 실제 dead code 경고가 나오는지 확인한다.  
경고 없으면 어노테이션이 불필요했던 것이 확인된 것.

### 5-C: `market/us.rs` TODO 주석 처리

**위치**: `crates/server/src/market/us.rs:257`

```rust
// TODO: US-VTS may also need a balance fallback if it proves unstable like Domestic VTS.
```

이 TODO는 실거래 운영 경험이 쌓인 후에야 판단 가능하다.  
Linear 이슈로 등록하거나, 현재로서는 주석 유지가 적절하다.

### 커밋 메시지

```
chore: update CLAUDE.md and remove spurious #[allow(dead_code)] annotations

- CLAUDE.md: replace dual_client.rs with run_generic.rs (MarketAdapters)
  and reflect post-cleanup file structure
- config.rs: remove #[allow(dead_code)] from serde-deserialized config
  structs (serde Deserialize is a user of these fields)
```

---

## 전체 진행 순서 요약

```
Phase 1: pipeline/regime.rs 삭제
  └─ cargo test 통과 확인

Phase 2: lib.rs pub → pub(crate) 변경
  ├─ rs_kis_pilot cargo build로 공개 API 호환성 확인
  └─ 필요 시 pub use로 재노출

Phase 3: qualification/setup_score.rs 제거
  └─ cargo test 통과 확인

Phase 4: TradeSignal.quantity 제거 (rs_kis_server + rs_kis_pilot 동시)
  ├─ rs_kis_server 변경 후 rs_kis_pilot cargo update + 컴파일 오류 수정
  └─ 양쪽 모두 cargo test 통과 확인

Phase 5: CLAUDE.md + config.rs 어노테이션 정리
  └─ cargo clippy -- -D warnings 통과 확인
```

---

## 리뷰 요청 시 체크리스트 (전체)

- [ ] Phase 1: `cargo test` 통과 (73개 이상)
- [ ] Phase 2: `rs_kis_pilot cargo build` 오류 없음
- [ ] Phase 3: `qualification/` 디렉토리 완전 제거 또는 `setup_score.rs` 단독 제거
- [ ] Phase 4: `TradeSignal`에 `quantity` 필드 없음, 양쪽 레포 모두 테스트 통과
- [ ] Phase 5: `CLAUDE.md`에 `dual_client.rs` 없음, `cargo clippy` 클린
- [ ] 전체: `cargo test && cargo clippy -- -D warnings && cargo fmt --check` 완전 통과
