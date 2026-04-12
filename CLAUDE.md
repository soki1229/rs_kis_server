# rs_kis_server — Claude 작업 규칙

> **공통 규칙 우선:** `../AGENTS.md` → `AGENTS.md` 순서로 읽고 따른다. 이 파일은 Claude 전용 보충 정보다.

## 역할

**봇 프레임워크 라이브러리.**
인프라(DB, 킬스위치, 주문실행, 모니터링, 파이프라인)와 Strategy trait을 제공한다.
전략 구현체는 포함하지 않으며, `rs_kis_pilot` 같은 바이너리 크레이트에서
`StrategyBundle`을 구현하고 `kis_server::run()`을 호출하여 사용한다.

```
crates/server/src/
  lib.rs             ← 공개 인터페이스
  run.rs             ← run(config, strategies) 진입점; RegimeStrategy/DiscoveryStrategy Arc로 파이프라인에 주입
  strategy.rs        ← StrategyBundle, Discovery/Regime/Signal/Qualification/Risk trait
  types.rs           ← Market, Position, OrderRequest, FillInfo, WatchlistSet 등
  config.rs          ← ServerConfig, MarketConfig, RiskConfig, StrategyProfile 등
  error.rs           ← BotError
  state.rs           ← PipelineConfig, PipelineState, BotState
  regime.rs          ← RegimeInput, regime_channel (classify_regime은 테스트 전용 pub(crate))
  dual_client.rs     ← KR/US 듀얼 토큰 클라이언트 래퍼
  db/                ← SQLite DB (sqlx), 마이그레이션
  control/           ← 킬스위치, 복구
  execution/         ← 주문 실행, OrderState
  monitoring/        ← 알림, 전략 모니터
  position/          ← 포지션 관리
  qualification/     ← setup_score (pub(crate) — 파이프라인 내부 전용)
  pipeline/          ← tick, signal, execution, position, scheduler, regime, tuner, review
  shared/            ← telegram, rest, control, activity
  notion.rs          ← Notion 연동
```

## 관련 레포

```
rs_kis/           ← KIS API 라이브러리 (../rs_kis)
rs_kis_server/    ← 이 저장소 (봇 프레임워크)
rs_kis_pilot/     ← 전략 구현체 (../rs_kis_pilot)
```

---

## 핵심 아키텍처

### Strategy trait 패턴

`strategy.rs`에 5개의 trait이 정의되어 있다:

- `DiscoveryStrategy` — 워치리스트 생성 (async)
- `RegimeStrategy` — 시장 레짐 분류 (sync)
- `SignalStrategy` — 매매 신호 생성 (async)
- `QualificationStrategy` — 신호 검증/필터링 (sync)
- `RiskStrategy` — 포지션 사이징/리스크 관리 (sync)

`StrategyBundle`이 이들을 묶어 `run()`에 전달된다.

**현재 파이프라인 주입 상태:**
- `RegimeStrategy` → `Arc<dyn RegimeStrategy>` 로 `run_generic_regime_task`에 주입됨 ✅
- `DiscoveryStrategy` → `Arc<dyn DiscoveryStrategy>` 로 scheduler 태스크에 주입됨 ✅
- `SignalStrategy`, `QualificationStrategy`, `RiskStrategy` → `pipeline/signal.rs`에 `Arc<dyn Trait>`으로 완전 주입되어 의사결정 위임 완료 ✅

### main.rs

`main.rs`는 플레이스홀더. 실제 실행은 `rs_kis_pilot`에서 한다.

---

## 변경 시 주의사항

### Strategy trait 변경 시

trait 시그니처를 변경하면 `rs_kis_pilot`의 구현체도 업데이트해야 한다.

### types.rs 변경 시

`rs_kis_pilot`에서도 `kis_server::types::*`를 사용하므로 호환성 주의.

### rs_kis 의존성

`Cargo.toml`에서 `kis_api`를 `branch = "master"` 로 참조.
`rs_kis` 변경 시 `cargo update` 로 최신 반영. rev 고정 방식은 사용하지 않는다.

---

## 빌드 및 테스트

```bash
# 빌드
cargo build

# 전체 테스트
cargo test

# 특정 테스트
cargo test "kill_switch"
```

---

## 커밋 메시지 컨벤션

```
feat: 새 기능
fix: 버그 수정
refactor: 동작 변경 없는 코드 정리
test: 테스트 추가/수정
chore: 빌드/설정 변경
```

---

## 컨텍스트 복구 체크리스트

1. `git log --oneline -10` — 최근 변경 파악
2. `cargo test` — 현재 상태 확인
3. `rs_kis_pilot` 빌드 확인 — 인터페이스 호환성
