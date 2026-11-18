# PATTERN_ADOPTION

marketdata-kit declares its relationship to the WyattAu pattern canon
(github.com/WyattAu/pattern-standards). Validated by
`pattern-standards/scripts/adoption-check.py`.

The crate ships live market-data adapters (Binance REST klines and
trade WebSocket, Alpaca latest-trades polling) that normalize upstream
payloads into shared types. Hinted P18 (distribution canaries), P91
(data-budget adaptation) and P14 (fixed-topology reductions) have no
mechanism in this codebase: no drift monitoring, no bandwidth
adaptation, no aggregating reductions. All three are omitted.

| Pattern | Status | Evidence |
|---|---|---|
| T1 | adopted | src/binance.rs:52 (kline row cells parsed into a typed Candle, missing cells become Malformed), src/alpaca.rs:47 (trades payload parsed into Quote at the boundary) |
| T13 | adopted | src/binance.rs:161 (parse_trade is an I/O-free transform over one frame), src/binance.rs:68 (klines_refresh owns the client, loop and error shell) |
| P70 | partial | src/binance.rs:163 (accepts both the combined-stream envelope and the raw form), src/binance.rs:59 (missing volume defaults instead of failing) |
| P20 | partial | src/binance.rs:167 and src/alpaca.rs:63 (both live adapters emit feedkit::Quote, the same envelope the platform sim feed uses, so consumers need no live/sim shape branch); replay-clock unification is not implemented |
| P16 | planned | freshness gate on ts_ms before emit; timestamps currently fall back to 0 (src/alpaca.rs:62, src/binance.rs:166), target 2027-Q1 |
| P51 | planned | wire with_breaker on both clients; the breaker dep is declared but no client uses it (src/binance.rs:77, src/alpaca.rs:37; the "breaker-protected" claim at src/binance.rs:67 is not yet true), target 2027-Q1 |
| P107 | planned | Alpaca polling re-emits unchanged latest trades on every poll with no seen-set (src/alpaca.rs:47), target 2027-Q1 |
