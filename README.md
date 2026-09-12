# hl-blockscope

Terminal view of Hyperliquid blocks as they are produced. HyperCore on the left, HyperEVM on the right, rolling stats at the bottom.

Built for people who need to see what is in a block, not just that a block happened: traders (maker/taker flow, rejects, reverts), block builders (gas, base fee, big-block timing), node operators (block time jitter, proposer share).

## Run

```
cargo run --release            # live TUI, q to quit
cargo run --release -- --once  # one plain-text snapshot, no TUI
```

No config, no keys. Uses the public mainnet endpoints.

## What it shows

**HyperCore** (left)

| column | meaning |
|---|---|
| height, time | block height and HyperCore block time in ms |
| dt | ms since the previous block. `gap` = blocks were skipped between ticks |
| txs | transactions in the block |
| Alo/Gtc/Ioc | orders by time-in-force. Alo = post-only maker, Ioc = taker |
| cxl | cancels |
| evm | `evmRawTx` actions. EVM transactions travel inside HyperCore blocks |
| err | rejected transactions |
| proposer | validator that led the consensus round, resolved to its name |

**HyperEVM small** (right, top) - 1s blocks, 3M gas

| column | meaning |
|---|---|
| dt | seconds since previous block. The chain only exposes 1s granularity |
| gas% | gasUsed / gasLimit |
| gwei | base fee |
| CW l/c/s | calls to CoreWriter 0x3333..., decoded by action id: limit order / cancel / spot send. This is the EVM -> Core write path |
| sys | system transactions. This is the Core -> EVM direction |
| rev | reverted transactions |

**HyperEVM BIG** (right, bottom) - 60s blocks, 30M gas. Big blocks land on the minute, so the title shows a countdown to the next one.

**Rolling** (bottom) - over the last 90 blocks: block time p50/p95/max, orders per second, taker share, cancel/order ratio, error rate, proposer share, EVM utilisation, revert rate, CoreWriter action histogram.

## How it works

One background thread polls, the main thread draws.

HyperCore blocks come from the explorer endpoint (`POST rpc.hyperliquid.xyz/explorer`, type `blockDetails`). HyperCore produces about 14 blocks a second, faster than the explorer can be polled, so the tool fetches 3 consecutive blocks per tick and skips ahead. `dt` is real within a run of consecutive blocks and shows `gap` across a skip.

HyperEVM blocks come from the JSON-RPC endpoint: `eth_getBlockByNumber` for the block and its transactions, `eth_getBlockReceipts` for reverts, `eth_getSystemTxsByBlockNumber` for system transactions. Every EVM block is fetched, none are skipped. Small vs big is decided by `gasLimit`.

Validator names come from the info endpoint (`validatorSummaries`), matched on the signer key. Most validators sign with a key that is not their validator address, so matching on the validator address alone resolves only a few.

## Limits

- HyperCore is sampled, not followed. Rolling Core stats describe the sampled blocks.
- EVM block timestamps are whole seconds. Sub-second EVM cadence is not observable from the public RPC.
- `sys` counts system transactions but does not decode which token was bridged.
- Mainnet only. Change the three endpoint constants at the top of `main.rs` for testnet.

## regulator: execution quality vs CEX

Second binary. Benchmarks every HyperCore perp fill against Binance, Bybit and OKX USDT perps, and measures how often HL's mid is mispriced against them.

```
cargo run --release --bin regulator -- --coin BTC --secs 120 --csv fills.csv
```

Flags: `--coin` (BTC, ETH, SOL, HYPE...), `--secs` window, `--hl-fee-bps` (default 7.0, tier-0 taker), `--cex-fee-bps` (default 2.0, roughly VIP taker), `--csv` per-fill output.

Each fill's cost against the CEX mid is split into two parts, because they mean different things:

| part | clocks | meaning |
|---|---|---|
| effective half-spread | HL only | what the taker paid relative to HL's own mid. Execution quality. |
| basis | HL vs CEX | HL mid minus CEX composite mid, signed by side. A level difference between USDC and USDT perps, not execution. |

The mispricing clock reports the fraction of time HL's mid sits more than 1, 2, 5 bps from the CEX composite, raw and net of a 30s rolling-median basis. The adjusted column is the latency-mispricing figure; the raw one is dominated by the basis.

How the reference is built: for a fill at HL time T, take each venue's latest quote with `exch_ts <= T`, composite mid = median of venue mids, CEX spread = tightest single-venue spread. A max-bid/min-ask composite across venues crosses itself when quotes are a few ms apart and produces negative spreads, so it is not used.

This is a cross-clock join. It holds only while the venues' clocks agree. The tool prints `recv_wall - exch_ts` per feed as evidence; if those drift apart, the reference is wrong. The effective half-spread is HL-vs-HL and does not depend on it.

Sample, BTC, 120s: effective half-spread p50 0.06 bps, CEX tightest half-spread 0.01 bps, HL mid within 1 bps of CEX 98.7% of the time after basis adjustment (mean 0.14 bps). Beat rate on all-in cost is 0% at tier-0 fees: 7 bps taker on HL vs ~2 bps on a CEX VIP tier. The spread is a wash; the fee is the whole difference.

Reference clocks: HL's feed arrives ~300 ms after the CEX feeds. That is HL's speedbump, not clock error, and it is why the join is on exchange time rather than receive time.

The effective half-spread uses the last bbo strictly *before* the fill's timestamp. The bbo stamped with the fill's own millisecond is the post-trade state of that block: a sweep that exhausts a level and rests its remainder moves the touch in the same millisecond, and joining on it puts fills on the wrong side of the mid.

## classifier: who is trading against whom

Third binary. The public `trades` feed carries both counterparties, so every fill can be attributed. Each address is measured by what HL's mid did after its fills.

```
cargo run --release --bin classifier -- --coin BTC --secs 600 --csv fills.csv
```

Flags: `--coin`, `--secs`, `--min-fills` (orders needed to classify, default 5), `--pickoff-bps` (default 0.5), `--top`, `--csv`.

Unit of observation is the **order**, not the fill. A sweep that hits 43 resting orders in one millisecond is one decision with one markout; counting it 43 times inflates every t-stat. Taker observations are grouped by (taker, ms); maker observations by (maker, ms, taker).

| metric | definition |
|---|---|
| markout (taker view) | side × (mid at t+h / pre-trade mid − 1), h = 100 ms, 1 s, 5 s. HL mid vs HL mid, HL's clock. |
| eff / captd | half-spread the taker paid = half-spread the maker captured |
| realised spread | captured − markout: what the maker actually kept at horizon h |
| pickoff | share of a maker's hits where the taker's 1 s markout exceeded the threshold |
| preCEX | CEX composite move in the taker's direction over the 500 ms before the fill. Cross-clock; the latency-arb signature. |

Classes: `twap` if most of an address's orders carry a zero tx hash (HL TWAP sub-orders; they land on an exact 30 s cadence and mark out at ~0); `informed` if mean 1 s markout > 0 with t-stat > 2 over orders; `natural` otherwise; `thin` below `--min-fills`. The volume decomposition crosses taker class with whether the resting side belongs to a repeat maker.

Sample, BTC, 600 s, 655 fills → 357 orders, $3.3M: notional-weighted 1 s markout +0.34 bps against a half-spread of +0.10 bps, so makers' realised spread was −0.24 bps per dollar before fees. One address was informed at t = 3.1 with preCEX +1.0 bps: it hit HL after the CEX composite had already moved 1 bps in its direction. Fills in the last 5 s of a window cannot be marked out and are dropped, not zero-filled.

### k-means toggle

`--kmeans K` replaces the rule with k-means over each address's profile: mean markout at 100 ms / 1 s / 5 s, half-spread paid, pre-fill CEX move, TWAP share. Features are z-scored, seeding is k-means++, best of 10 restarts, deterministic. Clusters are numbered by centroid 1 s markout so `k0` is always the least informed; the centroid table is printed and is how you read them. `--kmeans 0` (default) is the rule.

The two answer different questions. The rule is a falsifiable statement per address ("this taker's 1 s markout is positive at t > 2"). K-means partitions whatever structure is there, whether or not it matches the names you had in mind, and a cluster with three members is not evidence of anything. Use the rule to make claims and k-means to look for classes the rule does not have.
