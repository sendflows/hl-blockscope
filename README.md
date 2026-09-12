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
