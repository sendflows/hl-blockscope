//! Live block-level view of Hyperliquid for traders, builders and node operators.
//!
//!   HyperCore   consecutive blocks: dt ms, orders by TIF (maker/taker flow), cancels,
//!               embedded evmRawTx, errors, proposer
//!   EVM small   1s / 3M gas: gas%, base fee, CoreWriter calls decoded by action id,
//!               system txs (Core->EVM), reverts, Core blocks spanned (dL1)
//!   EVM big     60s / 30M gas: land on the minute; countdown to the next one
//!   rolling     p50/p95 block time, taker share, proposer share, CW histogram
//!
//! `--once` prints a plain-text snapshot. `q` quits the TUI.

use std::{
    collections::{HashMap, VecDeque},
    sync::mpsc,
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crossterm::event::{self, Event, KeyCode};
use ratatui::{prelude::*, widgets::*};
use serde_json::{json, Value};

const EVM: &str = "https://rpc.hyperliquid.xyz/evm";
const EXPLORER: &str = "https://rpc.hyperliquid.xyz/explorer";
const INFO: &str = "https://api.hyperliquid.xyz/info";
const COREWRITER: &str = "0x3333333333333333333333333333333333333333";
const CAP: usize = 90;
const CORE_PER_TICK: u64 = 3;

// CoreWriter action ids (docs: interacting-with-hypercore). Index into Evm.cw.
const CW_NAMES: [&str; 6] = ["limit", "cancel", "spotSend", "usdClass", "sendAsset", "other"];

#[derive(Clone)]
struct Evm {
    num: u64,
    ts: u64,
    big: bool,
    gas_used: u64,
    gas_limit: u64,
    base_fee: u64,
    txs: usize,
    cw: [usize; 6],
    sys: usize,
    reverted: usize,
}

#[derive(Clone)]
struct Core {
    height: u64,
    ms: u64,
    txs: u64,
    orders: usize,
    alo: usize,
    gtc: usize,
    ioc: usize,
    cancels: usize,
    evm_raw: usize,
    errs: usize,
    proposer: String,
}

enum Msg {
    Evm(Evm),
    Core(Core),
    Validators(HashMap<String, String>),
    Err(String),
}

/// Block `proposer` is the validator's SIGNER key, which for 28 of 35 mainnet
/// validators differs from the `validator` address. Map both, signer first.
fn fetch_validators(c: &Client) -> HashMap<String, String> {
    let mut m = HashMap::new();
    if let Some(vs) = post(c, INFO, json!({"type":"validatorSummaries"})).and_then(|v| v.as_array().cloned()) {
        for v in vs {
            let name = v.get("name").and_then(Value::as_str).unwrap_or("?").to_string();
            for k in ["signer", "validator"] {
                if let Some(a) = v.get(k).and_then(Value::as_str) {
                    m.entry(a.to_lowercase()).or_insert_with(|| name.clone());
                }
            }
        }
    }
    m
}

type Client = reqwest::blocking::Client;

fn hex(v: Option<&Value>) -> u64 {
    v.and_then(Value::as_str)
        .and_then(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16).ok())
        .unwrap_or(0)
}

fn post(c: &Client, url: &str, body: Value) -> Option<Value> {
    c.post(url).json(&body).send().ok()?.json().ok()
}

fn rpc(c: &Client, method: &str, params: Value) -> Option<Value> {
    post(c, EVM, json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))?.get("result").cloned()
}

fn cw_slot(input: &str) -> usize {
    // sendRawAction(bytes): selector(4) | offset(32) | len(32) | version(1) | action id (3)
    let id = input.get(140..146).and_then(|h| u32::from_str_radix(h, 16).ok()).unwrap_or(0);
    match id {
        1 => 0,
        10 | 11 => 1,
        6 => 2,
        7 => 3,
        13 => 4,
        _ => 5,
    }
}

fn fetch_evm(c: &Client, num: u64) -> Option<Evm> {
    let tag = format!("0x{num:x}");
    let b = rpc(c, "eth_getBlockByNumber", json!([tag, true]))?;
    let txs = b.get("transactions")?.as_array()?;
    let mut cw = [0usize; 6];
    for t in txs {
        if t.get("to").and_then(Value::as_str).is_some_and(|a| a.eq_ignore_ascii_case(COREWRITER)) {
            cw[cw_slot(t.get("input").and_then(Value::as_str).unwrap_or(""))] += 1;
        }
    }
    let sys = rpc(c, "eth_getSystemTxsByBlockNumber", json!([tag]))
        .and_then(|v| v.as_array().map(Vec::len))
        .unwrap_or(0);
    let reverted = rpc(c, "eth_getBlockReceipts", json!([tag]))
        .and_then(|v| v.as_array().map(|r| r.iter().filter(|x| hex(x.get("status")) == 0).count()))
        .unwrap_or(0);
    // NOTE: reading the l1BlockNumber precompile with a historical block tag
    // does NOT work on the public RPC - eth_call only serves latest state, so
    // it returns the current head regardless of tag. Do not add it back.
    let gas_limit = hex(b.get("gasLimit"));
    Some(Evm {
        num,
        ts: hex(b.get("timestamp")),
        big: gas_limit > 10_000_000,
        gas_used: hex(b.get("gasUsed")),
        gas_limit,
        base_fee: hex(b.get("baseFeePerGas")),
        txs: txs.len(),
        cw,
        sys,
        reverted,
    })
}

fn fetch_core(c: &Client, height: u64) -> Option<Core> {
    let d = post(c, EXPLORER, json!({"type":"blockDetails","height":height}))?.get("blockDetails")?.clone();
    let txs = d.get("txs").and_then(Value::as_array).cloned().unwrap_or_default();
    let mut k = Core {
        height: d.get("height").and_then(Value::as_u64).unwrap_or(height),
        ms: d.get("blockTime").and_then(Value::as_u64).unwrap_or(0),
        txs: d.get("numTxs").and_then(Value::as_u64).unwrap_or(txs.len() as u64),
        orders: 0,
        alo: 0,
        gtc: 0,
        ioc: 0,
        cancels: 0,
        evm_raw: 0,
        errs: 0,
        proposer: d.get("proposer").and_then(Value::as_str).unwrap_or("?").to_lowercase(),
    };
    for t in &txs {
        if t.get("error").is_some_and(|e| !e.is_null()) {
            k.errs += 1;
        }
        let a = t.get("action").cloned().unwrap_or_default();
        match a.get("type").and_then(Value::as_str).unwrap_or("") {
            "order" => {
                for o in a.get("orders").and_then(Value::as_array).into_iter().flatten() {
                    k.orders += 1;
                    match o.pointer("/t/limit/tif").and_then(Value::as_str) {
                        Some("Alo") => k.alo += 1,
                        Some("Gtc") => k.gtc += 1,
                        Some("Ioc") => k.ioc += 1,
                        _ => {}
                    }
                }
            }
            "cancel" | "cancelByCloid" => {
                k.cancels += a.get("cancels").and_then(Value::as_array).map_or(1, Vec::len)
            }
            "evmRawTx" => k.evm_raw += 1,
            _ => {}
        }
    }
    Some(k)
}

fn poller(tx: mpsc::Sender<Msg>) {
    let c = Client::builder().timeout(Duration::from_secs(12)).build().unwrap();
    let (mut last_evm, mut last_core) = (0u64, 0u64);
    let _ = tx.send(Msg::Validators(fetch_validators(&c)));
    loop {
        match rpc(&c, "eth_blockNumber", json!([])) {
            Some(v) => {
                let head = hex(Some(&v));
                let from = if last_evm == 0 { head.saturating_sub(2) } else { last_evm + 1 };
                for n in from..=head {
                    if let Some(b) = fetch_evm(&c, n) {
                        let _ = tx.send(Msg::Evm(b));
                    }
                }
                last_evm = head;
            }
            None => {
                let _ = tx.send(Msg::Err("evm rpc unreachable".into()));
            }
        }
        // Core produces ~14 blocks/s; we fetch CORE_PER_TICK consecutive ones per
        // tick so dt is real, and skip ahead between ticks (gaps are shown).
        let head = hex(rpc(
            &c,
            "eth_call",
            json!([{"to":"0x0000000000000000000000000000000000000809","data":"0x"}, "latest"]),
        )
        .as_ref());
        if head > 0 {
            let from = if last_core == 0 { head - CORE_PER_TICK + 1 } else { (last_core + 1).max(head - CORE_PER_TICK + 1) };
            for h in from..=head {
                match fetch_core(&c, h) {
                    Some(b) => {
                        let _ = tx.send(Msg::Core(b));
                    }
                    None => {
                        let _ = tx.send(Msg::Err("core explorer unreachable".into()));
                    }
                }
            }
            last_core = head;
        }
        thread::sleep(Duration::from_millis(700));
    }
}

fn clock(ms: u64) -> String {
    let s = ms / 1000 % 86_400;
    format!("{:02}:{:02}:{:02}.{:03}", s / 3600, s % 3600 / 60, s % 60, ms % 1000)
}

fn pct(a: u64, b: u64) -> f64 {
    if b == 0 { 0.0 } else { a as f64 / b as f64 * 100.0 }
}

fn now_s() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

fn percentile(v: &mut [f64], p: f64) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[((v.len() - 1) as f64 * p).round() as usize]
}

/// Aggregate view over the retained window. Everything traders/builders/nodes
/// ask about that is not a single-block property.
struct Rolling {
    core_dt_p50: f64,
    core_dt_p95: f64,
    core_dt_max: f64,
    orders_per_s: f64,
    taker_share: f64,
    cancel_ratio: f64,
    err_rate: f64,
    proposers: Vec<(String, usize)>,
    evm_util: f64,
    evm_dts_p50: f64,
    base_fee_gwei: f64,
    revert_rate: f64,
    cw: [usize; 6],
    sys: usize,
    big_util: f64,
    next_big_s: u64,
}

fn rolling(core: &VecDeque<Core>, evm: &VecDeque<Evm>) -> Rolling {
    let mut dts: Vec<f64> = Vec::new();
    let mut span_ms = 0u64;
    for w in core.iter().collect::<Vec<_>>().windows(2) {
        if w[0].height == w[1].height + 1 && w[0].ms >= w[1].ms {
            dts.push((w[0].ms - w[1].ms) as f64);
            span_ms += w[0].ms - w[1].ms;
        }
    }
    let orders: usize = core.iter().map(|c| c.orders).sum();
    let ioc: usize = core.iter().map(|c| c.ioc).sum();
    let cancels: usize = core.iter().map(|c| c.cancels).sum();
    let txs: u64 = core.iter().map(|c| c.txs).sum();
    let errs: usize = core.iter().map(|c| c.errs).sum();
    let mut pm: HashMap<&str, usize> = HashMap::new();
    for c in core {
        *pm.entry(&c.proposer).or_default() += 1;
    }
    let mut proposers: Vec<(String, usize)> = pm.into_iter().map(|(k, v)| (k.to_string(), v)).collect();
    proposers.sort_by_key(|(_, v)| std::cmp::Reverse(*v));
    proposers.truncate(3);

    let small: Vec<&Evm> = evm.iter().filter(|b| !b.big).collect();
    let bigs: Vec<&Evm> = evm.iter().filter(|b| b.big).collect();
    let mut evm_dts: Vec<f64> = small.windows(2).filter(|w| w[0].ts >= w[1].ts).map(|w| (w[0].ts - w[1].ts) as f64).collect();
    let mut cw = [0usize; 6];
    for b in evm {
        for i in 0..6 {
            cw[i] += b.cw[i];
        }
    }
    let n_small = small.len().max(1) as f64;
    Rolling {
        core_dt_p50: percentile(&mut dts.clone(), 0.5),
        core_dt_p95: percentile(&mut dts.clone(), 0.95),
        core_dt_max: dts.iter().cloned().fold(0.0, f64::max),
        orders_per_s: if span_ms > 0 { orders as f64 / (span_ms as f64 / 1000.0) } else { 0.0 },
        taker_share: if orders > 0 { ioc as f64 / orders as f64 * 100.0 } else { 0.0 },
        cancel_ratio: if orders > 0 { cancels as f64 / orders as f64 } else { 0.0 },
        err_rate: if txs > 0 { errs as f64 / txs as f64 * 100.0 } else { 0.0 },
        proposers,
        evm_util: small.iter().map(|b| pct(b.gas_used, b.gas_limit)).sum::<f64>() / n_small,
        evm_dts_p50: percentile(&mut evm_dts, 0.5),
        base_fee_gwei: small.first().map_or(0.0, |b| b.base_fee as f64 / 1e9),
        revert_rate: {
            let t: usize = small.iter().map(|b| b.txs).sum();
            let r: usize = small.iter().map(|b| b.reverted).sum();
            if t > 0 { r as f64 / t as f64 * 100.0 } else { 0.0 }
        },
        cw,
        sys: evm.iter().map(|b| b.sys).sum(),
        big_util: bigs.first().map_or(0.0, |b| pct(b.gas_used, b.gas_limit)),
        next_big_s: 60 - now_s() % 60,
    }
}

fn once() {
    let c = Client::builder().timeout(Duration::from_secs(15)).build().unwrap();
    let head = hex(rpc(&c, "eth_blockNumber", json!([])).as_ref());
    let mut evm: VecDeque<Evm> = VecDeque::new();
    for n in (head.saturating_sub(5)..=head).rev() {
        if let Some(b) = fetch_evm(&c, n) {
            evm.push_back(b);
        }
    }
    let h = hex(rpc(&c, "eth_call", json!([{"to":"0x0000000000000000000000000000000000000809","data":"0x"}, "latest"])).as_ref());
    let names = fetch_validators(&c);
    let mut core: VecDeque<Core> = VecDeque::new();
    for height in (h.saturating_sub(5)..=h).rev() {
        if let Some(b) = fetch_core(&c, height) {
            core.push_back(b);
        }
    }
    println!("HyperEVM   block     kind  dt s  txs  gas%    base   CW l/c/s/u/a/o   sys  rev");
    for (i, b) in evm.iter().enumerate() {
        let dts = evm.get(i + 1).map_or("-".into(), |p| (b.ts.saturating_sub(p.ts)).to_string());
        println!(
            "           {:<9} {:<5} {:>4}  {:>3}  {:>5.1}  {:>5.2}g  {}/{}/{}/{}/{}/{}      {:>3}  {:>3}",
            b.num, if b.big { "BIG" } else { "sml" }, dts, b.txs, pct(b.gas_used, b.gas_limit),
            b.base_fee as f64 / 1e9, b.cw[0], b.cw[1], b.cw[2], b.cw[3], b.cw[4], b.cw[5], b.sys, b.reverted
        );
    }
    println!("\nHyperCore  height       time (UTC)     dt ms  txs  orders  Alo/Gtc/Ioc     cancels  evmRaw  err  proposer");
    for (i, b) in core.iter().enumerate() {
        let dt = core.get(i + 1).map_or("-".into(), |p| (b.ms.saturating_sub(p.ms)).to_string());
        println!(
            "           {:<12} {}  {:>5}  {:>4}  {:>5}   {:>4}/{:<4}/{:<4}  {:>6}  {:>5}  {:>3}  {}",
            b.height, clock(b.ms), dt, b.txs, b.orders, b.alo, b.gtc, b.ioc, b.cancels, b.evm_raw, b.errs,
            names.get(&b.proposer).cloned().unwrap_or_else(|| b.proposer.chars().take(10).collect())
        );
    }
    let r = rolling(&core, &evm);
    println!(
        "\nrolling    core dt p50/p95/max {:.0}/{:.0}/{:.0} ms | {:.0} orders/s | taker(Ioc) {:.1}% | cancel/order {:.2} | err {:.2}%",
        r.core_dt_p50, r.core_dt_p95, r.core_dt_max, r.orders_per_s, r.taker_share, r.cancel_ratio, r.err_rate
    );
    println!(
        "           evm small util {:.1}% | dt p50 {:.0} s (1s-granular) | base {:.2} gwei | revert {:.1}% | next BIG in {}s",
        r.evm_util, r.evm_dts_p50, r.base_fee_gwei, r.revert_rate, r.next_big_s
    );
}

fn dim(on: bool, c: Color) -> Style {
    if on { Style::new().fg(c).bold() } else { Style::new().fg(Color::DarkGray) }
}

fn main() -> std::io::Result<()> {
    if std::env::args().any(|a| a == "--once") {
        once();
        return Ok(());
    }
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || poller(tx));

    let mut term = ratatui::init();
    let mut evm: VecDeque<Evm> = VecDeque::new();
    let mut core: VecDeque<Core> = VecDeque::new();
    let mut err = String::new();
    let mut names: HashMap<String, String> = HashMap::new();

    loop {
        while let Ok(m) = rx.try_recv() {
            match m {
                Msg::Evm(b) => {
                    evm.push_front(b);
                    evm.truncate(CAP);
                    err.clear();
                }
                Msg::Core(b) => {
                    if core.front().map(|f| f.height) != Some(b.height) {
                        core.push_front(b);
                        core.truncate(CAP);
                    }
                    err.clear();
                }
                Msg::Validators(v) => names = v,
                Msg::Err(e) => err = e,
            }
        }
        let r = rolling(&core, &evm);
        let label = |a: &str| names.get(a).cloned().unwrap_or_else(|| a.chars().take(10).collect());

        term.draw(|f| {
            let v = Layout::vertical([Constraint::Min(0), Constraint::Length(7)]).split(f.area());
            let h = Layout::horizontal([Constraint::Percentage(52), Constraint::Percentage(48)]).split(v[0]);
            let right = Layout::vertical([Constraint::Percentage(65), Constraint::Percentage(35)]).split(h[1]);

            // ---- HyperCore ----
            let core_vec: Vec<&Core> = core.iter().collect();
            let rows: Vec<Row> = core_vec
                .iter()
                .enumerate()
                .map(|(i, b)| {
                    let (dt, gap) = match core_vec.get(i + 1) {
                        Some(p) if p.height + 1 == b.height => ((b.ms.saturating_sub(p.ms)).to_string(), false),
                        Some(_) => ("gap".into(), true),
                        None => ("-".into(), false),
                    };
                    Row::new(vec![
                        Cell::from(b.height.to_string()).style(Style::new().fg(Color::Cyan)),
                        Cell::from(clock(b.ms)).style(Style::new().fg(Color::DarkGray)),
                        Cell::from(dt).style(if gap { Style::new().fg(Color::DarkGray) } else { Style::new().fg(Color::White) }),
                        Cell::from(b.txs.to_string()).style(Style::new().fg(Color::Yellow)),
                        Cell::from(format!("{:>3}/{:<3}/{:<3}", b.alo, b.gtc, b.ioc)),
                        Cell::from(b.cancels.to_string()),
                        Cell::from(b.evm_raw.to_string()).style(dim(b.evm_raw > 0, Color::Magenta)),
                        Cell::from(b.errs.to_string()).style(dim(b.errs > 0, Color::Red)),
                        Cell::from(label(&b.proposer).chars().take(16).collect::<String>()).style(Style::new().fg(Color::DarkGray)),
                    ])
                })
                .collect();
            f.render_widget(
                Table::new(rows, [
                    Constraint::Length(11), Constraint::Length(12), Constraint::Length(5), Constraint::Length(5),
                    Constraint::Length(12), Constraint::Length(5), Constraint::Length(4), Constraint::Length(4), Constraint::Length(16),
                ])
                .header(Row::new(vec!["height", "time (UTC)", "dt", "txs", "Alo/Gtc/Ioc", "cxl", "evm", "err", "proposer"]).style(Style::new().fg(Color::Green).bold()))
                .block(Block::bordered().title(format!(" HyperCore  {} consecutive/tick ", CORE_PER_TICK)).border_style(Style::new().fg(Color::Green))),
                h[0],
            );

            // ---- EVM small ----
            let small: Vec<&Evm> = evm.iter().filter(|b| !b.big).collect();
            let rows: Vec<Row> = small
                .iter()
                .enumerate()
                .map(|(i, b)| {
                    let dts = small.get(i + 1).map_or("-".into(), |p| b.ts.saturating_sub(p.ts).to_string());
                    let cwn: usize = b.cw.iter().sum();
                    Row::new(vec![
                        Cell::from(b.num.to_string()).style(Style::new().fg(Color::Magenta)),
                        Cell::from(dts).style(Style::new().fg(Color::DarkGray)),
                        Cell::from(b.txs.to_string()).style(Style::new().fg(Color::Yellow)),
                        Cell::from(format!("{:>5.1}", pct(b.gas_used, b.gas_limit))),
                        Cell::from(format!("{:.2}", b.base_fee as f64 / 1e9)),
                        Cell::from(format!("{} {}/{}/{}", cwn, b.cw[0], b.cw[1], b.cw[2])).style(dim(cwn > 0, Color::LightCyan)),
                        Cell::from(b.sys.to_string()).style(dim(b.sys > 0, Color::LightGreen)),
                        Cell::from(b.reverted.to_string()).style(dim(b.reverted > 0, Color::Red)),
                    ])
                })
                .collect();
            f.render_widget(
                Table::new(rows, [
                    Constraint::Length(9), Constraint::Length(4), Constraint::Length(4), Constraint::Length(6),
                    Constraint::Length(5), Constraint::Length(11), Constraint::Length(4), Constraint::Length(4),
                ])
                .header(Row::new(vec!["block", "dt", "txs", "gas%", "gwei", "CW l/c/s", "sys", "rev"]).style(Style::new().fg(Color::Magenta).bold()))
                .block(Block::bordered().title(" HyperEVM small  1s / 3M gas ").border_style(Style::new().fg(Color::Magenta))),
                right[0],
            );

            // ---- EVM big ----
            let bigs: Vec<&Evm> = evm.iter().filter(|b| b.big).collect();
            let rows: Vec<Row> = bigs
                .iter()
                .map(|b| {
                    let cwn: usize = b.cw.iter().sum();
                    Row::new(vec![
                        Cell::from(b.num.to_string()).style(Style::new().fg(Color::LightRed)),
                        Cell::from(clock(b.ts * 1000)),
                        Cell::from(b.txs.to_string()).style(Style::new().fg(Color::Yellow)),
                        Cell::from(format!("{:>5.1}", pct(b.gas_used, b.gas_limit))),
                        Cell::from(format!("{}", b.gas_used / 1000)),
                        Cell::from(cwn.to_string()).style(dim(cwn > 0, Color::LightCyan)),
                        Cell::from(b.reverted.to_string()).style(dim(b.reverted > 0, Color::Red)),
                    ])
                })
                .collect();
            f.render_widget(
                Table::new(rows, [
                    Constraint::Length(9), Constraint::Length(12), Constraint::Length(4), Constraint::Length(6),
                    Constraint::Length(7), Constraint::Length(3), Constraint::Length(4),
                ])
                .header(Row::new(vec!["block", "time (UTC)", "txs", "gas%", "kgas", "CW", "rev"]).style(Style::new().fg(Color::LightRed).bold()))
                .block(Block::bordered().title(format!(" HyperEVM BIG  60s / 30M gas  -  next in {:>2}s ", r.next_big_s)).border_style(Style::new().fg(Color::LightRed))),
                right[1],
            );

            // ---- rolling ----
            let props = r.proposers.iter().map(|(p, n)| format!("{} {:.0}%", label(p), *n as f64 / core.len().max(1) as f64 * 100.0)).collect::<Vec<_>>().join("  ");
            let cwh = CW_NAMES.iter().zip(r.cw.iter()).filter(|(_, n)| **n > 0).map(|(k, n)| format!("{k}:{n}")).collect::<Vec<_>>().join(" ");
            let text = vec![
                Line::from(format!(
                    "core   dt p50/p95/max {:.0}/{:.0}/{:.0} ms   {:.0} orders/s   taker(Ioc) {:.1}%   cancel/order {:.2}   err {:.2}%   n={}",
                    r.core_dt_p50, r.core_dt_p95, r.core_dt_max, r.orders_per_s, r.taker_share, r.cancel_ratio, r.err_rate, core.len()
                )),
                Line::from(format!("       proposers  {props}")),
                Line::from(format!(
                    "evm    small util {:.1}%   dt p50 {:.0}s (ts is 1s-granular)   base {:.2} gwei   revert {:.1}%   BIG util {:.1}%   n={}",
                    r.evm_util, r.evm_dts_p50, r.base_fee_gwei, r.revert_rate, r.big_util, evm.len()
                )),
                Line::from(format!("       CoreWriter {}   |   Core->EVM system txs {}", if cwh.is_empty() { "none".into() } else { cwh }, r.sys)),
                Line::from(if err.is_empty() { "q quit".to_string() } else { format!("error: {err}") }).style(Style::new().fg(if err.is_empty() { Color::DarkGray } else { Color::Red })),
            ];
            f.render_widget(Paragraph::new(text).block(Block::bordered().title(format!(" rolling window (last {CAP}) "))), v[1]);
        })?;

        if event::poll(Duration::from_millis(200))? {
            if let Event::Key(k) = event::read()? {
                if matches!(k.code, KeyCode::Char('q') | KeyCode::Esc) {
                    break;
                }
            }
        }
    }
    ratatui::restore();
    Ok(())
}
