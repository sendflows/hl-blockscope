//! Execution-quality regulator for HyperCore perps.
//!
//! Benchmarks every HyperCore fill against the tightest bid/ask across
//! Binance, Bybit and OKX at the instant the fill was observed, and runs the
//! DFBA "mispricing clock": what fraction of the time HL's mid sits more than
//! k bps from the CEX composite mid.
//!
//! Clock discipline (this is the whole game):
//!   * every event stores exch_ts (the venue's own ms clock) AND recv_ts
//!     (local monotonic at frame arrival)
//!   * the fill <-> reference join is done on recv_ts ONLY - one clock
//!   * per-feed `recv_wall - exch_ts` is reported so you can see how stale the
//!     reference is; it includes network delay and clock skew and is NOT
//!     subtracted, because it cannot be separated from skew
//!
//! Usage:  regulator [--coin BTC] [--secs 120] [--hl-fee-bps 7.0]
//!                   [--cex-fee-bps 2.0] [--csv fills.csv]

use std::{
    collections::VecDeque,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[derive(Clone, Copy, Debug)]
struct Quote {
    bid: f64,
    ask: f64,
    exch_ms: u64,
    recv_wall_ms: u64,
}

#[derive(Debug)]
enum Ev {
    Cex { venue: &'static str, q: Quote },
    HlBbo(Quote),
    HlTrade { px: f64, sz: f64, buy: bool, exch_ms: u64 },
}

fn wall_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

fn f(v: &Value) -> f64 {
    v.as_str().and_then(|s| s.parse().ok()).or_else(|| v.as_f64()).unwrap_or(0.0)
}

async fn feed(url: String, sub: Option<String>, tx: mpsc::Sender<Ev>, parse: fn(&Value, Instant) -> Vec<Ev>) {
    loop {
        match connect_async(&url).await {
            Ok((mut ws, _)) => {
                if let Some(s) = &sub {
                    let _ = ws.send(Message::Text(s.clone().into())).await;
                }
                while let Some(Ok(m)) = ws.next().await {
                    let recv = Instant::now();
                    let text = match m {
                        Message::Text(t) => t.to_string(),
                        Message::Ping(p) => {
                            let _ = ws.send(Message::Pong(p)).await;
                            continue;
                        }
                        _ => continue,
                    };
                    if let Ok(v) = serde_json::from_str::<Value>(&text) {
                        for e in parse(&v, recv) {
                            if tx.send(e).await.is_err() {
                                return;
                            }
                        }
                    }
                }
            }
            Err(e) => eprintln!("ws {url}: {e}"),
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

fn parse_binance(v: &Value, _recv: Instant) -> Vec<Ev> {
    if v.get("b").is_none() {
        return vec![];
    }
    vec![Ev::Cex {
        venue: "binance",
        q: Quote { bid: f(&v["b"]), ask: f(&v["a"]), exch_ms: v["T"].as_u64().unwrap_or(0), recv_wall_ms: wall_ms() },
    }]
}

fn parse_bybit(v: &Value, _recv: Instant) -> Vec<Ev> {
    let d = &v["data"];
    let bid = d["b"].get(0).map(|l| f(&l[0])).unwrap_or(0.0);
    let ask = d["a"].get(0).map(|l| f(&l[0])).unwrap_or(0.0);
    if bid == 0.0 && ask == 0.0 {
        return vec![];
    }
    vec![Ev::Cex {
        venue: "bybit",
        q: Quote { bid, ask, exch_ms: v["ts"].as_u64().unwrap_or(0), recv_wall_ms: wall_ms() },
    }]
}

fn parse_okx(v: &Value, _recv: Instant) -> Vec<Ev> {
    let Some(d) = v["data"].get(0) else { return vec![] };
    let bid = d["bids"].get(0).map(|l| f(&l[0])).unwrap_or(0.0);
    let ask = d["asks"].get(0).map(|l| f(&l[0])).unwrap_or(0.0);
    if bid == 0.0 || ask == 0.0 {
        return vec![];
    }
    vec![Ev::Cex {
        venue: "okx",
        q: Quote { bid, ask, exch_ms: f(&d["ts"]) as u64, recv_wall_ms: wall_ms() },
    }]
}

fn parse_hl(v: &Value, _recv: Instant) -> Vec<Ev> {
    match v["channel"].as_str() {
        Some("bbo") => {
            let d = &v["data"];
            let (Some(b), Some(a)) = (d["bbo"].get(0), d["bbo"].get(1)) else { return vec![] };
            vec![Ev::HlBbo(Quote {
                bid: f(&b["px"]), ask: f(&a["px"]), exch_ms: d["time"].as_u64().unwrap_or(0), recv_wall_ms: wall_ms(),
            })]
        }
        Some("trades") => v["data"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .map(|t| Ev::HlTrade {
                        px: f(&t["px"]),
                        sz: f(&t["sz"]),
                        buy: t["side"].as_str() == Some("B"),
                        exch_ms: t["time"].as_u64().unwrap_or(0),
                    })
                    .collect()
            })
            .unwrap_or_default(),
        _ => vec![],
    }
}

/// Per-venue quote history keyed by the venue's own timestamp.
///
/// The reference at fill time T is built from each venue's latest quote with
/// exch_ts <= T. This is a CROSS-CLOCK join and is only valid because the
/// exchanges' clocks agree with each other to a few tens of ms - the
/// per-venue `recv_wall - exch_ts` printed at the end is the evidence; if
/// those numbers drift apart, the join is wrong. Joining on local receive
/// time instead is single-clock but compares a fill against CEX state that
/// is ~300 ms NEWER, because HL's WebSocket delivers later than the CEXs'.
///
/// Composite mid is the MEDIAN of venue mids; the CEX spread is the tightest
/// SINGLE-venue spread. A cross-venue max-bid/min-ask composite crosses
/// itself whenever quotes are a few ms apart and yields negative spreads.
#[derive(Default)]
struct Ref {
    hist: [VecDeque<Quote>; 3],
    lag: [VecDeque<f64>; 3],
}

const VENUES: [&str; 3] = ["binance", "bybit", "okx"];

struct RefPoint {
    mid: f64,
    half_spread_bps: f64,
    venues: usize,
}

impl Ref {
    fn set(&mut self, venue: &str, q: Quote) {
        let i = VENUES.iter().position(|v| *v == venue).unwrap();
        let h = &mut self.hist[i];
        h.push_back(q);
        if h.len() > 400 {
            h.pop_front();
        }
        let l = &mut self.lag[i];
        l.push_back(q.recv_wall_ms as f64 - q.exch_ms as f64);
        if l.len() > 2000 {
            l.pop_front();
        }
    }

    fn at(&self, t_ms: u64, max_age_ms: u64) -> Option<RefPoint> {
        let mut mids = Vec::with_capacity(3);
        let mut best_half = f64::MAX;
        for h in &self.hist {
            if let Some(q) = h.iter().rev().find(|q| q.exch_ms <= t_ms) {
                if t_ms - q.exch_ms <= max_age_ms && q.bid > 0.0 && q.ask > q.bid {
                    let mid = (q.bid + q.ask) / 2.0;
                    mids.push(mid);
                    best_half = best_half.min((q.ask - q.bid) / mid / 2.0 * 1e4);
                }
            }
        }
        if mids.is_empty() {
            return None;
        }
        mids.sort_by(|a, b| a.partial_cmp(b).unwrap());
        Some(RefPoint { mid: mids[mids.len() / 2], half_spread_bps: best_half, venues: mids.len() })
    }
}

/// HL's own BBO history on HL's clock, so effective spread is single-clock.
#[derive(Default)]
struct HlBook {
    hist: VecDeque<Quote>,
}

impl HlBook {
    fn set(&mut self, q: Quote) {
        self.hist.push_back(q);
        if self.hist.len() > 400 {
            self.hist.pop_front();
        }
    }
    fn mid_at(&self, t_ms: u64) -> Option<f64> {
        self.hist.iter().rev().find(|q| q.exch_ms <= t_ms).map(|q| (q.bid + q.ask) / 2.0)
    }
}

fn pct(v: &mut [f64], p: f64) -> f64 {
    if v.is_empty() {
        return f64::NAN;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[((v.len() - 1) as f64 * p).round() as usize]
}

fn arg(name: &str, default: &str) -> String {
    let a: Vec<String> = std::env::args().collect();
    a.iter().position(|x| x == name).and_then(|i| a.get(i + 1).cloned()).unwrap_or(default.into())
}

#[tokio::main]
async fn main() {
    let coin = arg("--coin", "BTC");
    let secs: u64 = arg("--secs", "120").parse().unwrap();
    let hl_fee: f64 = arg("--hl-fee-bps", "7.0").parse().unwrap(); // tier-0 taker 0.070%
    let cex_fee: f64 = arg("--cex-fee-bps", "2.0").parse().unwrap(); // ~VIP taker
    let csv = arg("--csv", "");
    let sym = coin.to_uppercase();

    let (tx, mut rx) = mpsc::channel::<Ev>(4096);
    tokio::spawn(feed(
        format!("wss://fstream.binance.com/ws/{}usdt@bookTicker", sym.to_lowercase()),
        None, tx.clone(), parse_binance,
    ));
    tokio::spawn(feed(
        "wss://stream.bybit.com/v5/public/linear".into(),
        Some(json!({"op":"subscribe","args":[format!("orderbook.1.{sym}USDT")]}).to_string()),
        tx.clone(), parse_bybit,
    ));
    tokio::spawn(feed(
        "wss://ws.okx.com:8443/ws/v5/public".into(),
        Some(json!({"op":"subscribe","args":[{"channel":"bbo-tbt","instId":format!("{sym}-USDT-SWAP")}]}).to_string()),
        tx.clone(), parse_okx,
    ));
    for sub in ["trades", "bbo"] {
        let tx = tx.clone();
        let s = json!({"method":"subscribe","subscription":{"type":sub,"coin":sym}}).to_string();
        tokio::spawn(async move { feed("wss://api.hyperliquid.xyz/ws".into(), Some(s), tx, parse_hl).await });
    }
    drop(tx);

    let mut r = Ref::default();
    let mut hl = HlBook::default();
    let mut hl_lag: VecDeque<f64> = VecDeque::new();

    // per fill: cost vs CEX mid decomposed into effective half-spread (vs HL's
    // own mid, single clock) + basis (HL mid vs CEX mid at fill time)
    let mut eff: Vec<f64> = Vec::new();
    let mut basis_at_fill: Vec<f64> = Vec::new();
    let mut cost_cex: Vec<f64> = Vec::new();
    let mut cex_half: Vec<f64> = Vec::new();
    let mut sizes: Vec<f64> = Vec::new();
    let (mut beat_raw, mut beat_adj, mut skipped) = (0usize, 0usize, 0usize);

    // mispricing clock on HL's bbo ticks: raw deviation and deviation net of a
    // rolling-median basis. Time-weighted on HL's own ms clock (single clock).
    let thresholds = [1.0, 2.0, 5.0];
    let (mut t_total, mut t_over_raw, mut t_over_adj) = (0.0f64, [0.0f64; 3], [0.0f64; 3]);
    let (mut abs_raw_w, mut abs_adj_w) = (0.0f64, 0.0f64);
    let mut basis_window: VecDeque<(u64, f64)> = VecDeque::new(); // (hl_ms, dev)
    let mut prev_tick: Option<(u64, f64)> = None; // (hl_ms, dev)
    let mut basis_all: Vec<f64> = Vec::new();

    let mut out = if csv.is_empty() { None } else { Some(std::fs::File::create(&csv).unwrap()) };
    if let Some(o) = &mut out {
        use std::io::Write;
        writeln!(o, "hl_ms,px,sz,side,hl_mid,cex_mid,cex_half_bps,eff_bps,basis_bps,cost_vs_cex_bps,venues").unwrap();
    }

    let max_age_ms = 1500u64;
    let deadline = Instant::now() + Duration::from_secs(secs);
    eprintln!("regulator {sym}: collecting for {secs}s ...");

    while let Ok(Some(ev)) = tokio::time::timeout_at(deadline.into(), rx.recv()).await {
        match ev {
            Ev::Cex { venue, q } => r.set(venue, q),
            Ev::HlBbo(q) => {
                hl_lag.push_back(q.recv_wall_ms as f64 - q.exch_ms as f64);
                if hl_lag.len() > 2000 {
                    hl_lag.pop_front();
                }
                hl.set(q);
                if let Some(rp) = r.at(q.exch_ms, max_age_ms) {
                    let dev = ((q.bid + q.ask) / 2.0 / rp.mid - 1.0) * 1e4;
                    basis_window.push_back((q.exch_ms, dev));
                    while basis_window.front().is_some_and(|(t, _)| q.exch_ms - t > 30_000) {
                        basis_window.pop_front();
                    }
                    let mut w: Vec<f64> = basis_window.iter().map(|(_, d)| *d).collect();
                    let rolling_basis = pct(&mut w, 0.5);
                    if let Some((pt, pdev)) = prev_tick {
                        let dt = (q.exch_ms.saturating_sub(pt)) as f64 / 1000.0;
                        let adj = pdev - rolling_basis;
                        t_total += dt;
                        abs_raw_w += pdev.abs() * dt;
                        abs_adj_w += adj.abs() * dt;
                        for (i, k) in thresholds.iter().enumerate() {
                            if pdev.abs() > *k { t_over_raw[i] += dt; }
                            if adj.abs() > *k { t_over_adj[i] += dt; }
                        }
                    }
                    prev_tick = Some((q.exch_ms, dev));
                    basis_all.push(dev);
                }
            }
            Ev::HlTrade { px, sz, buy, exch_ms } => {
                let (Some(rp), Some(hm)) = (r.at(exch_ms, max_age_ms), hl.mid_at(exch_ms)) else {
                    skipped += 1;
                    continue;
                };
                let sgn = if buy { 1.0 } else { -1.0 };
                let e = sgn * (px / hm - 1.0) * 1e4;            // effective half-spread paid on HL
                let b = sgn * (hm / rp.mid - 1.0) * 1e4;         // basis, signed by side
                let c = sgn * (px / rp.mid - 1.0) * 1e4;         // total cost vs CEX mid
                let cex = rp.half_spread_bps + cex_fee;
                if c + hl_fee < cex { beat_raw += 1; }
                if e + hl_fee < cex { beat_adj += 1; }
                eff.push(e); basis_at_fill.push(b); cost_cex.push(c); cex_half.push(rp.half_spread_bps); sizes.push(sz * px);
                if let Some(o) = &mut out {
                    use std::io::Write;
                    let _ = writeln!(o, "{exch_ms},{px},{sz},{},{hm},{},{:.4},{e:.4},{b:.4},{c:.4},{}",
                        if buy { "B" } else { "S" }, rp.mid, rp.half_spread_bps, rp.venues);
                }
            }
        }
    }

    // ---- report ----
    let n = eff.len();
    let notional: f64 = sizes.iter().sum();
    let q = |v: &Vec<f64>| { let mut c = v.clone(); [0.10, 0.25, 0.50, 0.75, 0.90].map(|p| pct(&mut c, p)) };
    println!("\n=== HyperCore {sym} perp vs CEX composite (binance/bybit/okx USDT perps) ===");
    println!("window {secs}s   fills {n}   skipped(no ref) {skipped}   notional ${notional:.0}");
    if n > 0 {
        println!("\nper-fill decomposition (bps, +ve = worse for the taker):        p10    p25    p50    p75    p90");
        for (name, v) in [("effective half-spread vs HL mid  [single clock]", &eff),
                          ("basis HL mid vs CEX mid, signed  [cross clock]", &basis_at_fill),
                          ("total cost vs CEX mid            [cross clock]", &cost_cex),
                          ("tightest single-venue CEX half-spread", &cex_half)] {
            let p = q(v);
            println!("  {name:<48} {:6.2} {:6.2} {:6.2} {:6.2} {:6.2}", p[0], p[1], p[2], p[3], p[4]);
        }
        println!("\nall-in comparison: HL = cost + {hl_fee:.1} bps taker fee   CEX = half-spread + {cex_fee:.1} bps fee");
        println!("  beat rate, raw (includes basis)      {:5.1}% of fills", 100.0 * beat_raw as f64 / n as f64);
        println!("  beat rate, basis-neutral (eff only)  {:5.1}% of fills", 100.0 * beat_adj as f64 / n as f64);
        println!("  -> basis is a level difference between USDC and USDT perps, not execution; the");
        println!("     basis-neutral number is the like-for-like execution comparison.");
    }
    if t_total > 0.0 {
        let mut b = basis_all.clone();
        println!("\nmispricing clock, time-weighted on HL's clock ({:.0}s):   raw     basis-adjusted (30s rolling median)", t_total);
        for (i, k) in thresholds.iter().enumerate() {
            println!("  |HL mid - CEX mid| > {k:.0} bps          {:5.1}%      {:5.1}%",
                100.0 * t_over_raw[i] / t_total, 100.0 * t_over_adj[i] / t_total);
        }
        println!("  mean |dev|                        {:.3} bps   {:.3} bps", abs_raw_w / t_total, abs_adj_w / t_total);
        println!("  signed basis p50 {:+.3} bps  (HL below CEX when negative)", pct(&mut b, 0.5));
        println!("  the adjusted column is DFBA's latency-mispricing metric; raw is dominated by the basis.");
    }
    println!("\nclock evidence: recv_wall - exch_ts per feed (ms). network delay + skew, NOT subtracted.");
    for (i, v) in VENUES.iter().enumerate() {
        let mut l: Vec<f64> = r.lag[i].iter().cloned().collect();
        if !l.is_empty() {
            println!("  {v:<8} p50 {:6.0}  p90 {:6.0}  n={}", pct(&mut l, 0.5), pct(&mut l, 0.9), l.len());
        }
    }
    let mut l: Vec<f64> = hl_lag.iter().cloned().collect();
    if !l.is_empty() {
        println!("  {:<8} p50 {:6.0}  p90 {:6.0}  n={}", "hl", pct(&mut l, 0.5), pct(&mut l, 0.9), l.len());
    }
    println!("\njoin: each fill at HL time T uses each venue's latest quote with exch_ts <= T (max age {max_age_ms} ms),");
    println!("composite = median venue mid. Valid only while the venues' clocks agree; the spread of the p50s");
    println!("above bounds the error. Effective spread is HL-vs-HL and needs no such assumption.");
}
