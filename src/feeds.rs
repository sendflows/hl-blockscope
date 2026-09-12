//! WebSocket feeds shared by the regulator and classifier binaries.
//!
//! Every event carries `exch_ms` (the venue's own millisecond clock) and
//! `recv_wall_ms` (local wall clock at frame arrival). The two are never
//! subtracted except to print `recv_wall - exch_ts` as evidence of delivery
//! delay + skew. HyperCore's feed arrives ~300 ms after the CEX feeds; that is
//! a deliberate speedbump on HL's side, not clock error.

use std::{
    collections::VecDeque,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[derive(Clone, Copy, Debug)]
pub struct Quote {
    pub bid: f64,
    pub ask: f64,
    pub exch_ms: u64,
    pub recv_wall_ms: u64,
}

impl Quote {
    pub fn mid(&self) -> f64 {
        (self.bid + self.ask) / 2.0
    }
}

#[derive(Debug)]
pub struct Trade {
    pub px: f64,
    pub sz: f64,
    /// taker side
    pub buy: bool,
    pub exch_ms: u64,
    pub buyer: String,
    pub seller: String,
    /// zero hash on HL marks a fill not attributable to a normal order tx
    pub zero_hash: bool,
}

impl Trade {
    pub fn taker(&self) -> &str {
        if self.buy { &self.buyer } else { &self.seller }
    }
    pub fn maker(&self) -> &str {
        if self.buy { &self.seller } else { &self.buyer }
    }
}

#[derive(Debug)]
pub enum Ev {
    Cex { venue: &'static str, q: Quote },
    HlBbo(Quote),
    HlTrade(Trade),
}

pub const VENUES: [&str; 3] = ["binance", "bybit", "okx"];

pub fn wall_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

fn f(v: &Value) -> f64 {
    v.as_str().and_then(|s| s.parse().ok()).or_else(|| v.as_f64()).unwrap_or(0.0)
}

async fn feed(url: String, sub: Option<String>, tx: mpsc::Sender<Ev>, parse: fn(&Value) -> Vec<Ev>) {
    loop {
        match connect_async(&url).await {
            Ok((mut ws, _)) => {
                if let Some(s) = &sub {
                    let _ = ws.send(Message::Text(s.clone().into())).await;
                }
                while let Some(Ok(m)) = ws.next().await {
                    let text = match m {
                        Message::Text(t) => t.to_string(),
                        Message::Ping(p) => {
                            let _ = ws.send(Message::Pong(p)).await;
                            continue;
                        }
                        _ => continue,
                    };
                    if let Ok(v) = serde_json::from_str::<Value>(&text) {
                        for e in parse(&v) {
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

fn parse_binance(v: &Value) -> Vec<Ev> {
    if v.get("b").is_none() {
        return vec![];
    }
    vec![Ev::Cex {
        venue: "binance",
        q: Quote { bid: f(&v["b"]), ask: f(&v["a"]), exch_ms: v["T"].as_u64().unwrap_or(0), recv_wall_ms: wall_ms() },
    }]
}

fn parse_bybit(v: &Value) -> Vec<Ev> {
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

fn parse_okx(v: &Value) -> Vec<Ev> {
    let Some(d) = v["data"].get(0) else { return vec![] };
    let bid = d["bids"].get(0).map(|l| f(&l[0])).unwrap_or(0.0);
    let ask = d["asks"].get(0).map(|l| f(&l[0])).unwrap_or(0.0);
    if bid == 0.0 || ask == 0.0 {
        return vec![];
    }
    vec![Ev::Cex { venue: "okx", q: Quote { bid, ask, exch_ms: f(&d["ts"]) as u64, recv_wall_ms: wall_ms() } }]
}

fn parse_hl(v: &Value) -> Vec<Ev> {
    match v["channel"].as_str() {
        Some("bbo") => {
            let d = &v["data"];
            let (Some(b), Some(a)) = (d["bbo"].get(0), d["bbo"].get(1)) else { return vec![] };
            vec![Ev::HlBbo(Quote {
                bid: f(&b["px"]),
                ask: f(&a["px"]),
                exch_ms: d["time"].as_u64().unwrap_or(0),
                recv_wall_ms: wall_ms(),
            })]
        }
        Some("trades") => v["data"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .map(|t| {
                        let u = &t["users"];
                        Ev::HlTrade(Trade {
                            px: f(&t["px"]),
                            sz: f(&t["sz"]),
                            buy: t["side"].as_str() == Some("B"),
                            exch_ms: t["time"].as_u64().unwrap_or(0),
                            buyer: u[0].as_str().unwrap_or("").to_string(),
                            seller: u[1].as_str().unwrap_or("").to_string(),
                            zero_hash: t["hash"].as_str().is_some_and(|h| h.trim_start_matches("0x").bytes().all(|c| c == b'0')),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default(),
        _ => vec![],
    }
}

/// Spawn HL trades+bbo for `sym`, and optionally the three CEX USDT-perp
/// book-ticker feeds. Returns the merged receiver.
pub fn spawn(sym: &str, cex: bool) -> mpsc::Receiver<Ev> {
    let (tx, rx) = mpsc::channel::<Ev>(65536);
    if cex {
        let lower = sym.to_lowercase();
        let t = tx.clone();
        tokio::spawn(feed(format!("wss://fstream.binance.com/ws/{lower}usdt@bookTicker"), None, t, parse_binance));
        let t = tx.clone();
        let s = json!({"op":"subscribe","args":[format!("orderbook.1.{sym}USDT")]}).to_string();
        tokio::spawn(feed("wss://stream.bybit.com/v5/public/linear".into(), Some(s), t, parse_bybit));
        let t = tx.clone();
        let s = json!({"op":"subscribe","args":[{"channel":"bbo-tbt","instId":format!("{sym}-USDT-SWAP")}]}).to_string();
        tokio::spawn(feed("wss://ws.okx.com:8443/ws/v5/public".into(), Some(s), t, parse_okx));
    }
    for sub in ["trades", "bbo"] {
        let t = tx.clone();
        let s = json!({"method":"subscribe","subscription":{"type":sub,"coin":sym}}).to_string();
        tokio::spawn(feed("wss://api.hyperliquid.xyz/ws".into(), Some(s), t, parse_hl));
    }
    rx
}

/// Per-venue quote history keyed by the venue's own timestamp.
///
/// The reference at fill time T is built from each venue's latest quote with
/// exch_ts <= T. This is a CROSS-CLOCK join and is only valid because the
/// exchanges' clocks agree with each other to a few tens of ms - the
/// per-venue `recv_wall - exch_ts` printed at the end is the evidence; if
/// those numbers drift apart, the join is wrong. Joining on local receive
/// time instead is single-clock but compares a fill against CEX state that
/// is ~300 ms NEWER, because HL's feed sits behind a deliberate speedbump.
///
/// Composite mid is the MEDIAN of venue mids; the CEX spread is the tightest
/// SINGLE-venue spread. A cross-venue max-bid/min-ask composite crosses
/// itself whenever quotes are a few ms apart and yields negative spreads.
#[derive(Default)]
pub struct Ref {
    pub hist: [VecDeque<Quote>; 3],
    pub lag: [VecDeque<f64>; 3],
}

pub struct RefPoint {
    pub mid: f64,
    pub half_spread_bps: f64,
    pub venues: usize,
}

impl Ref {
    pub fn set(&mut self, venue: &str, q: Quote) {
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

    pub fn at(&self, t_ms: u64, max_age_ms: u64) -> Option<RefPoint> {
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
pub struct HlBook {
    pub hist: VecDeque<Quote>,
}

impl HlBook {
    pub fn set(&mut self, q: Quote) {
        self.hist.push_back(q);
        if self.hist.len() > 400 {
            self.hist.pop_front();
        }
    }
    /// Mid as of HL time t (last bbo with exch_ts <= t).
    pub fn mid_at(&self, t_ms: u64) -> Option<f64> {
        self.hist.iter().rev().find(|q| q.exch_ms <= t_ms).map(Quote::mid)
    }
    /// Mid the taker saw BEFORE a fill stamped t. The bbo stamped t is the
    /// post-trade state of the block containing the fill: a sweep that
    /// exhausts a level and rests its remainder moves the touch in the same
    /// millisecond, so `mid_at(t)` puts fills on the wrong side of the mid.
    pub fn mid_before(&self, t_ms: u64) -> Option<f64> {
        self.hist.iter().rev().find(|q| q.exch_ms < t_ms).map(Quote::mid)
    }
}

/// Nearest-p quantile; sorts in place.
pub fn pct(v: &mut [f64], p: f64) -> f64 {
    if v.is_empty() {
        return f64::NAN;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[((v.len() - 1) as f64 * p).round() as usize]
}

pub fn arg(name: &str, default: &str) -> String {
    let a: Vec<String> = std::env::args().collect();
    a.iter().position(|x| x == name).and_then(|i| a.get(i + 1).cloned()).unwrap_or(default.into())
}

pub fn deadline(secs: u64) -> Instant {
    Instant::now() + Duration::from_secs(secs)
}
