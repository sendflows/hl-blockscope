//! Execution-quality regulator for HyperCore perps.
//!
//! Benchmarks every HyperCore fill against Binance/Bybit/OKX USDT perps at the
//! fill's own timestamp, and runs the DFBA "mispricing clock": what fraction
//! of the time HL's mid sits more than k bps from the CEX composite mid.
//!
//! Clock discipline:
//!   * effective half-spread is fill vs HL's own mid: one clock, HL's
//!   * basis and mispricing use each venue's latest quote with exch_ts <= T:
//!     a cross-clock join that holds only while the venues' clocks agree;
//!     `recv_wall - exch_ts` per feed is printed as the evidence
//!   * HL's feed arrives ~300 ms after the CEX feeds. That is HL's speedbump,
//!     not skew, and it is why the join is on exchange time, not receive time
//!
//! Usage:  regulator [--coin BTC] [--secs 120] [--hl-fee-bps 7.0]
//!                   [--cex-fee-bps 2.0] [--csv fills.csv]

use std::collections::VecDeque;

use hl_blockscope::feeds::{arg, deadline, pct, spawn, Ev, HlBook, Ref, VENUES};

#[tokio::main]
async fn main() {
    let coin = arg("--coin", "BTC");
    let secs: u64 = arg("--secs", "120").parse().unwrap();
    let hl_fee: f64 = arg("--hl-fee-bps", "7.0").parse().unwrap(); // tier-0 taker 0.070%
    let cex_fee: f64 = arg("--cex-fee-bps", "2.0").parse().unwrap(); // ~VIP taker
    let csv = arg("--csv", "");
    let sym = coin.to_uppercase();
    let mut rx = spawn(&sym, true);

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
    let deadline = deadline(secs);
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
            Ev::HlTrade(t) => {
                let (px, sz, buy, exch_ms) = (t.px, t.sz, t.buy, t.exch_ms);
                let (Some(rp), Some(hm)) = (r.at(exch_ms, max_age_ms), hl.mid_before(exch_ms)) else {
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
