//! Participant classifier for HyperCore perps.
//!
//! For every fill in the window, records who took and who made (the public
//! `trades` feed carries both addresses), then measures each address by what
//! the mid did AFTER its fills:
//!
//!   markout_h  = side * (HL mid at t+h / HL mid at t - 1) in bps, taker view
//!
//! Positive markout means the taker was right; the maker was picked off. A
//! taker whose markouts are consistently positive is informed (a scalper in
//! DFBA's taxonomy); ~zero is natural flow. Maker quality is the mirror:
//! realised spread = half-spread captured - markout.
//!
//! Every markout is HL mid vs HL mid on HL's own clock. No cross-clock join.
//! The one cross-clock number is the pre-fill CEX move (did the CEX composite
//! move in the taker's direction in the 500 ms before the fill); it is the
//! latency-arb signature, reported separately and labelled as such.
//!
//! Classification is either the rule (default: informed if the 1 s markout
//! is positive at t > 2) or k-means on the per-address markout profile
//! (`--kmeans K`). The rule is falsifiable per address; k-means finds
//! structure you did not name but its clusters need reading by centroid.
//!
//! Usage:  classifier [--coin BTC] [--secs 300] [--min-fills 5] [--kmeans 0]
//!                    [--pickoff-bps 0.5] [--top 15] [--csv fills.csv]

use std::collections::{HashMap, VecDeque};

use hl_blockscope::feeds::{arg, deadline, pct, spawn, Ev, HlBook, Ref, Trade};

const H: [u64; 3] = [100, 1_000, 5_000];
const PRE_MS: u64 = 500;

struct Pending {
    t: Trade,
    mid0: f64,
    eff_bps: f64,
    pre_cex_bps: Option<f64>,
    mo: [Option<f64>; 3],
}

/// A fill with its markouts resolved.
struct Fill {
    ms: u64,
    taker: String,
    maker: String,
    notional: f64,
    eff: f64,
    mo: [f64; 3],
    pre: Option<f64>,
    /// zero tx hash: a TWAP sub-order (HL executes TWAPs every 30 s with no
    /// user tx), verified by the 30 s cadence per taker
    twap: bool,
}

/// One observation for statistics. Several fills share one observation when
/// they come from the same aggressive order: a sweep hitting 68 resting
/// orders in one millisecond is one decision, not 68, and its markouts are
/// identical by construction. Counting them separately inflates every t-stat.
#[derive(Default)]
struct Acc {
    n: usize,
    fills: usize,
    notional: f64,
    eff: Vec<f64>,
    mo: [Vec<f64>; 3],
    pre: Vec<f64>,
    pickoffs: usize,
    twap: usize,
}

impl Acc {
    fn push(&mut self, group: &[&Fill], pickoff_bps: f64) {
        let notional: f64 = group.iter().map(|f| f.notional).sum();
        let eff = group.iter().map(|f| f.eff * f.notional).sum::<f64>() / notional;
        let f0 = group[0];
        self.n += 1;
        self.fills += group.len();
        self.notional += notional;
        self.eff.push(eff);
        for i in 0..3 {
            self.mo[i].push(f0.mo[i]);
        }
        if let Some(p) = f0.pre {
            self.pre.push(p);
        }
        self.pickoffs += (f0.mo[1] > pickoff_bps) as usize;
        self.twap += f0.twap as usize;
    }
}

/// Group fills by key, preserving first-seen order.
fn group_by<'a, K: std::hash::Hash + Eq>(fills: &'a [Fill], key: impl Fn(&Fill) -> K) -> Vec<Vec<&'a Fill>> {
    let mut idx: HashMap<K, usize> = HashMap::new();
    let mut out: Vec<Vec<&Fill>> = Vec::new();
    for f in fills {
        let k = key(f);
        match idx.get(&k) {
            Some(&i) => out[i].push(f),
            None => {
                idx.insert(k, out.len());
                out.push(vec![f]);
            }
        }
    }
    out
}

fn mean(v: &[f64]) -> f64 {
    if v.is_empty() { f64::NAN } else { v.iter().sum::<f64>() / v.len() as f64 }
}

/// t-statistic of the mean against zero.
fn tstat(v: &[f64]) -> f64 {
    let n = v.len();
    if n < 2 {
        return f64::NAN;
    }
    let m = mean(v);
    let var = v.iter().map(|x| (x - m).powi(2)).sum::<f64>() / (n - 1) as f64;
    if var == 0.0 { f64::NAN } else { m / (var / n as f64).sqrt() }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Class {
    Informed,
    Natural,
    Twap,
    Thin,
    /// k-means cluster, index into the centroid table
    Cluster(u8),
}

impl Class {
    fn label(self) -> String {
        match self {
            Class::Informed => "informed".into(),
            Class::Natural => "natural".into(),
            Class::Twap => "twap".into(),
            Class::Thin => "thin".into(),
            Class::Cluster(i) => format!("k{i}"),
        }
    }
}

/// Rule classifier on the 1 s markout: informed if the mean is positive and
/// at least 2 standard errors from zero; anything else with enough orders is
/// natural. Direction alone is not enough - a taker with n=5 and one lucky
/// fill has a positive mean and no evidence.
fn classify_rule(a: &Acc, min_fills: usize) -> Class {
    if a.twap * 2 > a.n {
        return Class::Twap;
    }
    if a.n < min_fills {
        return Class::Thin;
    }
    let m = mean(&a.mo[1]);
    if m > 0.0 && tstat(&a.mo[1]) > 2.0 { Class::Informed } else { Class::Natural }
}

/// Per-address feature vector for clustering. Raw units; standardised inside
/// `kmeans` so no feature dominates by scale.
const FEATURES: [&str; 6] = ["mo100", "mo1s", "mo5s", "eff", "preCEX", "twap%"];

fn features(a: &Acc) -> [f64; 6] {
    [
        mean(&a.mo[0]),
        mean(&a.mo[1]),
        mean(&a.mo[2]),
        mean(&a.eff),
        if a.pre.is_empty() { 0.0 } else { mean(&a.pre) },
        a.twap as f64 / a.n as f64,
    ]
}

/// Lloyd's k-means with k-means++ seeding, z-scored features, best of
/// `restarts` by inertia. Deterministic: seeded LCG, no external RNG.
/// Returns (assignment per row, centroids in RAW units).
fn kmeans(rows: &[[f64; 6]], k: usize, restarts: usize) -> (Vec<usize>, Vec<[f64; 6]>) {
    let n = rows.len();
    let d = FEATURES.len();
    // z-score
    let mut mu = [0.0; 6];
    let mut sd = [0.0; 6];
    for j in 0..d {
        mu[j] = rows.iter().map(|r| r[j]).sum::<f64>() / n as f64;
        sd[j] = (rows.iter().map(|r| (r[j] - mu[j]).powi(2)).sum::<f64>() / n as f64).sqrt();
        if sd[j] == 0.0 {
            sd[j] = 1.0;
        }
    }
    let x: Vec<[f64; 6]> = rows.iter().map(|r| std::array::from_fn(|j| (r[j] - mu[j]) / sd[j])).collect();
    let dist2 = |a: &[f64; 6], b: &[f64; 6]| (0..d).map(|j| (a[j] - b[j]).powi(2)).sum::<f64>();

    let mut seed = 0x9E37_79B9_7F4A_7C15u64;
    let mut rnd = move || {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (seed >> 11) as f64 / (1u64 << 53) as f64
    };

    let mut best: Option<(f64, Vec<usize>, Vec<[f64; 6]>)> = None;
    for _ in 0..restarts {
        // k-means++ seeding
        let mut c: Vec<[f64; 6]> = vec![x[(rnd() * n as f64) as usize]];
        while c.len() < k {
            let w: Vec<f64> = x.iter().map(|p| c.iter().map(|q| dist2(p, q)).fold(f64::MAX, f64::min)).collect();
            let total: f64 = w.iter().sum();
            let mut r = rnd() * total;
            let mut pick = n - 1;
            for (i, wi) in w.iter().enumerate() {
                if r <= *wi {
                    pick = i;
                    break;
                }
                r -= wi;
            }
            c.push(x[pick]);
        }
        let mut assign = vec![0usize; n];
        for _ in 0..100 {
            let mut changed = false;
            for (i, p) in x.iter().enumerate() {
                let a = (0..k).min_by(|&a, &b| dist2(p, &c[a]).partial_cmp(&dist2(p, &c[b])).unwrap()).unwrap();
                if assign[i] != a {
                    assign[i] = a;
                    changed = true;
                }
            }
            for (ci, cent) in c.iter_mut().enumerate() {
                let members: Vec<&[f64; 6]> = x.iter().zip(&assign).filter(|(_, a)| **a == ci).map(|(p, _)| p).collect();
                if !members.is_empty() {
                    *cent = std::array::from_fn(|j| members.iter().map(|m| m[j]).sum::<f64>() / members.len() as f64);
                }
            }
            if !changed {
                break;
            }
        }
        let inertia: f64 = x.iter().zip(&assign).map(|(p, a)| dist2(p, &c[*a])).sum();
        if best.as_ref().is_none_or(|b| inertia < b.0) {
            best = Some((inertia, assign, c));
        }
    }
    let (_, assign, c) = best.unwrap();
    // back to raw units, and order clusters by centroid 1 s markout so k0 is
    // always the least informed
    let mut raw: Vec<[f64; 6]> = c.iter().map(|cent| std::array::from_fn(|j| cent[j] * sd[j] + mu[j])).collect();
    let mut order: Vec<usize> = (0..k).collect();
    order.sort_by(|a, b| raw[*a][1].partial_cmp(&raw[*b][1]).unwrap());
    let rank: Vec<usize> = (0..k).map(|ci| order.iter().position(|o| *o == ci).unwrap()).collect();
    raw = order.iter().map(|o| raw[*o]).collect();
    (assign.into_iter().map(|a| rank[a]).collect(), raw)
}

fn short(a: &str) -> String {
    if a.len() > 12 { format!("{}..{}", &a[..6], &a[a.len() - 4..]) } else { a.to_string() }
}

#[tokio::main]
async fn main() {
    let sym = arg("--coin", "BTC").to_uppercase();
    let secs: u64 = arg("--secs", "300").parse().unwrap();
    let min_fills: usize = arg("--min-fills", "5").parse().unwrap();
    let pickoff_bps: f64 = arg("--pickoff-bps", "0.5").parse().unwrap();
    let k: usize = arg("--kmeans", "0").parse().unwrap(); // 0 = rule classifier
    let top: usize = arg("--top", "15").parse().unwrap();
    let csv = arg("--csv", "");
    let mut rx = spawn(&sym, true);

    let mut book = HlBook::default();
    let mut cex = Ref::default();
    let mut pending: VecDeque<Pending> = VecDeque::new();
    let mut fills: Vec<Fill> = Vec::new();
    let (mut zero_hash, mut no_mid) = (0usize, 0usize);

    let mut out = if csv.is_empty() { None } else { Some(std::fs::File::create(&csv).unwrap()) };
    if let Some(o) = &mut out {
        use std::io::Write;
        writeln!(o, "hl_ms,px,sz,side,taker,maker,mid0,eff_bps,mo100_bps,mo1s_bps,mo5s_bps,pre_cex_bps,zero_hash").unwrap();
    }

    let deadline = deadline(secs);
    eprintln!("classifier {sym}: collecting for {secs}s (markouts need +5s after the last fill) ...");

    // resolve pending fills against the book; returns fully-resolved ones
    fn resolve(pending: &mut VecDeque<Pending>, book: &HlBook, now_ms: u64) -> Vec<Pending> {
        let mut done = Vec::new();
        for p in pending.iter_mut() {
            let sgn = if p.t.buy { 1.0 } else { -1.0 };
            for (i, h) in H.iter().enumerate() {
                if p.mo[i].is_none() && now_ms >= p.t.exch_ms + h {
                    if let Some(m) = book.mid_at(p.t.exch_ms + h) {
                        p.mo[i] = Some(sgn * (m / p.mid0 - 1.0) * 1e4);
                    }
                }
            }
        }
        while pending.front().is_some_and(|p| p.mo.iter().all(|m| m.is_some())) {
            done.push(pending.pop_front().unwrap());
        }
        done
    }

    while let Ok(Some(ev)) = tokio::time::timeout_at(deadline.into(), rx.recv()).await {
        match ev {
            Ev::Cex { venue, q } => cex.set(venue, q),
            Ev::HlBbo(q) => {
                book.set(q);
                for p in resolve(&mut pending, &book, q.exch_ms) {
                    let mo = [p.mo[0].unwrap(), p.mo[1].unwrap(), p.mo[2].unwrap()];
                    if let Some(o) = &mut out {
                        use std::io::Write;
                        let _ = writeln!(
                            o, "{},{},{},{},{},{},{},{:.4},{:.4},{:.4},{:.4},{},{}",
                            p.t.exch_ms, p.t.px, p.t.sz, if p.t.buy { "B" } else { "S" }, p.t.taker(), p.t.maker(),
                            p.mid0, p.eff_bps, mo[0], mo[1], mo[2],
                            p.pre_cex_bps.map(|x| format!("{x:.4}")).unwrap_or_default(), p.t.zero_hash as u8
                        );
                    }
                    fills.push(Fill {
                        ms: p.t.exch_ms,
                        taker: p.t.taker().to_string(),
                        maker: p.t.maker().to_string(),
                        notional: p.t.px * p.t.sz,
                        eff: p.eff_bps,
                        mo,
                        pre: p.pre_cex_bps,
                        twap: p.t.zero_hash,
                    });
                }
            }
            Ev::HlTrade(t) => {
                zero_hash += t.zero_hash as usize;
                // pre-trade mid: the bbo stamped with the fill's own ms is post-trade state
                let Some(mid0) = book.mid_before(t.exch_ms) else {
                    no_mid += 1;
                    continue;
                };
                let sgn = if t.buy { 1.0 } else { -1.0 };
                let eff_bps = sgn * (t.px / mid0 - 1.0) * 1e4;
                let pre_cex_bps = match (cex.at(t.exch_ms, 1500), cex.at(t.exch_ms - PRE_MS, 1500)) {
                    (Some(now), Some(before)) => Some(sgn * (now.mid / before.mid - 1.0) * 1e4),
                    _ => None,
                };
                pending.push_back(Pending { t, mid0, eff_bps, pre_cex_bps, mo: [None; 3] });
            }
        }
    }
    // fills in the last 5 s of the window cannot be marked out; they are dropped, not zero-filled
    let unresolved = pending.len();

    // ---- aggregate to observations ----
    // taker observation = one aggressive order = (taker, ms). maker observation
    // = one resting order hit = (maker, ms, taker); a maker with several
    // resting orders at one level hit by one sweep is still one decision.
    let orders = group_by(&fills, |f| (f.taker.clone(), f.ms));
    let maker_hits = group_by(&fills, |f| (f.maker.clone(), f.ms, f.taker.clone()));
    let mut takers: HashMap<&str, Acc> = HashMap::new();
    for g in &orders {
        takers.entry(&g[0].taker).or_default().push(g, pickoff_bps);
    }
    let mut makers: HashMap<&str, Acc> = HashMap::new();
    for g in &maker_hits {
        makers.entry(&g[0].maker).or_default().push(g, pickoff_bps);
    }

    // ---- report ----
    let n = fills.len();
    let notional: f64 = fills.iter().map(|f| f.notional).sum();
    println!("\n=== HyperCore {sym} participant classification, {secs}s window ===");
    println!("fills {n} -> orders {}   notional ${notional:.0}   unresolved (last 5s) {unresolved}   no HL mid {no_mid}   twap (zero-hash) fills {zero_hash}",
        orders.len());
    println!("takers {}   makers {}", takers.len(), makers.len());
    if orders.is_empty() {
        return;
    }

    // whole-market markout distribution per ORDER, taker view
    let mut all = Acc::default();
    for g in &orders {
        all.push(g, pickoff_bps);
    }
    println!("\nmarket-wide taker markout per order (bps, HL mid vs HL mid, one clock):   p10    p25    p50    p75    p90   mean  t-stat");
    for (i, h) in H.iter().enumerate() {
        let mut c = all.mo[i].clone();
        let q = [0.10, 0.25, 0.50, 0.75, 0.90].map(|p| pct(&mut c, p));
        println!("  +{:<5}ms{:>58} {:6.2} {:6.2} {:6.2} {:6.2} {:6.2} {:6.2} {:6.1}",
            h, "", q[0], q[1], q[2], q[3], q[4], mean(&all.mo[i]), tstat(&all.mo[i]));
    }
    let w: f64 = fills.iter().map(|f| f.notional * f.mo[1]).sum::<f64>() / notional;
    let e: f64 = fills.iter().map(|f| f.notional * f.eff).sum::<f64>() / notional;
    println!("  notional-weighted: half-spread paid {e:+.3} bps, 1s markout {w:+.3} bps -> maker realised spread {:+.3} bps per $", e - w);

    // ---- takers ----
    let mut classes: HashMap<&str, Class> = takers.iter().map(|(a, acc)| (*a, classify_rule(acc, min_fills))).collect();
    let mut centroids: Vec<[f64; 6]> = Vec::new();
    if k > 0 {
        // cluster only addresses with enough orders; thin stays thin
        let eligible: Vec<(&str, [f64; 6])> =
            takers.iter().filter(|(_, a)| a.n >= min_fills).map(|(a, acc)| (*a, features(acc))).collect();
        if eligible.len() >= k {
            let rows: Vec<[f64; 6]> = eligible.iter().map(|e| e.1).collect();
            let (assign, c) = kmeans(&rows, k, 10);
            centroids = c;
            for (a, _) in &takers {
                classes.insert(a, Class::Thin);
            }
            for ((a, _), ci) in eligible.iter().zip(assign) {
                classes.insert(a, Class::Cluster(ci as u8));
            }
        } else {
            eprintln!("kmeans: only {} addresses with >= {min_fills} orders, need >= {k}; using rule", eligible.len());
        }
    }
    let mut tk: Vec<(&&str, &Acc)> = takers.iter().collect();
    tk.sort_by(|a, b| b.1.notional.partial_cmp(&a.1.notional).unwrap());
    println!("\ntakers by notional (top {top}):");
    println!("  {:<14} {:>6} {:>5} {:>10} {:>7} {:>7} {:>7} {:>7} {:>6} {:>8}  class",
        "address", "orders", "fills", "notional", "eff", "mo100", "mo1s", "mo5s", "t1s", "preCEX");
    for (a, acc) in tk.iter().take(top) {
        println!("  {:<14} {:>6} {:>5} {:>10.0} {:>7.2} {:>7.2} {:>7.2} {:>7.2} {:>6.1} {:>8}  {}",
            short(a), acc.n, acc.fills, acc.notional, mean(&acc.eff), mean(&acc.mo[0]), mean(&acc.mo[1]), mean(&acc.mo[2]),
            tstat(&acc.mo[1]),
            if acc.pre.is_empty() { "-".into() } else { format!("{:+.2}", mean(&acc.pre)) },
            classes[*a].label());
    }
    println!("  eff = half-spread paid vs pre-trade HL mid; mo = taker markout; t1s = t-stat of mo1s over orders;");
    println!("  preCEX = CEX composite move in taker's direction over the {PRE_MS} ms BEFORE the fill [cross clock]");

    // ---- makers ----
    let mut mk: Vec<(&&str, &Acc)> = makers.iter().collect();
    mk.sort_by(|a, b| b.1.notional.partial_cmp(&a.1.notional).unwrap());
    println!("\nmakers by notional (top {top}):");
    println!("  {:<14} {:>5} {:>10} {:>7} {:>7} {:>7} {:>7} {:>7} {:>8}",
        "address", "hits", "notional", "share", "captd", "advsel", "real1s", "real5s", "pickoff");
    let maker_total: f64 = mk.iter().map(|m| m.1.notional).sum();
    for (a, acc) in mk.iter().take(top) {
        let captured = mean(&acc.eff);
        let adv = mean(&acc.mo[1]);
        println!("  {:<14} {:>5} {:>10.0} {:>6.1}% {:>7.2} {:>7.2} {:>7.2} {:>7.2} {:>7.1}%",
            short(a), acc.n, acc.notional, 100.0 * acc.notional / maker_total, captured, -adv,
            captured - adv, captured - mean(&acc.mo[2]), 100.0 * acc.pickoffs as f64 / acc.n as f64);
    }
    println!("  captd = half-spread captured; advsel = -taker markout 1s; real = realised spread at horizon;");
    println!("  pickoff = share of hits where the taker's 1s markout exceeded {pickoff_bps} bps");
    let top5: f64 = mk.iter().take(5).map(|m| m.1.notional).sum::<f64>() / maker_total;
    let top5t: f64 = tk.iter().take(5).map(|m| m.1.notional).sum::<f64>() / notional;
    println!("  concentration: top-5 makers {:.0}% of maker notional, top-5 takers {:.0}% of taker notional",
        100.0 * top5, 100.0 * top5t);

    // ---- volume decomposition ----
    let mut by: HashMap<(Class, &str), (f64, usize)> = HashMap::new();
    for f in &fills {
        let tc = classes[f.taker.as_str()];
        // a maker hit at least min_fills times is a "maker"; otherwise it is a resting order from someone else
        let mc = if makers[f.maker.as_str()].n >= min_fills { "maker" } else { "other" };
        let e = by.entry((tc, mc)).or_default();
        e.0 += f.notional;
        e.1 += 1;
    }
    println!("\nvolume decomposition (taker class x maker role):");
    let mut rows: Vec<_> = by.iter().collect();
    rows.sort_by(|a, b| b.1 .0.partial_cmp(&a.1 .0).unwrap());
    for ((tc, mc), (nt, cnt)) in rows {
        println!("  {:<9} taker vs {:<6} resting   {:>5.1}%  ${:>10.0}  {cnt} fills", tc.label(), mc, 100.0 * nt / notional, nt);
    }
    let twap: f64 = fills.iter().filter(|f| f.twap).map(|f| f.notional).sum::<f64>() / notional;
    if centroids.is_empty() {
        let informed: f64 = fills.iter().filter(|f| classes[f.taker.as_str()] == Class::Informed).map(|f| f.notional).sum::<f64>() / notional;
        println!("  informed takers = {:.1}% of notional; twap sub-orders = {:.1}%; 'thin' = fewer than {min_fills} orders, unclassifiable in this window",
            100.0 * informed, 100.0 * twap);
        println!("\nclassification: rule. twap if most orders carry a zero tx hash; else informed if mean 1s markout > 0 with t-stat > 2 over orders; else natural. Markouts are HL-vs-HL on HL's clock.");
    } else {
        println!("  twap sub-orders = {:.1}% of notional; 'thin' = fewer than {min_fills} orders, not clustered", 100.0 * twap);
        println!("\nclassification: k-means, k={k}, z-scored features, k-means++ seeding, best of 10 restarts. Clusters ordered by centroid 1s markout.");
        println!("  {:<4} {:>6} {:>10}  {}", "id", "n", "notional", FEATURES.map(|f| format!("{f:>7}")).join(" "));
        for (i, c) in centroids.iter().enumerate() {
            let members: Vec<&Acc> = takers.iter().filter(|(a, _)| classes[**a] == Class::Cluster(i as u8)).map(|(_, acc)| acc).collect();
            let nt: f64 = members.iter().map(|m| m.notional).sum();
            println!("  k{i:<3} {:>6} {:>10.0}  {}", members.len(), nt, c.map(|v| format!("{v:>7.2}")).join(" "));
        }
        println!("  read the centroids: a cluster with high mo1s and high preCEX is latency arb; high twap% is TWAP flow; near-zero everything is natural.");
    }
}
