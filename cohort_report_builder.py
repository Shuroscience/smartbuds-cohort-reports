#!/usr/bin/env python3
"""Smartbuds Cohort Report Builder — generates HTML cohort reports from Mixpanel data."""

import argparse
import base64
import json
import os
import statistics
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import date, datetime, timedelta

API_SECRET = "a2042ec012fde1dc9000b3b06ead873c"
AUTH = base64.b64encode(f"{API_SECRET}:".encode()).decode()
CACHE_DIR = "/tmp/mp_cache"
os.makedirs(CACHE_DIR, exist_ok=True)

# ── Cohort definitions ─────────────────────────────────────────────────────────
COHORT_DEFS = [
    # (name, emoji, color, start_date, end_date_exclusive)
    ("All Prior",  "🔵", "#4a90d9",  None,         "2026-03-26"),
    ("Apple",      "🍎", "#f07030",  "2026-03-26", "2026-04-18"),
    ("Banana",     "🍌", "#27ae60",  "2026-04-18", "2026-05-02"),
    ("Cantaloupe", "🍈", "#9b59b6",  "2026-05-02", None),
]

# ── Helpers ────────────────────────────────────────────────────────────────────

def api_post(url, params, timeout=300):
    data = urllib.parse.urlencode(params).encode()
    req = urllib.request.Request(
        url, data=data,
        headers={"Authorization": f"Basic {AUTH}",
                 "Content-Type": "application/x-www-form-urlencoded"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def cache_path(name):
    return os.path.join(CACHE_DIR, f"{name}.json")


def load_cache(name):
    p = cache_path(name)
    if os.path.exists(p):
        with open(p) as f:
            return json.load(f)
    return None


def save_cache(name, data):
    with open(cache_path(name), "w") as f:
        json.dump(data, f)


def run_jql(script, params=None, cache_name=None):
    if cache_name:
        cached = load_cache(cache_name)
        if cached is not None:
            print(f"  [cache] {cache_name}")
            return cached
    body = {"script": script}
    if params:
        body["params"] = json.dumps(params)
    print(f"  [jql] {cache_name or 'query'}...")
    result = api_post("https://mixpanel.com/api/2.0/jql", body)
    if cache_name:
        save_cache(cache_name, result)
    return result


def get_all_profiles():
    cached = load_cache("profiles")
    if cached is not None:
        print("  [cache] profiles")
        return cached
    print("  [engage] fetching all profiles...")
    all_profiles = []
    session_id = None
    page = 0
    while True:
        params = {"page": page}
        if session_id:
            params["session_id"] = session_id
        result = api_post("https://mixpanel.com/api/2.0/engage", params)
        all_profiles.extend(result["results"])
        if not session_id:
            session_id = result.get("session_id")
        if len(result["results"]) < result.get("page_size", 1000):
            break
        page += 1
    save_cache("profiles", all_profiles)
    print(f"  [engage] fetched {len(all_profiles)} profiles")
    return all_profiles


# ── JQL query ─────────────────────────────────────────────────────────────────

JQL_METRICS = """
function main() {
  var REF = new Date('2026-01-01T00:00:00Z').getTime();  // ms

  return Events({
    from_date: '2026-01-01',
    to_date: params.to_date
  })
  .groupByUser(function(acc, events) {
    if (!acc) acc = {min_day: 999999, days: {}};
    for (var i = 0; i < events.length; i++) {
      var e = events[i];
      var dayIdx = Math.floor((e.time - REF) / 86400000);
      if (dayIdx < acc.min_day) acc.min_day = dayIdx;
      if (!acc.days[dayIdx]) acc.days[dayIdx] = {n: 0, s: 0, o: 0};
      acc.days[dayIdx].n++;
      if (e.name === 'sleep_session_started') acc.days[dayIdx].s = 1;  // flag: 1 session per day
      if (e.name === 'enter_home_screen') acc.days[dayIdx].o++;
    }
    return acc;
  })
  .map(function(r) {
    var d = r.value;
    if (!d || d.min_day === 999999) return null;

    var REF2 = new Date('2026-01-01T00:00:00Z').getTime();  // ms
    var minDay = d.min_day;
    var firstDate = new Date(REF2 + minDay * 86400000).toISOString().split('T')[0];

    var s7=0, s14=0, s28=0, n7=0, n14=0, o7=0, o14=0, dwa7=0, dwa14=0;

    var keys = Object.keys(d.days);
    for (var i = 0; i < keys.length; i++) {
      var day = parseInt(keys[i]);
      var rel = day - minDay;
      var c = d.days[keys[i]];
      if (rel >= 0) {
        if (rel < 7)  { s7+=c.s; n7+=c.n; o7+=c.o; dwa7++; }
        if (rel < 14) { s14+=c.s; n14+=c.n; o14+=c.o; dwa14++; }
        if (rel < 28) { s28+=c.s; }
      }
    }

    return {
      id: r.key[0],
      first_date: firstDate,
      s7: s7, s14: s14, s28: s28,
      n7: n7, n14: n14,
      o7: o7, o14: o14,
      dwa7: dwa7, dwa14: dwa14
    };
  })
  .filter(function(r) { return r !== null; });
}
"""


# ── Stats helpers ──────────────────────────────────────────────────────────────

def med(vals):
    if not vals:
        return 0
    return statistics.median(vals)


def mean(vals):
    if not vals:
        return 0.0
    return statistics.mean(vals)


def pct(numer, denom):
    if not denom:
        return 0.0
    return round(100 * numer / denom, 1)


def delta_pts(a, b):
    d = round(b - a, 1)
    if d > 0:
        return f'<span class="pos">↑ +{d} pts</span>'
    elif d < 0:
        return f'<span class="neg">↓ {d} pts</span>'
    return f'<span class="neu">— {d} pts</span>'


def delta_pct(a, b):
    if a == 0:
        return '<span class="neu">—</span>'
    d = round(100 * (b - a) / a)
    if d > 0:
        return f'<span class="pos">↑ +{d}%</span>'
    elif d < 0:
        return f'<span class="neg">↓ {d}%</span>'
    return f'<span class="neu">↑ +0%</span>'


# ── Session histogram ──────────────────────────────────────────────────────────

def session_hist(users, field):
    """Compute session distribution: 0,1,2,3,4-5,6+"""
    buckets = [0] * 6  # 0,1,2,3,4-5,6+
    for u in users:
        s = u[field]
        if s == 0: buckets[0] += 1
        elif s == 1: buckets[1] += 1
        elif s == 2: buckets[2] += 1
        elif s == 3: buckets[3] += 1
        elif s <= 5: buckets[4] += 1
        else: buckets[5] += 1
    total = len(users) or 1
    return [round(100 * b / total) for b in buckets]


def render_bar_chart(cohorts_data, field, labels=None):
    """Render a grouped bar chart as pure CSS/HTML."""
    if labels is None:
        labels = ["0", "1", "2", "3", "4–5", "6+"]

    # Compute histograms
    hists = []
    for (name, emoji, color, users) in cohorts_data:
        h = session_hist(users, field)
        hists.append((name, color, h))

    max_val = max(v for h in hists for v in h[2]) or 1

    legend_html = "".join(
        f'<span><span style="display:inline-block;width:12px;height:12px;background:{color};'
        f'border-radius:2px;margin-right:4px;vertical-align:middle;"></span>{name}</span>'
        for (name, color, _) in hists
    )

    bars_html = ""
    for i, label in enumerate(labels):
        group = '<div style="display:flex;align-items:flex-end;gap:2px;width:100%;justify-content:center;">'
        for (name, color, h) in hists:
            pct_val = h[i]
            bar_h = max(2, int(100 * pct_val / max_val))
            group += (
                f'<div style="display:flex;flex-direction:column;align-items:center;">'
                f'<span style="font-size:0.62rem;color:{color};margin-bottom:2px;font-weight:600;">{pct_val}%</span>'
                f'<div style="width:12px;height:{bar_h}px;background:{color};border-radius:2px 2px 0 0;"></div>'
                f'</div>'
            )
        group += '</div>'
        bars_html += (
            f'<div style="display:flex;flex-direction:column;align-items:center;flex:1;">'
            f'{group}'
            f'<div style="font-size:0.78rem;color:#555;margin-top:6px;text-align:center;">{label}</div>'
            f'</div>'
        )

    return (
        f'<div style="display:flex;gap:16px;font-size:0.78rem;color:#555;margin-bottom:8px;">{legend_html}</div>'
        f'<div style="display:flex;align-items:flex-end;height:120px;gap:6px;border-bottom:1px solid #e8e8e8;padding-top:16px;">'
        f'{bars_html}'
        f'</div>'
    )


# ── Cohort metrics ─────────────────────────────────────────────────────────────

def compute_metrics(users, days_elapsed_fn):
    """
    users: list of user dicts with s7, s14, s28, n7, n14, o7, o14, dwa7, dwa14, first_date
    days_elapsed_fn: fn(first_date_str) -> days since onboarding (for coverage)
    analysis_date: date object
    """
    eligible7  = [u for u in users if days_elapsed_fn(u["first_date"]) >= 7]
    eligible14 = [u for u in users if days_elapsed_fn(u["first_date"]) >= 14]
    eligible28 = [u for u in users if days_elapsed_fn(u["first_date"]) >= 28]

    def w7(u):  return u["s7"]
    def w14(u): return u["s14"]
    def w28(u): return u["s28"]

    gfr_users = eligible14
    gfr_count = sum(1 for u in gfr_users if u["s14"] >= 5)
    good_fruit_n = len(gfr_users)
    good_fruit_rate = pct(gfr_count, good_fruit_n)

    good_fruit_28 = [u for u in eligible28 if u["s14"] >= 5]
    rot_n = len(good_fruit_28)
    rot_count = sum(1 for u in good_fruit_28 if u["s28"] - u["s14"] == 0)
    rot_rate = pct(rot_count, rot_n) if rot_n >= 5 else None

    s7vals  = [u["s7"]  for u in eligible7]
    s14vals = [u["s14"] for u in eligible14]
    n7vals  = [u["n7"]  for u in eligible7]
    n14vals = [u["n14"] for u in eligible14]
    o7vals  = [u["o7"]  for u in eligible7]
    o14vals = [u["o14"] for u in eligible14]

    return {
        "n": len(users),
        "n7": len(eligible7),
        "n14": len(eligible14),
        "n28": len(eligible28),
        "good_fruit_rate": good_fruit_rate,
        "good_fruit_n": good_fruit_n,
        "rot_rate": rot_rate,
        "rot_n": rot_n,
        "rot_gf_n": len(good_fruit_28),
        # 7-day
        "med_s7":   round(med(s7vals), 1) if s7vals else 0,
        "mean_s7":  round(mean(s7vals), 1) if s7vals else 0,
        "pct1s7":   pct(sum(1 for v in s7vals if v >= 1), len(s7vals)),
        "pct3s7":   pct(sum(1 for v in s7vals if v >= 3), len(s7vals)),
        "pct0s7":   pct(sum(1 for v in s7vals if v == 0), len(s7vals)),
        "med_o7":   round(med(o7vals), 1) if o7vals else 0,
        "med_dwa7": round(med([u["dwa7"] for u in eligible7]), 1) if eligible7 else 0,
        "med_n7":   round(med(n7vals), 1) if n7vals else 0,
        # 14-day
        "med_s14":  round(med(s14vals), 1) if s14vals else 0,
        "mean_s14": round(mean(s14vals), 1) if s14vals else 0,
        "pct1s14":  pct(sum(1 for v in s14vals if v >= 1), len(s14vals)),
        "pct3s14":  pct(sum(1 for v in s14vals if v >= 3), len(s14vals)),
        "pct0s14":  pct(sum(1 for v in s14vals if v == 0), len(s14vals)),
        "med_o14":  round(med(o14vals), 1) if o14vals else 0,
        "med_dwa14": round(med([u["dwa14"] for u in eligible14]), 1) if eligible14 else 0,
        "med_n14":  round(med(n14vals), 1) if n14vals else 0,
    }


# ── HTML generation ────────────────────────────────────────────────────────────

CSS = """
  body  { font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
           max-width:960px;margin:0 auto;padding:40px 20px;color:#1a1a2e;background:#f9f9fb; }
  h1    { font-size:1.6rem;border-bottom:3px solid #4a90d9;padding-bottom:10px; }
  h2    { font-size:1.1rem;color:#2c3e50;margin-top:36px;background:#eef3fb;
           padding:8px 12px;border-left:4px solid #4a90d9;border-radius:0 4px 4px 0; }
  .meta { color:#777;font-size:0.85rem;margin-bottom:20px; }
  .callout      { background:#fff3cd;border:1px solid #ffc107;border-radius:6px;
                   padding:12px 16px;font-size:0.88rem;margin:14px 0;color:#856404; }
  .callout strong { color:#664d03; }
  .callout-blue { background:#cfe2ff;border-color:#4a90d9;color:#084298; }
  .callout-blue strong { color:#052c65; }
  .grid2  { display:grid;grid-template-columns:1fr 1fr;gap:16px;margin:16px 0; }
  .card   { background:white;border-radius:8px;padding:16px 20px;border:1px solid #dde3f0; }
  .card h4 { margin:0 0 4px 0;font-size:0.85rem;color:#666;font-weight:500; }
  .card .num { font-size:2rem;font-weight:700;color:#2c3e50; }
  .card .sub { font-size:0.8rem;color:#888;margin-top:2px;margin-bottom:8px; }
  table   { border-collapse:collapse;width:100%;margin:14px 0;font-size:0.91rem; }
  th      { background:#2c3e50;color:white;padding:9px 14px;text-align:left;font-weight:500; }
  td      { padding:8px 14px;border-bottom:1px solid #e8e8e8; }
  tr:nth-child(even) { background:#f5f8ff; }
  .highlight-row td { background:#fffbea !important;font-weight:600; }
  .pos  { color:#198754;font-weight:600; }
  .neg  { color:#dc3545;font-weight:600; }
  .neu  { color:#6c757d; }
  .coverage-badge { display:inline-block;background:#e9ecef;border-radius:10px;
                     padding:2px 10px;font-size:0.8rem;color:#495057;margin-left:8px; }
  footer { margin-top:48px;font-size:0.8rem;color:#aaa;border-top:1px solid #e0e0e0;padding-top:16px; }
"""


def card_block(label, color, n_total, sub, value, sub2=""):
    sub2_html = f'<div class="sub">{sub2}</div>' if sub2 else ''
    return (
        f'<div class="card">'
        f'<h4 style="color:{color}">{label}</h4>'
        f'<div class="num">{value}</div>'
        f'<div class="sub">{sub}</div>'
        f'{sub2_html}'
        f'</div>'
    )


def gfr_block(prior_m, cohorts_m, cohorts_info):
    """Renders the Good Fruit Rate card."""
    p = prior_m
    p_color = "#4a90d9"

    def fmt_val(m):
        if m["good_fruit_n"] == 0:
            return "—"
        return f'{m["good_fruit_rate"]}%'

    inner = (
        f'<div>'
        f'<div style="font-size:0.72rem;font-weight:700;color:{p_color};text-transform:uppercase;'
        f'letter-spacing:0.05em;margin-bottom:4px;">All prior users</div>'
        f'<div style="font-size:2rem;font-weight:700;color:#2c3e50;">{fmt_val(p)}</div>'
        f'<div style="font-size:0.78rem;color:#999;">n={p["good_fruit_n"]}</div>'
        f'</div>'
    )
    focal_m = cohorts_m[-1]
    focal_info = cohorts_info[-1]
    for (m, info) in zip(cohorts_m, cohorts_info):
        name, emoji, color, start, end = info
        inner += (
            f'<div>'
            f'<div style="font-size:0.72rem;font-weight:700;color:{color};text-transform:uppercase;'
            f'letter-spacing:0.05em;margin-bottom:4px;">{name}</div>'
            f'<div style="font-size:2rem;font-weight:700;color:#2c3e50;">{fmt_val(m)}</div>'
            f'<div style="font-size:0.78rem;color:#999;">n={m["good_fruit_n"]}</div>'
            f'</div>'
        )

    # Delta vs prior
    if focal_m["good_fruit_n"] > 0 and p["good_fruit_n"] > 0:
        diff = round(focal_m["good_fruit_rate"] - p["good_fruit_rate"], 1)
        if diff > 0:
            delta = f'<div style="font-size:1.3rem;font-weight:700;"><span class="pos">↑ +{diff} pts</span></div>'
        elif diff < 0:
            delta = f'<div style="font-size:1.3rem;font-weight:700;"><span class="neg">↓ {diff} pts</span></div>'
        else:
            delta = f'<div style="font-size:1.3rem;font-weight:700;"><span class="neu">— 0 pts</span></div>'
        inner += delta

    targets = (
        '<div style="margin-top:10px;font-size:0.78rem;color:#999;border-top:1px solid #f0f0f0;padding-top:8px;">'
        'Industry targets &nbsp;·&nbsp; <span style="color:#198754;font-weight:600;">Strong ≥50%</span>'
        ' &nbsp;·&nbsp; <span style="color:#6c757d;font-weight:600;">Healthy ≥40%</span>'
        ' &nbsp;·&nbsp; <span style="color:#dc3545;font-weight:600;">Watch &lt;30%</span>'
        '</div>'
    )

    return (
        f'<div class="card">'
        f'<h4>Good Fruit Rate</h4>'
        f'<div style="font-size:0.78rem;color:#888;margin-bottom:12px;">'
        f'% reaching ≥5 sessions in first 14 days &nbsp;·&nbsp; 14-day eligible users only</div>'
        f'<div style="display:flex;align-items:baseline;gap:24px;flex-wrap:wrap;">{inner}</div>'
        f'{targets}'
        f'</div>'
    )


def rot_block(prior_m, cohorts_m, focal_pct28):
    p = prior_m
    p_color = "#4a90d9"

    def fmt_rot(m):
        if m["rot_rate"] is None:
            return "—"
        return f'{m["rot_rate"]}%'

    inner = (
        f'<div>'
        f'<div style="font-size:0.72rem;font-weight:700;color:{p_color};text-transform:uppercase;'
        f'letter-spacing:0.05em;margin-bottom:4px;">All prior users</div>'
        f'<div style="font-size:2rem;font-weight:700;color:#2c3e50;">{fmt_rot(p)}</div>'
        f'<div style="font-size:0.78rem;color:#999;">n={p["rot_gf_n"]} good fruit</div>'
        f'</div>'
    )
    for (m, info) in zip(cohorts_m, focal_pct28):
        name_c, color = info
        inner += (
            f'<div>'
            f'<div style="font-size:0.72rem;font-weight:700;color:{color};text-transform:uppercase;'
            f'letter-spacing:0.05em;margin-bottom:4px;">{name_c}</div>'
            f'<div style="font-size:{"2rem" if m["rot_rate"] is not None else "1.2rem"};'
            f'font-weight:700;color:#2c3e50;">{fmt_rot(m)}</div>'
            f'<div style="font-size:0.78rem;color:#999;">n={m["rot_gf_n"]} good fruit</div>'
            f'</div>'
        )

    targets = (
        '<div style="margin-top:10px;font-size:0.78rem;color:#999;border-top:1px solid #f0f0f0;padding-top:8px;">'
        'Industry targets &nbsp;·&nbsp; <span style="color:#198754;font-weight:600;">Strong ≤15%</span>'
        ' &nbsp;·&nbsp; <span style="color:#6c757d;font-weight:600;">Healthy ≤25%</span>'
        ' &nbsp;·&nbsp; <span style="color:#dc3545;font-weight:600;">Watch &gt;35%</span>'
        '</div>'
    )

    return (
        f'<div class="card">'
        f'<h4>Rot Rate</h4>'
        f'<div style="font-size:0.78rem;color:#888;margin-bottom:12px;">'
        f'Of good-fruit users, % with 0 sessions in days 15–28 &nbsp;·&nbsp; 28-day eligible only</div>'
        f'<div style="display:flex;align-items:baseline;gap:24px;flex-wrap:wrap;">{inner}</div>'
        f'{targets}'
        f'</div>'
    )


def table_row(label, prior_v, cohort_vs, delta_fn, highlight=False):
    row_class = ' class="highlight-row"' if highlight else ''
    cells = f'<td>{label}</td><td>{prior_v}</td>'
    for v in cohort_vs:
        cells += f'<td>{v}</td>'
    # last cohort change vs second-to-last (or vs prior if only one cohort)
    cells += f'<td>{delta_fn}</td>'
    return f'<tr{row_class}>{cells}</tr>'


def fmt_n(v):
    if isinstance(v, float) and v == int(v):
        return str(int(v))
    return str(v)


def generate_html(
    cohort_name,
    cohort_start,
    analysis_date,
    prior_users,
    focal_users,
    reference_cohorts,   # list of (name, emoji, color, users) for additional reference cohorts
    prior_info,          # (name, emoji, color, start, end)
    focal_info,          # (name, emoji, color, start, end)
):
    ad = datetime.strptime(analysis_date, "%Y-%m-%d").date()

    def days_elapsed(first_date_str):
        fd = datetime.strptime(first_date_str, "%Y-%m-%d").date()
        return (ad - fd).days

    # Coverage
    def coverage(users, days):
        elig = sum(1 for u in users if days_elapsed(u["first_date"]) >= days)
        n = len(users)
        return elig, n

    foc_n = len(focal_users)
    foc_7n,  _ = coverage(focal_users, 7)
    foc_14n, _ = coverage(focal_users, 14)
    foc_28n, _ = coverage(focal_users, 28)

    # Prior metrics (all eligible)
    prior_m = compute_metrics(prior_users, days_elapsed)

    # Reference cohort metrics
    ref_ms = [compute_metrics(rc[3], days_elapsed) for rc in reference_cohorts]

    # Focal cohort metrics
    focal_m = compute_metrics(focal_users, days_elapsed)

    # Build coverage callout
    cov_parts = []
    cov_parts.append(
        f"Apple: {foc_7n}/{foc_n} have 7-day data ({pct(foc_7n, foc_n)}%), "
        f"{foc_14n}/{foc_n} have 14-day ({pct(foc_14n, foc_n)}%), "
        f"{foc_28n}/{foc_n} have 28-day / Rot Rate ({pct(foc_28n, foc_n)}%). "
        f"All prior-cohort users qualify for all windows."
    )

    # Actually use correct name
    cov_focal = (
        f"{cohort_name}: {foc_7n}/{foc_n} have 7-day data ({pct(foc_7n, foc_n)}%), "
        f"{foc_14n}/{foc_n} have 14-day ({pct(foc_14n, foc_n)}%), "
        f"{foc_28n}/{foc_n} have 28-day / Rot Rate ({pct(foc_28n, foc_n)}%). "
    )
    ref_covs = []
    for rc, rm in zip(reference_cohorts, ref_ms):
        rn, remoji, rcolor, rusers = rc
        r7, _ = coverage(rusers, 7)
        r14, _ = coverage(rusers, 14)
        r28, _ = coverage(rusers, 28)
        ref_covs.append(
            f"{rn}: {r7}/{len(rusers)} have 7-day data ({pct(r7, len(rusers))}%), "
            f"{r14}/{len(rusers)} have 14-day ({pct(r14, len(rusers))}%)"
        )
    cov_text = " &nbsp;·&nbsp; ".join(
        [f"All prior-cohort users qualify for all windows"] + ref_covs + [cov_focal.rstrip(". ")]
    )

    cov_html = (
        f'<div class="callout callout-blue"><strong>Data coverage:</strong> '
        f'{cov_focal}'
        + (" &nbsp;·&nbsp; ".join(ref_covs) + " &nbsp;·&nbsp; " if ref_covs else "")
        + f'All prior-cohort users qualify for all windows.</div>'
    )

    # Cohort size cards
    prior_start_disp = "before " + focal_info[3]  # before focal start
    n_cols = 2 + len(reference_cohorts)
    col_style = f"display:grid;grid-template-columns:{'1fr ' * n_cols};gap:{'12px' if n_cols > 2 else '16px'};margin:16px 0;"

    cards_html = f'<div style="{col_style}">'
    cards_html += (
        f'<div class="card"><h4>🔵 ALL PRIOR USERS</h4>'
        f'<div class="num">{len(prior_users):,}</div>'
        f'<div class="sub">Onboarded {prior_start_disp}</div></div>'
    )
    for rc in reference_cohorts:
        rn, remoji, rcolor, rusers = rc
        rinfo = next(d for d in COHORT_DEFS if d[0] == rn)
        r_start = rinfo[3]
        r_end = rinfo[4]
        date_range = f'{r_start}–{r_end}' if r_end else f'{r_start} or later'
        cards_html += (
            f'<div class="card"><h4>{remoji} {rn.upper()}</h4>'
            f'<div class="num">{len(rusers):,}</div>'
            f'<div class="sub">Onboarded {date_range}</div></div>'
        )
    foc_end = focal_info[4]
    foc_date_range = f'{focal_info[3] or ""}–{foc_end}' if foc_end else f'{focal_info[3]} or later'
    cards_html += (
        f'<div class="card"><h4>{focal_info[1]} {cohort_name.upper()}</h4>'
        f'<div class="num">{foc_n:,}</div>'
        f'<div class="sub">Onboarded {foc_date_range}</div></div>'
    )
    cards_html += '</div>'

    # Title
    ref_names = " vs ".join(f"<em>{rc[0]}</em>" for rc in reference_cohorts)
    if ref_names:
        title = f'Cohort Report — <em>{cohort_name}</em> vs {ref_names} vs All Prior Users'
    else:
        title = f'Cohort Report — <em>{cohort_name}</em> vs Prior Users'

    # Good Fruit Rate + Rot Rate
    all_focal_info = reference_cohorts + [(cohort_name, focal_info[1], focal_info[2], focal_users)]
    all_focal_m = ref_ms + [focal_m]
    all_cohort_defs_info = [
        next(d for d in COHORT_DEFS if d[0] == rc[0]) for rc in reference_cohorts
    ] + [focal_info]

    gfr_html = gfr_block(prior_m, all_focal_m, all_cohort_defs_info)

    rot_info = [(rc[0], rc[2]) for rc in reference_cohorts] + [(cohort_name, focal_info[2])]
    rot_html = rot_block(prior_m, all_focal_m, rot_info)

    # 7-day coverage badge
    badge7 = f'<span class="coverage-badge">{cohort_name}: {foc_7n}/{foc_n} ({pct(foc_7n, foc_n)}%)</span>'
    badge14 = f'<span class="coverage-badge">{cohort_name}: {foc_14n}/{foc_n} ({pct(foc_14n, foc_n)}%)</span>'

    # Determine column headers
    prior_label = f'All prior users (n={len(prior_users):,})'
    ref_labels = [f'{rc[0]} (n={len(rc[3]):,})' for rc in reference_cohorts]
    focal_label = f'{cohort_name} (n={focal_m["n7"]})'

    all_col_headers = [prior_label] + ref_labels + [focal_label]
    th_html = "".join(f'<th>{h}</th>' for h in all_col_headers)

    # Change column: last focal vs reference (or vs prior if no reference)
    ref_m = ref_ms[-1] if ref_ms else prior_m
    change_label = f'Change ({(reference_cohorts[-1][0] if reference_cohorts else "Prior")}→{cohort_name})'

    def all_vals_7(field_fn):
        vals = [field_fn(prior_m)]
        for rm in ref_ms:
            vals.append(field_fn(rm))
        vals.append(field_fn(focal_m))
        return vals

    def all_vals_14(field_fn):
        vals = [field_fn(prior_m)]
        for rm in ref_ms:
            vals.append(field_fn(rm))
        vals.append(field_fn(focal_m))
        return vals

    # 7-day table
    def row7(label, field_fn, delta_fn, highlight=False):
        vals = all_vals_7(field_fn)
        row_class = ' class="highlight-row"' if highlight else ''
        cells = f'<td>{label}</td>' + "".join(f'<td>{fmt_n(v)}</td>' for v in vals)
        ref_v = all_vals_7(field_fn)[-2] if len(all_vals_7(field_fn)) > 2 else all_vals_7(field_fn)[0]
        focal_v = all_vals_7(field_fn)[-1]
        cells += f'<td>{delta_fn(ref_v, focal_v)}</td>'
        return f'<tr{row_class}>{cells}</tr>'

    # Low coverage warning for 14-day
    low_cov_warn = ""
    if pct(foc_14n, foc_n) < 50:
        low_cov_warn = (
            f'<div class="callout"><strong>Heads up:</strong> Only {pct(foc_14n, foc_n)}% of '
            f'the {cohort_name} cohort has reached 14 days yet — these numbers will shift. '
            f'Check back for a more stable read.</div>'
        )

    t7_rows = ""
    for label, field_fn, delta_type in [
        ("Median sessions",    lambda m: m["med_s7"],  "pct"),
        ("Mean sessions",      lambda m: m["mean_s7"], "pct"),
        ("% with ≥1 session",  lambda m: f'{m["pct1s7"]}%', "pts"),
        ("% with ≥3 sessions", lambda m: f'{m["pct3s7"]}%', "pts"),
        ("% with 0 sessions",  lambda m: f'{m["pct0s7"]}%', "pts_inv"),
        ("Median app opens",   lambda m: m["med_o7"],  "pct"),
        ("Median days with app open", lambda m: m["med_dwa7"], "pct"),
        ("Median total app events",   lambda m: m["med_n7"],   "pct"),
    ]:
        vals = all_vals_7(field_fn)
        ref_v_raw = prior_m if not ref_ms else ref_ms[-1]
        foc_raw = focal_m

        if delta_type == "pts":
            ref_num = float(str(field_fn(ref_v_raw)).rstrip("%"))
            foc_num = float(str(field_fn(foc_raw)).rstrip("%"))
            delta_html = delta_pts(ref_num, foc_num)
        elif delta_type == "pts_inv":
            ref_num = float(str(field_fn(ref_v_raw)).rstrip("%"))
            foc_num = float(str(field_fn(foc_raw)).rstrip("%"))
            diff = round(foc_num - ref_num, 1)
            if diff < 0:
                delta_html = f'<span class="pos">↓ {diff} pts</span>'
            elif diff > 0:
                delta_html = f'<span class="neg">↑ +{diff} pts</span>'
            else:
                delta_html = '<span class="neu">— 0 pts</span>'
        else:
            ref_num = field_fn(ref_v_raw)
            foc_num = field_fn(foc_raw)
            delta_html = delta_pct(
                float(str(ref_num).rstrip("%")) if isinstance(ref_num, str) else ref_num,
                float(str(foc_num).rstrip("%")) if isinstance(foc_num, str) else foc_num
            )

        row_class = ' class="highlight-row"' if label == "Median sessions" else ''
        cells = f'<td>{label}</td>' + "".join(f'<td>{fmt_n(v)}</td>' for v in vals) + f'<td>{delta_html}</td>'
        t7_rows += f'<tr{row_class}>{cells}</tr>'

    t14_rows = ""
    for label, field_fn, delta_type in [
        ("Median sessions",    lambda m: m["med_s14"],  "pct"),
        ("Mean sessions",      lambda m: m["mean_s14"], "pct"),
        ("% with ≥1 session",  lambda m: f'{m["pct1s14"]}%', "pts"),
        ("% with ≥3 sessions", lambda m: f'{m["pct3s14"]}%', "pts"),
        ("% with 0 sessions",  lambda m: f'{m["pct0s14"]}%', "pts_inv"),
        ("Median app opens",   lambda m: m["med_o14"],  "pct"),
        ("Median days with app open", lambda m: m["med_dwa14"], "pct"),
        ("Median total app events",   lambda m: m["med_n14"],   "pct"),
    ]:
        vals = all_vals_14(field_fn)
        ref_v_raw = prior_m if not ref_ms else ref_ms[-1]
        foc_raw = focal_m

        if delta_type == "pts":
            ref_num = float(str(field_fn(ref_v_raw)).rstrip("%"))
            foc_num = float(str(field_fn(foc_raw)).rstrip("%"))
            delta_html = delta_pts(ref_num, foc_num)
        elif delta_type == "pts_inv":
            ref_num = float(str(field_fn(ref_v_raw)).rstrip("%"))
            foc_num = float(str(field_fn(foc_raw)).rstrip("%"))
            diff = round(foc_num - ref_num, 1)
            if diff < 0:
                delta_html = f'<span class="pos">↓ {diff} pts</span>'
            elif diff > 0:
                delta_html = f'<span class="neg">↑ +{diff} pts</span>'
            else:
                delta_html = '<span class="neu">— 0 pts</span>'
        else:
            ref_num = field_fn(ref_v_raw)
            foc_num = field_fn(foc_raw)
            delta_html = delta_pct(
                float(str(ref_num).rstrip("%")) if isinstance(ref_num, str) else ref_num,
                float(str(foc_num).rstrip("%")) if isinstance(foc_num, str) else foc_num
            )

        row_class = ' class="highlight-row"' if label == "Median sessions" else ''
        cells = f'<td>{label}</td>' + "".join(f'<td>{fmt_n(v)}</td>' for v in vals) + f'<td>{delta_html}</td>'
        t14_rows += f'<tr{row_class}>{cells}</tr>'

    # Bar charts
    chart_cohorts_7 = [("All prior users", "#4a90d9", prior_users)] + [
        (rc[0], rc[2], [u for u in rc[3] if days_elapsed(u["first_date"]) >= 7])
        for rc in reference_cohorts
    ] + [(cohort_name, focal_info[2], [u for u in focal_users if days_elapsed(u["first_date"]) >= 7])]

    chart_cohorts_14 = [("All prior users", "#4a90d9", prior_users)] + [
        (rc[0], rc[2], [u for u in rc[3] if days_elapsed(u["first_date"]) >= 14])
        for rc in reference_cohorts
    ] + [(cohort_name, focal_info[2], [u for u in focal_users if days_elapsed(u["first_date"]) >= 14])]

    chart7_html = render_bar_chart(
        [(n, "", c, u) for (n, c, u) in chart_cohorts_7],
        "s7"
    )
    chart14_html = render_bar_chart(
        [(n, "", c, u) for (n, c, u) in chart_cohorts_14],
        "s14"
    )

    th7 = f'<tr><th>Metric</th>{"".join(f"<th>{h}</th>" for h in all_col_headers)}<th>{change_label}</th></tr>'
    th14 = th7  # same structure for 14 day

    targets_7 = (
        '<div style="margin-top:10px;font-size:0.78rem;color:#999;border-top:1px solid #f0f0f0;padding-top:8px;">'
        '  Targets (7-day) &nbsp;·&nbsp; % with ≥1 session: '
        '<span style="color:#198754;font-weight:600;">Strong ≥85%</span> &nbsp;·&nbsp; '
        '<span style="color:#6c757d;font-weight:600;">Healthy ≥75%</span> &nbsp;·&nbsp; '
        '% with ≥3 sessions: <span style="color:#198754;font-weight:600;">Strong ≥60%</span> &nbsp;·&nbsp; '
        '<span style="color:#6c757d;font-weight:600;">Healthy ≥50%</span> &nbsp;·&nbsp; '
        'Median sessions: <span style="color:#198754;font-weight:600;">Strong ≥4</span> &nbsp;·&nbsp; '
        '<span style="color:#6c757d;font-weight:600;">Healthy ≥3</span>'
        '</div>'
    )
    targets_14 = (
        '<div style="margin-top:10px;font-size:0.78rem;color:#999;border-top:1px solid #f0f0f0;padding-top:8px;">'
        '  Targets (14-day) &nbsp;·&nbsp; % with ≥1 session: '
        '<span style="color:#198754;font-weight:600;">Strong ≥90%</span> &nbsp;·&nbsp; '
        '<span style="color:#6c757d;font-weight:600;">Healthy ≥80%</span> &nbsp;·&nbsp; '
        '% with ≥3 sessions: <span style="color:#198754;font-weight:600;">Strong ≥65%</span> &nbsp;·&nbsp; '
        '<span style="color:#6c757d;font-weight:600;">Healthy ≥55%</span> &nbsp;·&nbsp; '
        'Median sessions: <span style="color:#198754;font-weight:600;">Strong ≥7</span> &nbsp;·&nbsp; '
        '<span style="color:#6c757d;font-weight:600;">Healthy ≥5</span>'
        '</div>'
    )

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")

    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>Cohort Report: {title.replace('<em>', '').replace('</em>', '')}</title>
<style>
{CSS}
</style>
</head>
<body>
<h1>{title}</h1>
<p class="meta">
  Analysis date: {analysis_date} &nbsp;·&nbsp;
  {cohort_name} cohort start: {focal_info[3]} &nbsp;·&nbsp;
  Smartbuds app &nbsp;·&nbsp; Internal users excluded
</p>

<!-- ── COHORT SIZE CARDS ── -->
{cards_html}

{cov_html}

<!-- ── GOOD FRUIT RATE & ROT RATE ── -->
<h2>Habit + Durability Signals</h2>
<div class="callout">
  <strong>Good Fruit Rate</strong> = % of cohort who logged ≥5 sessions in their first 14 days — the habit-formation signal. &nbsp;|&nbsp;
  <strong>Rot Rate</strong> = of those good-fruit users, % who went inactive (0 sessions) in days 15–28 — the durability signal.
  Apple baseline hypothesis: ~35% Good Fruit Rate.
</div>
<div class="grid2">
{gfr_html}
{rot_html}
</div>

<!-- ── FIRST 7 DAYS ── -->
<h2>First 7 Days
  {badge7}
</h2>

<table>
  {th7}
  {t7_rows}
</table>
{targets_7}

<p style="font-size:0.85rem;color:#555;margin:16px 0 6px 0;font-weight:600;">Sessions logged — first 7 days</p>
{chart7_html}

<!-- ── FIRST 14 DAYS ── -->
<h2>First 14 Days
  {badge14}
</h2>
{low_cov_warn}

<table>
  {th14}
  {t14_rows}
</table>
{targets_14}

<p style="font-size:0.85rem;color:#555;margin:16px 0 6px 0;font-weight:600;">Sessions logged — first 14 days</p>
{chart14_html}

<footer>
  Report generated {now_str} &nbsp;·&nbsp;
  Cohort: {cohort_name} (start {focal_info[3]}) &nbsp;·&nbsp;
  Data source: Mixpanel &nbsp;·&nbsp; Excludes Internal cohort + nextsense.io emails
</footer>
</body>
</html>"""
    return html


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cohort-name", required=True)
    parser.add_argument("--cohort-start", required=True)
    parser.add_argument("--analysis-date", required=True)
    args = parser.parse_args()

    cohort_name   = args.cohort_name
    cohort_start  = args.cohort_start
    analysis_date = args.analysis_date

    print(f"\n=== Cohort Report: {cohort_name} | Analysis: {analysis_date} ===\n")

    # 1. Get internal user exclusion list
    print("Step 1: Loading user profiles...")
    profiles = get_all_profiles()
    exclude_ids = set()
    for p in profiles:
        props = p.get("$properties", {})
        email = props.get("email", "") or ""
        cohort_prop = props.get("user_cohort", "") or ""
        if cohort_prop == "Internal" or "nextsense.io" in email:
            exclude_ids.add(p["$distinct_id"])
    print(f"  Excluding {len(exclude_ids)} internal users")

    # 2. Run JQL
    print("Step 2: Running Mixpanel JQL query...")
    cache_name = f"jql_metrics_{analysis_date.replace('-', '')}"
    raw = run_jql(JQL_METRICS, {"to_date": analysis_date}, cache_name)
    print(f"  Got {len(raw)} user records from JQL")

    # 3. Filter and segment
    users = [r for r in raw if r and r.get("id") not in exclude_ids]
    print(f"  After exclusions: {len(users)} users")

    # Look up cohort boundaries
    focal_def = next(d for d in COHORT_DEFS if d[0] == cohort_name)
    focal_start = focal_def[3]
    focal_end   = focal_def[4]

    # Segment users
    def in_cohort(user, start, end):
        fd = user["first_date"]
        if start and fd < start:
            return False
        if end and fd >= end:
            return False
        return True

    # Prior users = everyone before focal cohort start
    prior_users  = [u for u in users if u["first_date"] < focal_start]

    # Focal users
    focal_users  = [u for u in users if in_cohort(u, focal_start, focal_end)]

    # Reference cohorts (all defined cohorts between prior and focal, in order)
    focal_idx = next(i for i, d in enumerate(COHORT_DEFS) if d[0] == cohort_name)
    ref_cohorts = []
    for i in range(1, focal_idx):  # COHORT_DEFS[0] is "All Prior", skip it
        d = COHORT_DEFS[i]
        rusers = [u for u in users if in_cohort(u, d[3], d[4])]
        if rusers:
            ref_cohorts.append((d[0], d[1], d[2], rusers))

    print(f"  Prior users: {len(prior_users)}")
    for rc in ref_cohorts:
        print(f"  {rc[0]}: {len(rc[3])}")
    print(f"  {cohort_name}: {len(focal_users)}")

    # 4. Generate HTML
    print("Step 3: Generating HTML report...")
    html = generate_html(
        cohort_name=cohort_name,
        cohort_start=cohort_start,
        analysis_date=analysis_date,
        prior_users=prior_users,
        focal_users=focal_users,
        reference_cohorts=ref_cohorts,
        prior_info=COHORT_DEFS[0],
        focal_info=focal_def,
    )

    # 5. Write output
    out_path = os.path.expanduser(
        f"~/Desktop/Smartbuds_{cohort_name}_Cohort_Report_{analysis_date}.html"
    )
    with open(out_path, "w") as f:
        f.write(html)
    print(f"  Written: {out_path}")
    print(f"\nDone!")


if __name__ == "__main__":
    main()
