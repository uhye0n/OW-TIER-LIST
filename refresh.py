#!/usr/bin/env python3
"""Refresh the Korea Overwatch meta tier page (low / mid / high brackets) from Nexon's official hero stats.

Usage:
    python3 refresh.py --page CURRENT.html --out NEW.html [--force]

Scrapes https://overwatch.nexon.com/hero/rate (competitive role queue, PC):
  Korea Bronze..Grandmaster+Champion (8 ranks), plus Asia/Americas/Europe Grandmaster+Champion
  (the last three only supply map-effect priors for small samples).
Rebuilds the model and rewrites the data block between the OWDATA markers of the page.

The final line is always one of:
    RESULT: UPDATED     (NEW.html written; publish it)
    RESULT: UNCHANGED   (Nexon numbers identical to the page; nothing written)
    RESULT: ERROR ...   (nothing written; do not publish)
"""
import argparse
import hashlib
import json
import math
import re
import statistics as st
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
BASE = "https://overwatch.nexon.com/hero/rate"
KST = timezone(timedelta(hours=9))
MARK_BEGIN = "/*OWDATA:BEGIN*/"
MARK_END = "/*OWDATA:END*/"

COMMON = {"input": "pc", "rq": "2", "role": "all"}
KR_LOWER = ["bronze", "silver", "gold", "platinum", "emerald", "diamond"]
GROUPS = [  # (id, short label, rank label, dataset ids) in page order
    ("high", "상위", "마스터·그랜드마스터·챔피언", ["kr_master", "kr_gm"]),
    ("mid", "중위", "플래티넘·에메랄드·다이아몬드", ["kr_platinum", "kr_emerald", "kr_diamond"]),
    ("low", "하위", "브론즈·실버·골드", ["kr_bronze", "kr_silver", "kr_gold"]),
]
DATASETS = {
    **{f"kr_{r}": {"region": "korea", "rank": r} for r in KR_LOWER},
    "kr_master": {"region": "korea", "rank": "master"},
    "kr_gm": {"region": "korea", "rank": "grandmaster"},
    "asia": {"region": "asia", "rank": "grandmaster"},
    "americas": {"region": "americas", "rank": "grandmaster"},
    "europe": {"region": "europe", "rank": "grandmaster"},
}
REGIONS = ["asia", "americas", "europe"]
ROLE_ORDER = {"damage": 0, "tank": 1, "support": 2}
ROLE_LABEL = {"damage": "공격", "tank": "돌격", "support": "지원"}
ROLE_PICK_SUM = {"damage": 200.0, "tank": 100.0, "support": 200.0}
MODE_PREF = ["쟁탈", "호위", "혼합", "밀기", "플래시포인트"]
SHORT_NAMES = {"antarctic-peninsula": "남극 반도", "shambali-monastery": "샴발리"}

K_PR, K_BR, K_WR, K0 = 40, 40, 80, 80
CAP = 780          # ban-rate quantization cannot resolve match counts at or above this
RELIABLE = 600     # below this the quantization estimate is taken as exact


def log(msg):
    print(msg, flush=True)


def die(msg):
    print(f"RESULT: ERROR {msg}", flush=True)
    sys.exit(2)


# ---------------------------------------------------------------- Nuxt payload
_SPECIAL = {"Reactive", "ShallowReactive", "Ref", "ShallowRef", "EmptyRef", "EmptyShallowRef",
            "Set", "Map", "Date", "RegExp", "BigInt", "Error", "null", "NuxtError", "Island"}


def nuxt_payload(html):
    m = re.search(r'<script[^>]*id="__NUXT_DATA__"[^>]*>(.*?)</script>', html, re.S)
    if not m:
        raise ValueError("__NUXT_DATA__ not found")
    arr = json.loads(m.group(1))
    cache = {}
    sys.setrecursionlimit(100000)

    def h(i):
        if isinstance(i, int) and i < 0:
            return None
        if i in cache:
            return cache[i]
        v = arr[i]
        if isinstance(v, list):
            if v and isinstance(v[0], str) and v[0] in _SPECIAL:
                t = v[0]
                if t in ("Reactive", "ShallowReactive", "Ref", "ShallowRef", "NuxtError", "Island"):
                    r = h(v[1])
                elif t in ("EmptyRef", "EmptyShallowRef"):
                    r = None
                elif t == "Set":
                    r = [h(x) for x in v[1:]]
                elif t in ("Map", "null"):
                    r = {}
                    for k in range(1, len(v), 2):
                        r[str(h(v[k]) if t == "Map" else v[k])] = h(v[k + 1])
                else:
                    r = v[1] if len(v) > 1 else None
            else:
                r = []
                cache[i] = r
                r.extend(h(x) for x in v)
                return r
        elif isinstance(v, dict):
            r = {}
            cache[i] = r
            for k, x in v.items():
                r[k] = h(x)
            return r
        else:
            r = v
        cache[i] = r
        return r

    return h(0)


def fetch(params, tries=4):
    q = "&".join(f"{k}={v}" for k, v in params.items())
    url = f"{BASE}?{q}"
    last = None
    for attempt in range(tries):
        try:
            r = subprocess.run(["curl", "-s", "-L", "--max-time", "40", "-A", UA, url],
                               capture_output=True, timeout=60)
            html = r.stdout.decode("utf-8", "ignore")
            data = nuxt_payload(html)["data"]
            hl = data["hero-rate-list"]
            if hl.get("status") != "success":
                raise ValueError(f"status {hl.get('status')}")
            return data
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(2 + 3 * attempt)
    raise RuntimeError(f"{url}: {last}")


def as_rows(data):
    lst = ((data.get("hero-rate-list") or {}).get("data") or {}).get("list") or []
    rows = {}
    for x in lst:
        pr, wr, br = x.get("pickRate"), x.get("winRate"), x.get("banRate")
        miss = pr is None or pr < 0 or wr is None or wr < 0
        rows[x["heroId"]] = None if miss else [float(pr), float(wr), float(max(br or 0, 0))]
    return rows, lst


# ---------------------------------------------------------------- sample sizes
def est_n(bans, nmin=3, nmax=6000):
    vals = [b for b in bans if b is not None and b > 0]
    if len(vals) < 3:
        return None
    for n in range(nmin, nmax + 1):
        ok = True
        for b in vals:
            c = round(b * n / 100)
            if c < 1 or abs(c * 100 / n - b) > 0.0501:
                ok = False
                break
        if ok:
            return n
    return None


def impute_offset(raw, ref):
    """Capped maps: same log-offset from the reference as the uncapped maps have (slope 1)."""
    unc = [m for m in raw if raw[m] is not None and raw[m] < CAP and ref.get(m)]
    a = st.median([math.log(raw[m]) - math.log(ref[m]) for m in unc]) if unc else 0.0
    out = {}
    for m, n in raw.items():
        if n is None or n >= CAP:
            out[m] = max(800, round(math.exp(a + math.log(ref[m])))) if ref.get(m) else 800
        else:
            out[m] = n
    return out


def impute_regress(raw, ref):
    """Log-linear fit on maps whose estimate is exact; applied to maps at or above RELIABLE."""
    rel = [m for m in raw if raw[m] is not None and raw[m] < RELIABLE and ref.get(m)]
    if len(rel) >= 5:
        xs = [math.log(ref[m]) for m in rel]
        ys = [math.log(raw[m]) for m in rel]
        mx, my = st.mean(xs), st.mean(ys)
        sxx = sum((x - mx) ** 2 for x in xs)
        beta = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / sxx if sxx > 0 else 1.0
        a = my - beta * mx
    elif rel:
        beta, a = 1.0, st.median([math.log(raw[m]) - math.log(ref[m]) for m in rel])
    else:
        beta, a = 1.0, math.log(2.8)
    out = {}
    for m, n in raw.items():
        if n is not None and n < RELIABLE:
            out[m] = n
            continue
        pred = math.exp(a + beta * math.log(ref[m])) if ref.get(m) else 800
        lb = 800 if (n is None or n >= 800) else n
        out[m] = int(round(max(lb, pred)))
    return out


# ---------------------------------------------------------------- model helpers
def shr(wr, g, prior, k=K_WR):
    if wr is None or g <= 0:
        return prior
    return (g * wr + k * prior) / (g + k)


def cell(rows, h):
    v = rows.get(h)
    if v is None:
        return {"pr": 0.0, "wr": None, "br": 0.0, "miss": True}
    return {"pr": v[0], "wr": v[1], "br": v[2], "miss": False}


def comps(c, C):
    prav = max(0.1, c["pr"]) / max(0.3, 1 - c["br"] / 100)
    return ((c["wr"] - 50) / C["wr_sd"], (math.log(prav) - C["pr_mu"]) / C["pr_sd"],
            (math.log(1 + max(0.0, c["br"])) - C["br_mu"]) / C["br_sd"])


def tier(x):
    x = round(x)
    return "S" if x >= 62 else "A" if x >= 55 else "B" if x >= 46 else "C" if x >= 40 else "D"


def r2(x):
    return None if x is None else round(x, 2)


def rank_shares(mix, parts, hero_ids):
    """Match share of each rank: the all-ranks pick and ban rates are a match-weighted
    average of the per-rank rates, so solve mix = sum_r w_r * part_r with w >= 0, sum w = 1."""
    keys = list(parts)
    A, b = [], []
    for h in hero_ids:
        if not mix.get(h) or not all(parts[k].get(h) for k in keys):
            continue
        for i in (0, 2):
            A.append([parts[k][h][i] for k in keys])
            b.append(mix[h][i])
    A.append([100.0] * len(keys))
    b.append(100.0)
    n = len(keys)
    H = [[sum(r[i] * r[j] for r in A) for j in range(n)] for i in range(n)]
    c = [sum(r[i] * y for r, y in zip(A, b)) for i in range(n)]
    w = [1.0 / n] * n
    for _ in range(5000):
        for j in range(n):
            g = sum(H[j][k] * w[k] for k in range(n)) - c[j]
            w[j] = max(0.0, w[j] - g / H[j][j])
    t = sum(w) or 1.0
    return {k: w[i] / t for i, k in enumerate(keys)}


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--page", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    try:
        page = open(args.page, encoding="utf-8").read()
    except OSError as e:
        die(f"cannot read page: {e}")
    blk = re.search(re.escape(MARK_BEGIN) + r"(.*?)" + re.escape(MARK_END), page, re.S)
    if not blk:
        die("OWDATA markers not found in page")
    try:
        old = json.loads(blk.group(1))
    except ValueError:
        old = None
    old_hash = ((old or {}).get("meta") or {}).get("rawHash")

    t0 = time.time()
    log("Fetching filters and Korea all-maps baselines...")
    try:
        first = fetch(dict(COMMON, map="all", **DATASETS["kr_master"]))
        default_all = fetch(dict(COMMON, map="all", region="korea"))
    except RuntimeError as e:
        die(f"fetch failed: {e}")
    filters = (first.get("hero-rate-filters") or {}).get("data") or {}
    fmaps = filters.get("maps") or []
    ranks = {x.get("value") for x in filters.get("ranks") or []}
    regions = {x.get("value") for x in filters.get("regions") or []}
    queues = {str(x.get("value")) for x in filters.get("rulesetQueues") or []}
    if not {"master", "grandmaster"} <= ranks or not {"korea", *REGIONS} <= regions or "2" not in queues:
        die(f"filter options changed: ranks={sorted(ranks)} regions={sorted(regions)} queues={sorted(queues)}")
    if str(first["hero-rate-list"].get("rq")) != "2":
        die("response is not competitive role queue")
    modes = {x["value"]: x["name"] for x in fmaps if x.get("parentValue") == "battlefield"}
    map_list = [{"id": x["value"], "name": x["name"], "mode": modes[x["parentValue"]]}
                for x in fmaps if x.get("parentValue") in modes]
    if len(map_list) < 10:
        die(f"only {len(map_list)} maps listed")
    sub_names = {x["value"]: x["name"] for x in filters.get("roles") or [] if x.get("parentValue")}

    # ---- scrape everything
    jobs = [(ds, m) for ds in DATASETS for m in ["all"] + [x["id"] for x in map_list]]
    log(f"Fetching {len(jobs)} stat pages from Nexon...")
    raw, hero_meta = {ds: {} for ds in DATASETS}, {}

    def work(job):
        ds, m = job
        if ds == "kr_master" and m == "all":
            return job, first
        return job, fetch(dict(COMMON, map=m, **DATASETS[ds]))

    try:
        with ThreadPoolExecutor(max_workers=4) as ex:
            for (ds, m), data in ex.map(work, jobs):
                rows, lst = as_rows(data)
                raw[ds]["all-maps" if m == "all" else m] = rows
                for x in lst:
                    hero_meta.setdefault(x["heroId"], x)
    except RuntimeError as e:
        die(f"fetch failed: {e}")
    log(f"Fetched in {time.time() - t0:.0f}s")

    default_rows, _ = as_rows(default_all)
    kr_sets = [ds for ds in DATASETS if ds.startswith("kr_")]
    alls = [json.dumps(raw[ds]["all-maps"], sort_keys=True) for ds in kr_sets]
    if len(set(alls)) < len(alls) or json.dumps(default_rows, sort_keys=True) in alls:
        die("rank filter was not applied (brackets returned identical data)")

    # ---- heroes (ordered by role, then name) and validation
    hero_ids = sorted(hero_meta, key=lambda h: (ROLE_ORDER.get(hero_meta[h]["role"], 9), hero_meta[h]["name"]))
    hero_ids = [h for h in hero_ids if hero_meta[h]["role"] in ROLE_ORDER]
    if len(hero_ids) < 40:
        die(f"only {len(hero_ids)} heroes")
    for ds in DATASETS:
        rows = raw[ds]["all-maps"]
        for role, target in ROLE_PICK_SUM.items():
            s = sum(rows[h][0] for h in hero_ids if hero_meta[h]["role"] == role and rows.get(h))
            if abs(s - target) > 3:
                die(f"{ds} {role} pick rates sum to {s:.1f}, expected {target:.0f}")
    maps = []
    for x in map_list:
        if any(raw[ds][x["id"]].get(h) for ds in kr_sets for h in hero_ids):
            maps.append(x)
        else:
            log(f"Skipping {x['name']}: no competitive data")
    map_ids = [x["id"] for x in maps]
    ALL = ["all-maps"] + map_ids

    # ---- change detection
    canon = {ds: {m: {h: raw[ds][m].get(h) for h in hero_ids} for m in ALL} for ds in DATASETS}
    raw_hash = hashlib.sha256(json.dumps(canon, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:16]
    if raw_hash == old_hash and not args.force:
        log(f"Nexon numbers match the published page (hash {raw_hash}).")
        print("RESULT: UNCHANGED", flush=True)
        return

    # ---- match counts
    nraw = {ds: {m: est_n([(raw[ds][m].get(h) or [0, 0, 0])[2] for h in hero_ids]) for m in map_ids} for ds in DATASETS}
    kr_gm_n = impute_offset(nraw["kr_gm"], nraw["asia"])
    asia_n = impute_offset(nraw["asia"], kr_gm_n)
    N = {"asia": asia_n,
         "americas": impute_offset(nraw["americas"], asia_n),
         "europe": impute_offset(nraw["europe"], asia_n),
         "kr_gm": kr_gm_n,
         "kr_master": impute_regress(nraw["kr_master"], kr_gm_n)}
    shares = rank_shares(default_rows, {ds: raw[ds]["all-maps"] for ds in kr_sets}, hero_ids)
    log("Estimated Korea match shares: " + ", ".join(f"{ds[3:]} {shares[ds] * 100:.1f}%" for ds in kr_sets))
    for ds in [f"kr_{r}" for r in KR_LOWER]:
        scale = shares[ds] / max(shares["kr_gm"], 1e-6)
        N[ds] = {m: (nraw[ds][m] if nraw[ds][m] is not None and nraw[ds][m] < RELIABLE
                     else int(max(800, round(kr_gm_n[m] * scale)))) for m in map_ids}
    for ds in N:
        N[ds]["all-maps"] = sum(N[ds][m] for m in map_ids)

    # ---- global (3 regions, GM+Champ) map effects
    NG = {m: sum(N[r][m] for r in REGIONS) for m in ALL}

    def pooled(m, h):
        cs = {r: cell(raw[r][m], h) for r in REGIONS}
        pr = sum(N[r][m] * cs[r]["pr"] for r in REGIONS) / NG[m]
        br = sum(N[r][m] * cs[r]["br"] for r in REGIONS) / NG[m]
        gs = [(N[r][m] * cs[r]["pr"], cs[r]["wr"]) for r in REGIONS if cs[r]["wr"] is not None]
        sg = sum(g for g, _ in gs)
        return pr, br, (sum(g * w for g, w in gs) / sg if sg > 0 else None)

    GA = {"all-maps": {}}
    for h in hero_ids:
        pr, br, wr = pooled("all-maps", h)
        GA["all-maps"][h] = {"pr": pr, "br": br, "wr": shr(wr, 2 * NG["all-maps"] * pr / 100, 50, K0)}
    eff = {}
    for m in map_ids:
        GA[m], eff[m] = {}, {}
        for h in hero_ids:
            pr, br, wr = pooled(m, h)
            P = GA["all-maps"][h]
            pra = (NG[m] * pr + K_PR * P["pr"]) / (NG[m] + K_PR)
            bra = (NG[m] * br + K_BR * P["br"]) / (NG[m] + K_BR)
            wra = shr(wr, 2 * NG[m] * pr / 100, P["wr"])
            GA[m][h] = {"pr": pra, "br": bra, "wr": wra}
            eff[m][h] = {"pr": pra / max(P["pr"], 0.05), "wr": wra - P["wr"], "br": (bra + 1) / (P["br"] + 1)}

    CONST = {}
    for role in ROLE_ORDER:
        hs = [h for h in hero_ids if hero_meta[h]["role"] == role]
        xs = []
        for m in map_ids:
            for h in hs:
                c = GA[m][h]
                prav = max(0.1, c["pr"]) / max(0.3, 1 - c["br"] / 100)
                xs.append((c["wr"] - 50, math.log(prav), math.log(1 + max(0.0, c["br"]))))
        cols = list(zip(*xs))
        CONST[role] = {"wr_sd": round(st.pstdev(cols[0]), 3), "pr_mu": round(st.mean(cols[1]), 4),
                       "pr_sd": round(st.pstdev(cols[1]), 3), "br_mu": round(st.mean(cols[2]), 3),
                       "br_sd": round(st.pstdev(cols[2]), 3), "n": len(hs)}

    # ---- Korea bracket groups = member ranks weighted by estimated matches
    def merge(dss):
        NP, KR = {}, {}
        for m in ALL:
            ns = [N[ds][m] for ds in dss]
            NP[m] = sum(ns)
            KR[m] = {}
            for h in hero_ids:
                cs = [cell(raw[ds][m], h) for ds in dss]
                gs = [(n * c["pr"], c["wr"]) for n, c in zip(ns, cs) if c["wr"] is not None]
                sg = sum(g for g, _ in gs)
                KR[m][h] = {"pr": sum(n * c["pr"] for n, c in zip(ns, cs)) / NP[m],
                            "br": sum(n * c["br"] for n, c in zip(ns, cs)) / NP[m],
                            "wr": sum(g * w for g, w in gs) / sg if sg > 0 else None,
                            "miss": all(c["miss"] for c in cs)}
        return NP, KR

    def smooth(NP, KR):
        SM = {"all-maps": {}}
        for h in hero_ids:
            c = KR["all-maps"][h]
            g = 2 * NP["all-maps"] * c["pr"] / 100
            SM["all-maps"][h] = {"pr": c["pr"], "br": c["br"], "wr": shr(c["wr"], g, GA["all-maps"][h]["wr"], K0), "g": g, "raw": c}
        for m in map_ids:
            SM[m] = {}
            for h in hero_ids:
                c, P, e = KR[m][h], SM["all-maps"][h], eff[m][h]
                g = 2 * NP[m] * c["pr"] / 100
                prp, wrp, brp = P["pr"] * e["pr"], P["wr"] + e["wr"], max(0.0, (P["br"] + 1) * e["br"] - 1)
                SM[m][h] = {"pr": (NP[m] * c["pr"] + K_PR * prp) / (NP[m] + K_PR),
                            "br": (NP[m] * c["br"] + K_BR * brp) / (NP[m] + K_BR),
                            "wr": shr(c["wr"], g, wrp), "g": g, "raw": c}
        return SM

    OUT = {}
    for gid, glabel, granks, dss in GROUPS:
        NP, KR = merge(dss)
        OUT[gid] = (glabel, granks, NP, smooth(NP, KR))

    # ---- export
    now = datetime.now(KST)
    roles_out = {}
    for role in ROLE_ORDER:
        n = sum(1 for h in hero_ids if hero_meta[h]["role"] == role)
        roles_out[role] = {"label": ROLE_LABEL[role], "avg": round(ROLE_PICK_SUM[role] / n, 3), "c": CONST[role]}
    mode_order = [x for x in MODE_PREF if any(mp["mode"] == x for mp in maps)]
    mode_order += [x for x in dict.fromkeys(mp["mode"] for mp in maps) if x not in mode_order]

    def short(mp):
        if mp["id"] in SHORT_NAMES:
            return SHORT_NAMES[mp["id"]]
        return mp["name"].split(":")[-1].strip()

    def cells(SM):
        return {m: [[r2(SM[m][h]["pr"]), r2(SM[m][h]["wr"]), r2(SM[m][h]["br"]),
                     r2(SM[m][h]["raw"]["pr"]), r2(SM[m][h]["raw"]["wr"]), r2(SM[m][h]["raw"]["br"]),
                     round(SM[m][h]["g"], 1), 1 if SM[m][h]["raw"]["miss"] else 0] for h in hero_ids] for m in ALL}

    data = {
        "meta": {"collectedAt": now.strftime("%Y-%m-%dT%H:%M+09:00"), "rawHash": raw_hash, "source": BASE,
                 "shares": {ds[3:]: round(shares[ds] * 100, 2) for ds in kr_sets}},
        "roles": roles_out,
        "modes": mode_order,
        "heroes": [{"id": h, "name": hero_meta[h]["name"], "role": hero_meta[h]["role"],
                    "sub": sub_names.get(hero_meta[h].get("subrole"), hero_meta[h].get("subrole") or "")} for h in hero_ids],
        "maps": [{"id": mp["id"], "name": mp["name"], "short": short(mp), "mode": mp["mode"]} for mp in maps],
        "brackets": [{"id": gid, "label": glabel, "ranks": granks, "N": {m: int(round(NP[m])) for m in ALL}, "cells": cells(SM)}
                     for gid, (glabel, granks, NP, SM) in OUT.items()],
    }
    blob = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    if "</" in blob or "*/" in blob:
        die("data contains a sequence that would break the page script")
    new_page = page[:blk.start(1)] + blob + page[blk.end(1):]
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(new_page)

    # ---- summary (default weights 50/30/20, same formula as the page)
    for gid, (glabel, granks, NP, SM) in OUT.items():
        parts = []
        for role in ROLE_ORDER:
            hs = [h for h in hero_ids if hero_meta[h]["role"] == role]
            C = CONST[role]
            comp = {(m, h): sum(w * x for w, x in zip((0.5, 0.3, 0.2), comps(SM[m][h], C))) for m in ALL for h in hs}
            pool = [comp[(m, h)] for m in map_ids for h in hs]
            mu, sd = st.mean(pool), st.pstdev(pool) or 1
            mi = {h: 50 + 10 * (comp[("all-maps", h)] - mu) / sd for h in hs}
            top = [hero_meta[h]["name"] for h in sorted(hs, key=lambda h: -mi[h]) if tier(mi[h]) == "S"]
            parts.append(f"{ROLE_LABEL[role]} S: {', '.join(top) or '없음'}")
        log(f"SUMMARY {glabel}({granks}): 추정 {int(NP['all-maps']):,}경기 | " + " | ".join(parts))
    log(f"Maps {len(map_ids)}, heroes {len(hero_ids)}. Wrote {args.out} (hash {raw_hash}, previous {old_hash})")
    print("RESULT: UPDATED", flush=True)


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:  # noqa: BLE001
        print(f"RESULT: ERROR unexpected {type(e).__name__}: {e}", flush=True)
        sys.exit(2)
