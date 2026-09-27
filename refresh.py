#!/usr/bin/env python3
"""Refresh the Overwatch tier list data for every server.

Usage:
    python3 refresh.py [--out data] [--history history] [--force]

Sources (competitive role queue, PC):
  Korea                      -> https://overwatch.nexon.com/hero/rate       (8 ranks + all ranks)
  Asia / Americas / Europe   -> https://overwatch.blizzard.com/ko-kr/rates/ (8 ranks + all ranks)

Writes data/index.json, data/<server>.json, history/<server>.json and archive/<server>.json
(one entry per day, KST, for the trend charts; --archive-only rebuilds today's entry from data/).
The final line is always one of:
    RESULT: UPDATED     (files written; commit them)
    RESULT: UNCHANGED   (source numbers identical to the last run; nothing written)
    RESULT: ERROR ...   (nothing written)
"""
import argparse
import hashlib
import json
import math
import os
import re
import statistics as st
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
NEXON = "https://overwatch.nexon.com/hero/rate"
BLIZZ = "https://overwatch.blizzard.com/ko-kr/rates/data/"
KST = timezone(timedelta(hours=9))

SERVERS = [  # id, label, source, source region value
    ("kr", "한국", "nexon", "korea"),
    ("asia", "아시아", "blizzard", "Asia"),
    ("americas", "아메리카", "blizzard", "Americas"),
    ("europe", "유럽", "blizzard", "Europe"),
]
PRIOR_SERVERS = ["asia", "americas", "europe"]   # pooled for map-effect priors
RANKS = ["bronze", "silver", "gold", "platinum", "emerald", "diamond", "master", "grandmaster"]
GROUPS = [  # id, label, rank label, ranks
    ("all", "전체", "브론즈~챔피언", RANKS),
    ("high", "상위", "마스터·그랜드마스터·챔피언", ["master", "grandmaster"]),
    ("mid", "중위", "플래티넘·에메랄드·다이아몬드", ["platinum", "emerald", "diamond"]),
    ("low", "하위", "브론즈·실버·골드", ["bronze", "silver", "gold"]),
]
ROLE_ORDER = {"tank": 0, "damage": 1, "support": 2}
ROLE_LABEL = {"tank": "돌격", "damage": "공격", "support": "지원"}
ROLE_PICK_SUM = {"tank": 100.0, "damage": 200.0, "support": 200.0}
MODE_PREF = ["쟁탈", "호위", "혼합", "밀기", "플래시포인트"]
SHORT_NAMES = {"antarctic-peninsula": "남극 반도", "shambali-monastery": "샴발리"}

K0 = 80           # prior weight (games) for a hero's all-maps win rate
K_BOUNDS = (5, 3000)  # clamp for the data-estimated prior weights
CAP = 780          # ban-rate quantization cannot resolve match counts at or above this
RELIABLE = 600     # below this the quantization estimate is taken as exact
PATCH_DROP = 0.7   # total matches falling below this share of the previous run = stats were reset
HISTORY_KEEP = 40


def log(msg):
    print(msg, flush=True)


def die(msg):
    print(f"RESULT: ERROR {msg}", flush=True)
    sys.exit(2)


# ---------------------------------------------------------------- fetching
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


def curl(url, headers=()):
    cmd = ["curl", "-s", "-L", "--max-time", "40", "-A", UA]
    for hd in headers:
        cmd += ["-H", hd]
    r = subprocess.run(cmd + [url], capture_output=True, timeout=60)
    return r.stdout.decode("utf-8", "ignore")


def retry(fn, what, tries=5):
    last = None
    for attempt in range(tries):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(2 + 4 * attempt)
    raise RuntimeError(f"{what}: {last}")


def nexon_fetch(region, rank, mp):
    """Returns (rows, heroes_meta, filters)."""
    q = f"input=pc&rq=2&role=all&region={region}&map={'all' if mp == 'all-maps' else mp}"
    if rank:
        q += f"&rank={rank}"

    def go():
        data = nuxt_payload(curl(f"{NEXON}?{q}"))["data"]
        hl = data["hero-rate-list"]
        if hl.get("status") != "success" or str(hl.get("rq")) != "2":
            raise ValueError(f"bad response {hl.get('status')} rq={hl.get('rq')}")
        lst = (hl.get("data") or {}).get("list") or []
        rows, meta = {}, {}
        for x in lst:
            pr, wr, br = x.get("pickRate"), x.get("winRate"), x.get("banRate")
            miss = pr is None or pr < 0 or wr is None or wr < 0
            rows[x["heroId"]] = None if miss else [float(pr), float(wr), float(max(br or 0, 0))]
            meta[x["heroId"]] = {"name": x["name"], "role": x["role"], "sub": x.get("subrole"), "img": x.get("thumbnailUrl")}
        return rows, meta, (data.get("hero-rate-filters") or {}).get("data") or {}

    return retry(go, f"nexon {region}/{rank}/{mp}")


def blizz_fetch(region, rank, mp):
    tier = rank.capitalize() if rank else "All"
    q = f"input=PC&map={mp}&region={region}&role=All&rq=2&tier={tier}"

    def go():
        d = json.loads(curl(f"{BLIZZ}?{q}", ["X-Requested-With: XMLHttpRequest", "Accept: application/json"]))
        sel = d["rates"]["selected"]
        if sel.get("map") != mp or sel.get("region") != region or sel.get("tier") != tier or str(sel.get("rq")) != "2":
            raise ValueError(f"filters not applied: {sel}")
        rows, meta = {}, {}
        for x in d["rates"]["rates"]:
            c = x["cells"]
            pr, wr, br = c.get("pickrate"), c.get("winrate"), c.get("banrate")
            miss = pr is None or pr < 0 or wr is None or wr < 0
            rows[x["id"]] = None if miss else [float(pr), float(wr), float(max(br or 0, 0))]
            meta[x["id"]] = {"name": c.get("name"), "role": (x["hero"].get("role") or "").lower(),
                             "sub": x["hero"].get("subrole"), "img": x["hero"].get("portrait")}
        return rows, meta, None

    return retry(go, f"blizzard {region}/{tier}/{mp}")


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


def impute_regress(raw, ref, fallback_ratio):
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
        beta, a = 1.0, math.log(max(fallback_ratio, 0.05))
    out = {}
    for m, n in raw.items():
        if n is not None and n < RELIABLE:
            out[m] = n
            continue
        pred = math.exp(a + beta * math.log(ref[m])) if ref.get(m) else 800
        lb = 800 if (n is None or n >= 800) else n
        out[m] = int(round(max(lb, pred)))
    return out


def rank_shares(mix, parts, hero_ids, iters=5000):
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
    for _ in range(iters):
        for j in range(n):
            g = sum(H[j][k] * w[k] for k in range(n)) - c[j]
            w[j] = max(0.0, w[j] - g / H[j][j])
    t = sum(w) or 1.0
    return {k: w[i] / t for i, k in enumerate(keys)}


# ---------------------------------------------------------------- model helpers
def mirror(wr, pr):
    """Win rate outside mirror matches. When both teams field the hero the game counts as one win and one
    loss, pulling the published rate toward 50; with per-team pick share p (independent picks) a share p of
    the hero's games are mirrors, so the non-mirror rate is 50 + (wr - 50) / (1 - p)."""
    p = min(max(pr, 0.0) / 100, 0.9)
    return 50 + (wr - 50) / (1 - p)


def clampk(k):
    return min(K_BOUNDS[1], max(K_BOUNDS[0], k))


def shr(wr, g, prior, k=K0):
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


def r1(x):
    return None if x is None else round(x, 1)


def load_json(path, default=None):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


def write_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, path)


# ---------------------------------------------------------------- daily archive
def default_mi(exp, bracket):
    """Default-weight meta index from an exported server file, the same computation the page does
    (smoothed cells, per role, scaled on the pool of every map x hero)."""
    heroes, maps = exp["heroes"], [m["id"] for m in exp["maps"]]
    cells, out = bracket["cells"], {}
    for role in ROLE_ORDER:
        C = bracket["c"][role]
        idx = [i for i, h in enumerate(heroes) if h["role"] == role]

        def comp(c):
            prav = max(0.1, c[0]) / max(0.3, 1 - c[2] / 100)
            z = ((c[1] - 50) / C["wr_sd"], (math.log(prav) - C["pr_mu"]) / C["pr_sd"],
                 (math.log(1 + max(0.0, c[2])) - C["br_mu"]) / C["br_sd"])
            return 0.5 * z[0] + 0.3 * z[1] + 0.2 * z[2]
        pool = [comp(cells[m][i]) for m in maps for i in idx] or [comp(cells["all-maps"][i]) for i in idx]
        mu, sd = st.mean(pool), (st.pstdev(pool) or 1.0)
        for i in idx:
            out[heroes[i]["id"]] = round(50 + 10 * (comp(cells["all-maps"][i]) - mu) / sd)
    return out


def update_archive(path, exp):
    """Keep one all-maps entry per day (the day's latest data wins): per bracket the estimated matches and,
    per hero, [meta index, pick, win, ban] with the source (unsmoothed, bracket-merged) rates."""
    arc = load_json(path, None) or {"server": exp["meta"]["server"], "days": [], "patches": []}
    at = exp["meta"]["collectedAt"]
    day = at[:10]
    entry = {"d": day, "at": at, "b": {}}
    for b in exp["brackets"]:
        mi = default_mi(exp, b)
        h = {}
        for i, hero in enumerate(exp["heroes"]):
            c = b["cells"]["all-maps"][i]
            if c[7]:
                continue
            h[hero["id"]] = [mi[hero["id"]], c[3], c[4], c[5]]
        entry["b"][b["id"]] = {"n": b["N"].get("all-maps"), "h": h}
    arc["label"] = exp["meta"]["label"]
    arc["heroes"] = {x["id"]: {"name": x["name"], "role": x["role"], "img": x["img"]} for x in exp["heroes"]}
    arc["days"] = [x for x in arc.get("days", []) if x["d"] != day] + [entry]
    arc["days"].sort(key=lambda x: x["d"])
    ps = exp["meta"].get("patchStart")
    if ps and ps not in arc.setdefault("patches", []):
        arc["patches"].append(ps)
        arc["patches"].sort()
    write_json(path, arc)


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data")
    ap.add_argument("--history", default="history")
    ap.add_argument("--archive", default="archive")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--archive-only", action="store_true", help="only rebuild today's archive entries from data/")
    args = ap.parse_args()

    if args.archive_only:
        idx = load_json(os.path.join(args.out, "index.json"), {}) or {}
        for sv in idx.get("servers", []):
            exp = load_json(os.path.join(args.out, f"{sv['id']}.json"))
            if exp:
                update_archive(os.path.join(args.archive, f"{sv['id']}.json"), exp)
                log(f"archived {sv['id']}")
        print("RESULT: UPDATED", flush=True)
        return

    t0 = time.time()
    log("Reading Nexon filters...")
    try:
        _, _, filters = nexon_fetch("korea", "master", "all-maps")
    except RuntimeError as e:
        die(f"fetch failed: {e}")
    fmaps = filters.get("maps") or []
    ranks_ok = {x.get("value") for x in filters.get("ranks") or []}
    if not set(RANKS) <= ranks_ok:
        die(f"Nexon rank options changed: {sorted(ranks_ok)}")
    modes = {x["value"]: x["name"] for x in fmaps if x.get("parentValue") == "battlefield"}
    map_list = [{"id": x["value"], "name": x["name"], "mode": modes[x["parentValue"]]}
                for x in fmaps if x.get("parentValue") in modes]
    if len(map_list) < 10:
        die(f"only {len(map_list)} maps listed")
    sub_names = {x["value"]: x["name"] for x in filters.get("roles") or [] if x.get("parentValue")}
    all_maps = ["all-maps"] + [x["id"] for x in map_list]

    # ---- scrape: every server x (8 ranks + all ranks) x (all maps + each map)
    jobs = [(s, r, m) for s, _, _, _ in SERVERS for r in RANKS + [None] for m in all_maps]
    src = {s: (kind, reg) for s, _, kind, reg in SERVERS}
    log(f"Fetching {len(jobs)} stat pages...")
    raw = {s: {r: {} for r in RANKS + ["all"]} for s, _, _, _ in SERVERS}
    meta_kr, meta_bz = {}, {}

    def work(job):
        s, r, m = job
        kind, reg = src[s]
        return job, (nexon_fetch if kind == "nexon" else blizz_fetch)(reg, r, m)

    try:
        with ThreadPoolExecutor(max_workers=6) as ex:
            for (s, r, m), (rows, meta, _) in ex.map(work, jobs):
                raw[s][r or "all"][m] = rows
                (meta_kr if s == "kr" else meta_bz).update(meta)
    except RuntimeError as e:
        die(f"fetch failed: {e}")
    log(f"Fetched in {time.time() - t0:.0f}s")

    # ---- heroes and validation
    hero_ids = [h for h in meta_kr if meta_kr[h]["role"] in ROLE_ORDER]
    hero_ids.sort(key=lambda h: (ROLE_ORDER[meta_kr[h]["role"]], meta_kr[h]["name"]))
    if len(hero_ids) < 40:
        die(f"only {len(hero_ids)} heroes")
    role_of = {h: meta_kr[h]["role"] for h in hero_ids}
    for s, _, _, _ in SERVERS:
        alls = [json.dumps(raw[s][r]["all-maps"], sort_keys=True) for r in RANKS + ["all"]]
        if len(set(alls)) < len(alls):
            die(f"{s}: rank filter was not applied (identical data for different ranks)")
        for r in RANKS:
            rows = raw[s][r]["all-maps"]
            for role, target in ROLE_PICK_SUM.items():
                tot = sum(rows[h][0] for h in hero_ids if role_of[h] == role and rows.get(h))
                if abs(tot - target) > 3:
                    die(f"{s} {r} {role} pick rates sum to {tot:.1f}, expected {target:.0f}")
    maps = [x for x in map_list
            if any(raw[s][r][x["id"]].get(h) for s, _, _, _ in SERVERS for r in RANKS for h in hero_ids)]
    for x in map_list:
        if x not in maps:
            log(f"Skipping {x['name']}: no competitive data")
    map_ids = [x["id"] for x in maps]
    ALL = ["all-maps"] + map_ids

    # ---- change detection
    def canon(s):
        return {r: {m: {h: raw[s][r][m].get(h) for h in hero_ids} for m in ALL} for r in RANKS + ["all"]}
    hashes = {s: hashlib.sha256(json.dumps(canon(s), sort_keys=True).encode()).hexdigest()[:16]
              for s, _, _, _ in SERVERS}
    prev_index = load_json(os.path.join(args.out, "index.json"), {}) or {}
    prev_hashes = {x["id"]: x.get("hash") for x in prev_index.get("servers", [])}
    if all(prev_hashes.get(s) == h for s, h in hashes.items()) and not args.force:
        log("Source numbers match the published data.")
        print("RESULT: UNCHANGED", flush=True)
        return

    # ---- match counts per server x rank x map
    nraw = {s: {r: {m: est_n([(raw[s][r][m].get(h) or [0, 0, 0])[2] for h in hero_ids]) for m in map_ids}
                for r in RANKS} for s, _, _, _ in SERVERS}
    shares = {s: rank_shares(raw[s]["all"]["all-maps"], {r: raw[s][r]["all-maps"] for r in RANKS}, hero_ids)
              for s, _, _, _ in SERVERS}
    for s, label, _, _ in SERVERS:
        log(f"{label} match shares: " + ", ".join(f"{r} {shares[s][r] * 100:.1f}%" for r in RANKS))
    gm = {}
    gm["kr"] = impute_offset(nraw["kr"]["grandmaster"], nraw["asia"]["grandmaster"])
    gm["asia"] = impute_offset(nraw["asia"]["grandmaster"], gm["kr"])
    gm["americas"] = impute_offset(nraw["americas"]["grandmaster"], gm["asia"])
    gm["europe"] = impute_offset(nraw["europe"]["grandmaster"], gm["asia"])
    N = {}
    for s, _, _, _ in SERVERS:
        sh = shares[s]
        N[s] = {"grandmaster": gm[s],
                "master": impute_regress(nraw[s]["master"], gm[s], sh["master"] / max(sh["grandmaster"], 1e-6))}
        for r in RANKS[:6]:
            scale = sh[r] / max(sh["grandmaster"], 1e-6)
            N[s][r] = {m: (nraw[s][r][m] if nraw[s][r][m] is not None and nraw[s][r][m] < RELIABLE
                           else int(max(800, round(gm[s][m] * scale)))) for m in map_ids}
        # the six lower ranks: keep each map's total, but split it by that map's own rank mix (solved from the
        # official all-ranks numbers of the map) instead of the all-maps mix
        low = RANKS[:6]
        for m in map_ids:
            sm = rank_shares(raw[s]["all"][m], {r: raw[s][r][m] for r in RANKS}, hero_ids, iters=2000)
            tot_low, w_low = sum(N[s][r][m] for r in low), sum(sm[r] for r in low)
            if w_low > 0.2:
                for r in low:
                    N[s][r][m] = max(1, int(round(tot_low * sm[r] / w_low)))
        for r in RANKS:
            N[s][r]["all-maps"] = sum(N[s][r][m] for m in map_ids)

    # ---- bracket groups = member ranks weighted by estimated matches
    def merge(s, ranks):
        NP, KR = {}, {}
        for m in ALL:
            ns = [N[s][r][m] for r in ranks]
            NP[m] = sum(ns)
            KR[m] = {}
            for h in hero_ids:
                cs = [cell(raw[s][r][m], h) for r in ranks]
                gs = [(n * c["pr"], c["wr"]) for n, c in zip(ns, cs) if c["wr"] is not None]
                sg = sum(g for g, _ in gs)
                KR[m][h] = {"pr": sum(n * c["pr"] for n, c in zip(ns, cs)) / NP[m],
                            "br": sum(n * c["br"] for n, c in zip(ns, cs)) / NP[m],
                            "wr": sum(g * w for g, w in gs) / sg if sg > 0 else None,
                            "miss": all(c["miss"] for c in cs)}
        return NP, KR

    MERGED = {(s, gid): merge(s, ranks) for s, _, _, _ in SERVERS for gid, _, _, ranks in GROUPS}
    # the all-ranks bracket uses the official all-ranks numbers directly (match counts still from the estimates)
    for s, _, _, _ in SERVERS:
        MERGED[(s, "all")] = (MERGED[(s, "all")][0], {m: {h: cell(raw[s]["all"][m], h) for h in hero_ids} for m in ALL})

    # ---- prior weights estimated from the data, per bracket, pooled over all servers.
    #      tau^2 = true map-to-map spread = observed spread around the hero's all-maps value - sampling noise.
    #      Win rate: prior worth 2500 / tau^2 games. Pick / ban rate (relative spread): a hero with rate p gets
    #      (100 - p) / (k * p * tau^2) matches, k = 2 for picks (two teams per match), 1 for bans.
    KW = {}
    for gid, _, _, _ in GROUPS:
        ew = [0.0, 0.0]; ep = [0.0, 0.0]; eb = [0.0, 0.0]
        for s, _, _, _ in SERVERS:
            NP, KR = MERGED[(s, gid)]
            for h in hero_ids:
                a = KR["all-maps"][h]
                for m in map_ids:
                    c, n = KR[m][h], NP[m]
                    if c["miss"] or a["miss"] or n <= 0:
                        continue
                    g = 2 * n * c["pr"] / 100
                    if c["wr"] is not None and a["wr"] is not None and g >= 30:
                        ew[0] += g * ((c["wr"] - a["wr"]) ** 2 - 2500 / g); ew[1] += g
                    if a["pr"] >= 0.5:
                        ep[0] += n * ((c["pr"] / a["pr"] - 1) ** 2 - (100 - a["pr"]) / (2 * n * a["pr"])); ep[1] += n
                    if a["br"] >= 0.5:
                        eb[0] += n * ((c["br"] / a["br"] - 1) ** 2 - (100 - a["br"]) / (n * a["br"])); eb[1] += n
        t_wr = max(ew[0] / ew[1], 0.05) if ew[1] else 4.0
        t_pr = max(ep[0] / ep[1], 0.001) if ep[1] else 0.08
        t_br = max(eb[0] / eb[1], 0.001) if eb[1] else 0.15
        KW[gid] = {"wr": clampk(2500 / t_wr), "t_wr": t_wr ** 0.5, "t_pr": t_pr ** 0.5, "t_br": t_br ** 0.5}
        log(f"prior weights {gid}: win rate {KW[gid]['wr']:.0f} games (map spread {t_wr ** 0.5:.2f}%p), "
            f"pick spread {t_pr ** 0.5:.2f}, ban spread {t_br ** 0.5:.2f}")

    def k_pr(gid, p):
        p = max(p, 0.3)
        return clampk((100 - p) / (2 * p * KW[gid]["t_pr"] ** 2))

    def k_br(gid, b):
        b = max(b, 0.3)
        return clampk((100 - b) / (b * KW[gid]["t_br"] ** 2))

    # ---- map-effect priors per bracket: pooled Asia + Americas + Europe of the same bracket
    PRIOR, EFF, CONST = {}, {}, {}
    for gid, _, _, _ in GROUPS:
        NG = {m: sum(MERGED[(s, gid)][0][m] for s in PRIOR_SERVERS) for m in ALL}

        def pooled(m, h):
            cs = {s: MERGED[(s, gid)][1][m][h] for s in PRIOR_SERVERS}
            ws = {s: MERGED[(s, gid)][0][m] for s in PRIOR_SERVERS}
            pr = sum(ws[s] * cs[s]["pr"] for s in PRIOR_SERVERS) / NG[m]
            br = sum(ws[s] * cs[s]["br"] for s in PRIOR_SERVERS) / NG[m]
            gs = [(ws[s] * cs[s]["pr"], cs[s]["wr"]) for s in PRIOR_SERVERS if cs[s]["wr"] is not None]
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
                kp, kb = k_pr(gid, P["pr"]), k_br(gid, P["br"])
                pra = (NG[m] * pr + kp * P["pr"]) / (NG[m] + kp)
                bra = (NG[m] * br + kb * P["br"]) / (NG[m] + kb)
                wra = shr(wr, 2 * NG[m] * pr / 100, P["wr"], KW[gid]["wr"])
                GA[m][h] = {"pr": pra, "br": bra, "wr": wra}
                eff[m][h] = {"pr": pra / max(P["pr"], 0.05), "wr": wra - P["wr"], "br": (bra + 1) / (P["br"] + 1)}
        PRIOR[gid], EFF[gid] = GA, eff
        CONST[gid] = {}
        for role in ROLE_ORDER:
            xs = []
            for m in map_ids:
                for h in hero_ids:
                    if role_of[h] != role:
                        continue
                    c = GA[m][h]
                    prav = max(0.1, c["pr"]) / max(0.3, 1 - c["br"] / 100)
                    xs.append((mirror(c["wr"], c["pr"]) - 50, math.log(prav), math.log(1 + max(0.0, c["br"]))))
            cols = list(zip(*xs))
            CONST[gid][role] = {"wr_sd": round(st.pstdev(cols[0]), 3), "pr_mu": round(st.mean(cols[1]), 4),
                                "pr_sd": round(st.pstdev(cols[1]), 3), "br_mu": round(st.mean(cols[2]), 3),
                                "br_sd": round(st.pstdev(cols[2]), 3)}

    def smooth(s, gid):
        NP, KR = MERGED[(s, gid)]
        GA, eff = PRIOR[gid], EFF[gid]
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
                kp, kb = k_pr(gid, prp), k_br(gid, brp)
                SM[m][h] = {"pr": (NP[m] * c["pr"] + kp * prp) / (NP[m] + kp),
                            "br": (NP[m] * c["br"] + kb * brp) / (NP[m] + kb),
                            "wr": shr(c["wr"], g, wrp, KW[gid]["wr"]), "g": g, "raw": c}
        # adjusted values drop mirror matches from the win rate (after shrinking, on the published scale)
        for m in SM:
            for h in hero_ids:
                SM[m][h]["wr"] = mirror(SM[m][h]["wr"], SM[m][h]["pr"])
        return SM

    def meta_index(SM, gid):
        """Default-weight (50/30/20) meta index, the same formula the page uses."""
        out = {m: {} for m in ALL}
        for role in ROLE_ORDER:
            hs = [h for h in hero_ids if role_of[h] == role]
            C = CONST[gid][role]
            comp = {(m, h): sum(w * x for w, x in zip((0.5, 0.3, 0.2), comps(SM[m][h], C))) for m in ALL for h in hs}
            pool = [comp[(m, h)] for m in map_ids for h in hs]
            mu, sd = st.mean(pool), st.pstdev(pool) or 1
            for (m, h), v in comp.items():
                out[m][h] = round(50 + 10 * (v - mu) / sd)
        return out

    # ---- export
    now = datetime.now(KST)
    now_s = now.strftime("%Y-%m-%dT%H:%M+09:00")
    mode_order = [x for x in MODE_PREF if any(mp["mode"] == x for mp in maps)]
    mode_order += [x for x in dict.fromkeys(mp["mode"] for mp in maps) if x not in mode_order]

    def short(mp):
        return SHORT_NAMES.get(mp["id"]) or mp["name"].split(":")[-1].strip()

    heroes_out = []
    for h in hero_ids:
        img = (meta_bz.get(h) or {}).get("img") or meta_kr[h].get("img") or ""
        heroes_out.append({"id": h, "name": meta_kr[h]["name"], "role": role_of[h],
                           "sub": sub_names.get(meta_kr[h].get("sub"), meta_kr[h].get("sub") or ""), "img": img})
    roles_out = {role: {"label": ROLE_LABEL[role],
                        "avg": round(ROLE_PICK_SUM[role] / sum(1 for h in hero_ids if role_of[h] == role), 3)}
                 for role in sorted(ROLE_ORDER, key=ROLE_ORDER.get)}
    sources = {"nexon": "https://overwatch.nexon.com/hero/rate", "blizzard": "https://overwatch.blizzard.com/ko-kr/rates/"}

    index_servers = []
    summaries = []
    for s, label, kind, _ in SERVERS:
        hist_path = os.path.join(args.history, f"{s}.json")
        hist = load_json(hist_path, {"snapshots": [], "patchStart": None}) or {"snapshots": [], "patchStart": None}
        snaps = hist.get("snapshots") or []
        brackets, mi_now, totals = [], {}, {}
        for gid, glabel, granks, _ in GROUPS:
            NP = MERGED[(s, gid)][0]
            SM = smooth(s, gid)
            mi = meta_index(SM, gid)
            mi_now[gid] = {m: [mi[m][h] for h in hero_ids] for m in ALL}
            totals[gid] = int(NP["all-maps"])
            brackets.append({
                "id": gid, "label": glabel, "ranks": granks, "c": CONST[gid],
                "k": {"wr": round(KW[gid]["wr"]), "t_wr": round(KW[gid]["t_wr"], 2),
                      "pr5": round(k_pr(gid, 5)), "pr20": round(k_pr(gid, 20)), "br5": round(k_br(gid, 5))},
                "N": {m: int(round(NP[m])) for m in ALL},
                "cells": {m: [[r1(SM[m][h]["pr"]), r1(SM[m][h]["wr"]), r1(SM[m][h]["br"]),
                               r1(SM[m][h]["raw"]["pr"]), r1(SM[m][h]["raw"]["wr"]), r1(SM[m][h]["raw"]["br"]),
                               round(SM[m][h]["g"]), 1 if SM[m][h]["raw"]["miss"] else 0] for h in hero_ids] for m in ALL},
            })
            parts = []
            for role in sorted(ROLE_ORDER, key=ROLE_ORDER.get):
                top = [meta_kr[h]["name"] for h in sorted((h for h in hero_ids if role_of[h] == role),
                                                         key=lambda h: -mi["all-maps"][h]) if tier(mi["all-maps"][h]) == "S"]
                parts.append(f"{ROLE_LABEL[role]} S: {', '.join(top) or '없음'}")
            summaries.append(f"SUMMARY {label} {glabel}: 추정 {totals[gid]:,}경기 | " + " | ".join(parts))

        changed = not snaps or snaps[-1].get("hash") != hashes[s]
        prev = snaps[-1] if snaps else None
        if changed and prev:
            prev_total = sum(prev.get("totals", {}).values())
            if prev_total and sum(totals.values()) < PATCH_DROP * prev_total:
                hist["patchStart"] = now_s          # stats were reset: a new patch began
                hist["lastPatchTotals"] = prev.get("totals")
        if changed:
            snaps.append({"at": now_s, "hash": hashes[s], "totals": totals, "heroes": hero_ids, "mi": mi_now})
            hist["snapshots"] = snaps[-HISTORY_KEEP:]
            write_json(hist_path, hist)
        # comparison base: the previous distinct snapshot (the last one of the previous patch after a reset)
        base = hist["snapshots"][-2] if len(hist["snapshots"]) >= 2 else None
        compare = None
        if base:
            compare = {"at": base["at"], "heroes": base.get("heroes", []),
                       "patch": bool(hist.get("patchStart") and base["at"] < hist["patchStart"]),
                       "mi": {g: {"all-maps": v.get("all-maps"), **{m: v.get(m) for m in map_ids if m in v}}
                              for g, v in base.get("mi", {}).items()}}
        last_patch = hist.get("lastPatchTotals") or {}
        exported = {
            "meta": {"server": s, "label": label, "source": sources[kind], "collectedAt": now_s, "hash": hashes[s],
                     "shares": {r: round(shares[s][r] * 100, 2) for r in RANKS},
                     "patchStart": hist.get("patchStart"),
                     "sampleRatio": {g: round(totals[g] / last_patch[g], 3) for g in totals if last_patch.get(g)}},
            "roles": roles_out, "modes": mode_order, "heroes": heroes_out,
            "maps": [{"id": mp["id"], "name": mp["name"], "short": short(mp), "mode": mp["mode"]} for mp in maps],
            "brackets": brackets, "compare": compare,
        }
        write_json(os.path.join(args.out, f"{s}.json"), exported)
        update_archive(os.path.join(args.archive, f"{s}.json"), exported)
        index_servers.append({"id": s, "label": label, "source": kind, "hash": hashes[s], "collectedAt": now_s})

    write_json(os.path.join(args.out, "index.json"), {"collectedAt": now_s, "servers": index_servers})
    for line in summaries:
        log(line)
    log(f"Maps {len(map_ids)}, heroes {len(hero_ids)}, {time.time() - t0:.0f}s total.")
    print("RESULT: UPDATED", flush=True)


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:  # noqa: BLE001
        print(f"RESULT: ERROR unexpected {type(e).__name__}: {e}", flush=True)
        sys.exit(2)
