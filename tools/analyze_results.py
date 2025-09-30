#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, re, glob, argparse
from typing import Dict, Any, Optional, List
import pandas as pd, numpy as np
import matplotlib.pyplot as plt

# ---------- utilidades ----------
def read_csv_safe(path: str, **kwargs):
    try:
        if os.path.isfile(path):
            return pd.read_csv(path, **kwargs)
    except Exception as e:
        print(f"[WARN] No pude leer {path}: {e}")
    return None

def ensure_dir(p): os.makedirs(p, exist_ok=True)

def nice_name(d: Dict[str, Any]) -> str:
    base = d.get("name") or d.get("exp") or "Exp"
    dist = d.get("distribution", "?")
    pol = (d.get("policy", "?") or "?").replace("allkeys-", "")
    me = d.get("max_entries", "")
    suf = f" ({dist}, {pol}{', '+str(me) if me else ''})"
    return base + suf

def should_keep(name: str, include: Optional[str], exclude: Optional[str]) -> bool:
    if include and not re.search(include, name):
        return False
    if exclude and re.search(exclude, name):
        return False
    return True

# ---------- carga ----------
def load_experiment(exp_dir: str) -> Dict[str, Any]:
    exp_name = os.path.basename(exp_dir.rstrip("/"))
    meta = {}
    for cand in ("metadata.csv","metadata_before.csv","metadata_mid.csv"):
        md = read_csv_safe(os.path.join(exp_dir, cand))
        if md is not None and not md.empty:
            meta = {k.strip().lower(): md.iloc[0][k] for k in md.columns}
            break
    meta.setdefault("name", exp_name)
    return {
        "name": exp_name,
        "meta": meta,
        "summary": read_csv_safe(os.path.join(exp_dir, "summary.csv")),
        "interactions": read_csv_safe(os.path.join(exp_dir, "interactions.csv")),
    }

# ---------- latencias ----------
def extract_latency_ms_from_interactions(inter: pd.DataFrame) -> Optional[pd.Series]:
    if inter is None or inter.empty:
        return None
    # 1) Si existe columna latency_ms, úsala y normaliza unidades si hace falta:
    for cand in ["latency_ms", "latency", "llm_latency_ms"]:
        if cand in inter.columns:
            s = pd.to_numeric(inter[cand], errors="coerce")
            s = s.replace([np.inf, -np.inf], np.nan).dropna()
            if s.empty: break
            med = np.nanmedian(s)
            # Si parece microsegundos (mediana > 10k ms), divide entre 1000
            if med > 10000:
                s = s / 1000.0
            return s
    # 2) Si existen timestamps, calcula end-to-end: created_at - ts_generated
    def to_dt(col):
        return pd.to_datetime(inter[col], utc=True, errors="coerce") if col in inter.columns else None
    t_start = None
    for start_col in ["ts_generated", "ts_request", "ts_created", "ts_sent"]:
        t_start = to_dt(start_col)
        if t_start is not None and t_start.notna().any():
            break
    t_end = None
    for end_col in ["created_at", "ts_stored", "ts_answered", "ts_done"]:
        t_end = to_dt(end_col)
        if t_end is not None and t_end.notna().any():
            break
    if t_start is not None and t_end is not None:
        dt = (t_end - t_start).dt.total_seconds() * 1000.0
        dt = pd.to_numeric(dt, errors="coerce").replace([np.inf,-np.inf], np.nan).dropna()
        if not dt.empty:
            return dt
    return None

def percentile(s: pd.Series, q: float) -> float:
    if s is None or s.empty: return np.nan
    return float(np.percentile(s.dropna().values, q*100))

# ---------- throughput ----------
def compute_throughput_from_interactions(inter: pd.DataFrame):
    if inter is None or inter.empty or "created_at" not in inter.columns:
        return {"throughput_per_min": np.nan, "duration_min": np.nan}
    df = inter[["created_at"]].copy()
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True, errors="coerce")
    df = df.dropna()
    if df.empty: return {"throughput_per_min": np.nan, "duration_min": np.nan}
    tmin, tmax = df["created_at"].min(), df["created_at"].max()
    elapsed_min = max((tmax - tmin).total_seconds()/60.0, 1e-9)
    return {"throughput_per_min": len(df)/elapsed_min, "duration_min": elapsed_min}

# ---------- agregación ----------
def aggregate_metrics(exps: List[Dict[str, Any]]) -> pd.DataFrame:
    rows=[]
    for ex in exps:
        meta = ex["meta"]
        inter = ex["interactions"]
        # Recalcular latencias desde interactions
        lat = extract_latency_ms_from_interactions(inter) if inter is not None else None
        p50 = percentile(lat, 0.5) if lat is not None else np.nan
        p90 = percentile(lat, 0.9) if lat is not None else np.nan
        p99 = percentile(lat, 0.99) if lat is not None else np.nan
        avg_lat = float(np.nanmean(lat)) if lat is not None and not lat.empty else np.nan

        # Resumen tabular de summary.csv (si existe)
        s = ex["summary"]
        total=answered_any=cache_hits=cache_misses=0
        answer_rate_any=cache_hit_rate=np.nan
        avg_score=np.nan
        if s is not None and not s.empty:
            r = s.iloc[0]
            total=int(r.get("total",0)); answered_any=int(r.get("answered_any",0))
            answer_rate_any=float(r.get("answer_rate_any",np.nan)) if "answer_rate_any" in r else np.nan
            cache_hits=int(r.get("cache_hits",0)) if "cache_hits" in r else 0
            cache_misses=int(r.get("cache_misses",0)) if "cache_misses" in r else max(total-cache_hits,0)
            cache_hit_rate=float(r.get("cache_hit_rate",np.nan)) if "cache_hit_rate" in r else (100.0*cache_hits/max(total,1))
            if "avg_score" in r:
                try: avg_score=float(r.get("avg_score",np.nan))
                except: pass

        thr = compute_throughput_from_interactions(inter)

        rows.append({
            "experiment": meta.get("name") or ex["name"],
            "label": nice_name({**meta, "name": meta.get("name") or ex["name"]}),
            "distribution": meta.get("distribution",""),
            "policy": meta.get("policy",""),
            "max_entries": meta.get("max_entries",""),
            "rate": meta.get("rate",""),
            "concurrency": meta.get("concurrency",""),
            "pool_size": meta.get("pool_size",""),
            "sample_limit": meta.get("sample_limit",""),
            "total": total, "answered_any": answered_any,
            "answer_rate_any": answer_rate_any,
            "cache_hits": cache_hits, "cache_misses": cache_misses, "cache_hit_rate": cache_hit_rate,
            "avg_latency_ms": avg_lat, "avg_score": avg_score,
            "p50_ms": p50, "p90_ms": p90, "p99_ms": p99,
            "throughput_per_min": thr["throughput_per_min"],
            "duration_min": thr["duration_min"],
        })
    df = pd.DataFrame(rows)

    def key_exp(x):
        try: return int(str(x).upper().replace("E",""))
        except: return 999
    return df.sort_values(by="experiment", key=lambda s: s.map(key_exp))

# ---------- gráficos ----------
def bar_chart(df, xcol, ycol, title, out_path, ylabel=None, ylim=None):
    plt.figure(figsize=(10,4.8))
    plt.bar(df[xcol], df[ycol])
    plt.title(title)
    plt.xticks(rotation=15, ha='right')
    if ylabel: plt.ylabel(ylabel)
    if ylim: plt.ylim(*ylim)
    plt.tight_layout(); plt.savefig(out_path, dpi=160); plt.close()

def line_plot(x, y, title, out_path, xlabel=None, ylabel=None):
    plt.figure(figsize=(9,4.5))
    plt.plot(x, y)
    plt.title(title)
    if xlabel: plt.xlabel(xlabel)
    if ylabel: plt.ylabel(ylabel)
    plt.grid(True, alpha=0.3)
    plt.tight_layout(); plt.savefig(out_path, dpi=160); plt.close()

def warmup_plot(inter, step, title, out_path):
    if inter is None or inter.empty or "cached" not in inter.columns: return
    df = inter[["cached"]].copy()
    df["is_hit"] = df["cached"].astype(bool).astype(int)
    df["idx"] = np.arange(1, len(df)+1)
    df["bin"] = ((df["idx"]-1)//max(step,1))+1
    g = df.groupby("bin").agg(count=("is_hit","count"), hits=("is_hit","sum"))
    g["cum_count"] = g["count"].cumsum()
    g["cum_hits"] = g["hits"].cumsum()
    g["cum_hit_rate_%"] = 100.0 * g["cum_hits"] / g["cum_count"]
    line_plot(g["cum_count"], g["cum_hit_rate_%"], title, out_path,
              xlabel="Muestras", ylabel="Hit-rate acumulado (%)")

def latency_cdf_plot(lat_ms: Optional[pd.Series], title, out_path):
    if lat_ms is None or lat_ms.empty: return
    xs = np.sort(lat_ms.dropna().values)
    y = np.arange(1, len(xs)+1) / len(xs)
    plt.figure(figsize=(9,4.5))
    plt.plot(xs, y)
    plt.title(title)
    plt.xlabel("Latencia (ms)")
    plt.ylabel("CDF")
    plt.grid(True, alpha=0.3)
    plt.tight_layout(); plt.savefig(out_path, dpi=160); plt.close()

# ---------- reporte ----------
def write_markdown_report(out_dir: str, agg: pd.DataFrame):
    path = os.path.join(out_dir, "report.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write("# Resumen de Experimentos\n\n")
        f.write("**Experimentos incluidos en este reporte**\n\n")
        f.write(agg[["experiment","distribution","policy","max_entries","total","cache_hit_rate","avg_latency_ms","p50_ms","p90_ms","p99_ms","throughput_per_min"]].to_markdown(index=False))
        f.write("\n\n## Gráficos\n")
        f.write("- Hit-rate por experimento: `img/cache_hit_rate_by_experiment.png`\n")
        f.write("- Answer-rate: `img/answer_rate_by_experiment.png`\n")
        f.write("- Latencia promedio: `img/avg_latency_by_experiment.png`\n")
        f.write("- Percentiles de latencia: `img/latency_percentiles_by_experiment.png`\n")
        f.write("- Throughput/min: `img/throughput_by_experiment.png`\n")
        f.write("\n\n> Nota: Las latencias se recalcularon desde `interactions.csv` y se normalizaron cuando detectamos valores en microsegundos.\n")
    print(f"[OK] Reporte: {path}")

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True, help="Carpeta con subcarpetas E1, E2, ...")
    ap.add_argument("--out", required=True, help="Carpeta de salida")
    ap.add_argument("--warmup", type=int, default=500)
    ap.add_argument("--topn", type=int, default=20)  # (reservado por si luego agregas tops)
    ap.add_argument("--include", type=str, default=None, help="regex de inclusión (p.ej. ^E[1-6]$)")
    ap.add_argument("--exclude", type=str, default=None, help="regex de exclusión")
    args = ap.parse_args()

    ensure_dir(args.out); img = os.path.join(args.out, "img"); ensure_dir(img)

    cand = sorted([d for d in glob.glob(os.path.join(args.root,"*")) if os.path.isdir(d)])
    chosen = []
    for d in cand:
        name = os.path.basename(d)
        if should_keep(name, args.include, args.exclude):
            chosen.append(d)
    if not chosen:
        print(f"[ERROR] No encontré experimentos válidos en {args.root} con los filtros include={args.include} exclude={args.exclude}")
        return

    exps = [load_experiment(d) for d in chosen]
    agg = aggregate_metrics(exps)
    agg_path = os.path.join(args.out, "aggregated_metrics.csv"); agg.to_csv(agg_path, index=False)
    print(f"[OK] Guardado: {agg_path}")

    # Gráficos comparativos
    if not agg.empty:
        if "cache_hit_rate" in agg and agg["cache_hit_rate"].notna().any():
            bar_chart(agg,"experiment","cache_hit_rate","Cache hit-rate por experimento",
                      os.path.join(img,"cache_hit_rate_by_experiment.png"), "Hit-rate (%)", (0,100))
        if "answer_rate_any" in agg and agg["answer_rate_any"].notna().any():
            bar_chart(agg,"experiment","answer_rate_any","Answer rate por experimento",
                      os.path.join(img,"answer_rate_by_experiment.png"), "Answer rate (%)", (0,100))
        if "avg_latency_ms" in agg and agg["avg_latency_ms"].notna().any():
            bar_chart(agg,"experiment","avg_latency_ms","Latencia promedio por experimento",
                      os.path.join(img,"avg_latency_by_experiment.png"), "ms")
        if set(["p50_ms","p90_ms","p99_ms"]).issubset(agg.columns):
            df_lat = agg[["experiment","p50_ms","p90_ms","p99_ms"]].set_index("experiment")
            plt.figure(figsize=(10,4.8)); df_lat.plot(kind="bar", figsize=(10,4.8))
            plt.title("Percentiles de latencia (ms) por experimento"); plt.ylabel("ms")
            plt.xticks(rotation=15, ha="right"); plt.tight_layout()
            plt.savefig(os.path.join(img,"latency_percentiles_by_experiment.png"), dpi=160); plt.close()
        if "throughput_per_min" in agg and agg["throughput_per_min"].notna().any():
            bar_chart(agg,"experiment","throughput_per_min","Throughput por experimento (req/min)",
                      os.path.join(img,"throughput_by_experiment.png"), "req/min")

    # Warmup y CDF por experimento
    for ex in exps:
        name = ex["meta"].get("name") or ex["name"]
        inter = ex["interactions"]
        lat = extract_latency_ms_from_interactions(inter)
        warmup_plot(inter, args.warmup, f"{name} – Warmup hit-rate", os.path.join(img, f"cache_warmup_{name}.png"))
        latency_cdf_plot(lat, f"{name} – CDF latencia (ms)", os.path.join(img, f"latency_cdf_{name}.png"))

    # Reporte Markdown
    write_markdown_report(args.out, agg)

    print("\n" + "="*80)
    print("RESUMEN COMPARATIVO (para pegar al informe)")
    print("="*80)
    cols=["experiment","distribution","policy","max_entries","total","answered_any",
          "answer_rate_any","cache_hits","cache_hit_rate","avg_latency_ms",
          "p50_ms","p90_ms","p99_ms","throughput_per_min"]
    keep=[c for c in cols if c in agg.columns]
    print(agg[keep].to_string(index=False))
    print("\nGráficos guardados en:", img)
    print("CSV agregados en:", args.out)

if __name__=="__main__":
    main()
