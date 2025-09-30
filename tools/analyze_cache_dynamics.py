#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
analyze_cache_dynamics.py

Genera visualizaciones avanzadas de caché a partir de interactions.csv por experimento:
  1) CDF de distancia de reutilización (reuse distance / "distintas entre repeticiones")
     - por experimento
     - agregado por política (LRU/LFU)
  2) Hit-rate en el tiempo:
     - acumulado
     - rolling (ventana deslizante configurable)
  3) Scatter: cache_hit_rate vs p50_ms (color = política, tamaño = max_entries)
  4) Barras: hit-rate promedio por (política, tamaño)

Requisitos: pandas, numpy, matplotlib
Entradas: experiments_results/E*/interactions.csv (+ metadata.csv/summary.csv si está)
Salidas: report_out/img/*.png y *.pdf, y report_out/cache_dynamics_summary.csv
"""

import argparse, re, sys, os, glob
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# ---------------------- CLI ----------------------
def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True, help="Directorio con experiments_results/E*/interactions.csv")
    ap.add_argument("--out",  required=True, help="Directorio de salida (report_out)")
    ap.add_argument("--include", default=r"^E[0-9]+$", help="Regex de experimentos a incluir (por nombre de carpeta)")
    ap.add_argument("--exclude", default=r"", help="Regex de experimentos a excluir")
    ap.add_argument("--rolling", type=int, default=300, help="Tamaño de ventana para hit-rate rolling")
    ap.add_argument("--seed", type=int, default=42, help="Semilla")
    return ap.parse_args()

# ---------------------- Utilidades ----------------------
def ensure_dirs(out_dir):
    img_dir = os.path.join(out_dir, "img")
    os.makedirs(img_dir, exist_ok=True)
    return img_dir

def savefig_both(path_png):
    plt.tight_layout()
    plt.savefig(path_png, dpi=150)
    plt.savefig(path_png.replace(".png", ".pdf"))
    plt.close()

def load_metadata_label(exp_dir):
    """
    Intenta leer distribution/policy/max_entries/label de metadata.csv/summary.csv (si existen).
    Devuelve dict con valores o '?' / NaN cuando falte info.
    """
    meta = {"distribution":"?", "policy":"?", "max_entries":np.nan, "label": os.path.basename(exp_dir)}
    for cand in ["metadata.csv", "summary.csv", "metadata_before.csv"]:
        p = os.path.join(exp_dir, cand)
        if os.path.exists(p):
            try:
                df = pd.read_csv(p)
                for col in ["distribution","policy","max_entries","name","label"]:
                    if col in df.columns and len(df):
                        v = df[col].iloc[0]
                        if col == "name":
                            meta["label"] = str(v)
                        else:
                            meta[col] = v
            except Exception:
                pass
    return meta

def compute_latency_percentiles(df):
    out = {"avg_latency_ms":np.nan,"p50_ms":np.nan,"p90_ms":np.nan,"p99_ms":np.nan}
    if "latency_ms" not in df.columns: 
        return out
    lat = pd.to_numeric(df["latency_ms"], errors="coerce").dropna()
    if len(lat)==0: 
        return out
    out["avg_latency_ms"] = float(lat.mean())
    out["p50_ms"] = float(np.percentile(lat, 50))
    out["p90_ms"] = float(np.percentile(lat, 90))
    out["p99_ms"] = float(np.percentile(lat, 99))
    return out

def compute_hit_rate(df):
    if "cached" not in df.columns: 
        return 0, np.nan
    x = df["cached"].astype(str).str.lower().isin(["1","true","t","yes"])
    hits = int(x.sum())
    tot  = len(df)
    return hits, (100.0*hits/tot) if tot>0 else np.nan

def rolling_hit_rate(series_cached, window):
    x = series_cached.astype(str).str.lower().isin(["1","true","t","yes"]).astype(int)
    num = x.rolling(window, min_periods=1).sum()
    den = pd.Series(1, index=x.index).rolling(window, min_periods=1).sum()
    return (100.0 * num / den)

def reuse_distance_LRU_stack(question_ids):
    """
    Reuse distance (stack distance estilo LRU):
      - Si la clave aparece por primera vez -> NaN
      - Si ya estaba en el "stack" -> distancia = índice en el stack (0=head)
    Luego movemos la clave a la cabeza (como LRU).
    Nota: O(n^2) por list.index; con ~10k filas es suficiente para este trabajo.
    """
    stack = []
    out = np.full(len(question_ids), np.nan, dtype=float)
    seen = set()
    for i, q in enumerate(question_ids):
        if q in seen:
            idx = stack.index(q)   # nº de elementos por delante (distintas desde la última vez)
            out[i] = idx
            stack.pop(idx)
            stack.insert(0, q)
        else:
            seen.add(q)
            stack.insert(0, q)
    return out

# ---------------------- Main ----------------------
def main():
    args = parse_args()
    np.random.seed(args.seed)
    img_dir = ensure_dirs(args.out)

    inc_re = re.compile(args.include)
    exc_re = re.compile(args.exclude) if args.exclude else None

    # 1) Cargar runs
    runs = []
    for exp_dir in sorted(glob.glob(os.path.join(args.root, "E*"))):
        exp = os.path.basename(exp_dir)
        if not inc_re.search(exp): 
            continue
        if exc_re and exc_re.search(exp):
            continue
        f = os.path.join(exp_dir, "interactions.csv")
        if not os.path.exists(f):
            continue
        try:
            df = pd.read_csv(f)
        except Exception as e:
            print(f"[WARN] No pude leer {f}: {e}", file=sys.stderr)
            continue

        # ordenar por tiempo si existe
        if "created_at" in df.columns:
            df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
            df = df.sort_values("created_at").reset_index(drop=True)
        else:
            df = df.reset_index(drop=True)

        # normalizar 'cached' a bool
        if "cached" in df.columns:
            df["cached"] = df["cached"].astype(str).str.lower().isin(["1","true","t","yes"])

        meta = load_metadata_label(exp_dir)
        runs.append({"exp":exp, "dir":exp_dir, "df":df, **meta})

    if not runs:
        print("No se encontraron runs válidos.", file=sys.stderr)
        return

    # 2) REUSE DISTANCE: CDF por experimento
    legends = []
    plt.figure(figsize=(10,6))
    for r in runs:
        df = r["df"]
        if "question_id" not in df.columns:
            continue
        dist = reuse_distance_LRU_stack(df["question_id"].tolist())
        d = pd.Series(dist).dropna()
        if len(d)==0:
            continue
        xs = np.sort(d.values)
        ys = np.arange(1, len(xs)+1) / len(xs)
        plt.plot(xs, ys, lw=1.8)
        legends.append(f"{r['exp']} ({str(r['policy']).replace('allkeys-','')}, {int(r['max_entries']) if pd.notna(r['max_entries']) else '?'})")
        r["reuse_distance"] = d
    plt.xlabel("Reuse distance (nº de claves distintas entre repeticiones)")
    plt.ylabel("CDF")
    plt.title("Distribución de distancia de reutilización por experimento")
    if legends: plt.legend(legends, ncol=2)
    savefig_both(os.path.join(img_dir, "reuse_distance_cdf_by_experiment.png"))

    # 3) REUSE DISTANCE: CDF agregado por política
    by_pol = {}
    for r in runs:
        if "reuse_distance" in r:
            pol = str(r["policy"] or "?").replace("allkeys-","").upper()
            by_pol.setdefault(pol, []).append(r["reuse_distance"])
    if by_pol:
        plt.figure(figsize=(10,6))
        for pol, arrs in by_pol.items():
            cat = pd.concat(arrs, ignore_index=True)
            xs = np.sort(cat.values)
            ys = np.arange(1, len(xs)+1) / len(xs)
            plt.plot(xs, ys, lw=2, label=f"{pol} (E*)")
        plt.xlabel("Reuse distance (distintas entre repeticiones)")
        plt.ylabel("CDF")
        plt.title("Reuse distance por política (agregado)")
        plt.legend()
        savefig_both(os.path.join(img_dir, "reuse_distance_cdf_by_policy.png"))

    # 4) HIT-RATE ACUMULADO por experimento
    plt.figure(figsize=(11,6))
    for r in runs:
        df = r["df"]
        if "cached" not in df.columns:
            continue
        cum = df["cached"].astype(int).cumsum() / (np.arange(len(df))+1) * 100.0
        label = f"{r['exp']} ({str(r['policy']).replace('allkeys-','').upper()},{int(r['max_entries']) if pd.notna(r['max_entries']) else '?'})"
        plt.plot(cum.values, lw=1.8, label=label)
    plt.xlabel("Índice de solicitud (orden de llegada)")
    plt.ylabel("Hit-rate acumulado (%)")
    plt.title("Hit-rate acumulado en el tiempo")
    plt.legend(ncol=2)
    savefig_both(os.path.join(img_dir, "hitrate_cumulative_by_experiment.png"))

    # 5) HIT-RATE ROLLING por experimento
    plt.figure(figsize=(11,6))
    for r in runs:
        df = r["df"]
        if "cached" not in df.columns:
            continue
        roll = rolling_hit_rate(df["cached"], args.rolling)
        label = f"{r['exp']} {str(r['policy']).replace('allkeys-','').upper()} {int(r['max_entries']) if pd.notna(r['max_entries']) else '?'}"
        plt.plot(roll.values, lw=1.4, label=label)
    plt.xlabel(f"Índice de solicitud (rolling={args.rolling})")
    plt.ylabel("Hit-rate en ventana (%)")
    plt.title("Hit-rate en el tiempo (ventana deslizante)")
    plt.legend(ncol=2)
    savefig_both(os.path.join(img_dir, "hitrate_rolling_by_experiment.png"))

    # 6) SCATTER: cache_hit_rate vs p50_ms (color=política, tamaño=max_entries)
    rows = []
    for r in runs:
        df = r["df"]
        hits, hit_rate = compute_hit_rate(df)
        lat = compute_latency_percentiles(df)
        rows.append({
            "experiment": r["exp"],
            "policy": str(r["policy"] or "?").replace("allkeys-","").upper(),
            "max_entries": r["max_entries"],
            "cache_hit_rate": hit_rate,
            "p50_ms": lat["p50_ms"]
        })
    agg = pd.DataFrame(rows).dropna(subset=["cache_hit_rate","p50_ms"])

    if len(agg):
        plt.figure(figsize=(9,6))
        max_me = agg["max_entries"].fillna(0).replace(0, agg["max_entries"].max() or 1)
        sizes = (agg["max_entries"].fillna(max_me.max()).astype(float) / max_me.max()) * 600.0
        for pol in sorted(agg["policy"].unique()):
            sub = agg[agg["policy"]==pol]
            plt.scatter(sub["cache_hit_rate"], sub["p50_ms"], s=sizes[agg["policy"]==pol],
                        alpha=0.8, label=pol)
            for _, row in sub.iterrows():
                plt.annotate(row["experiment"], (row["cache_hit_rate"], row["p50_ms"]),
                             fontsize=8, xytext=(3,3), textcoords="offset points")
        plt.xlabel("Cache hit-rate (%)")
        plt.ylabel("Latencia p50 (ms)")
        plt.title("Trade-off: hit-rate vs latencia (tamaño ∝ max_entries)")
        plt.legend(title="Política")
        savefig_both(os.path.join(img_dir, "scatter_hit_vs_p50.png"))

    # 7) Barras: hit-rate promedio por (política, tamaño)
    if len(agg):
        combo = agg.copy()
        combo["max_entries"] = combo["max_entries"].fillna(0).astype(int)
        grp = combo.groupby(["policy","max_entries"], as_index=False)["cache_hit_rate"].mean()
        plt.figure(figsize=(8,5))
        x = np.arange(len(grp))
        plt.bar(x, grp["cache_hit_rate"].values)
        xt = [f"{row.policy} ({row.max_entries})" for row in grp.itertuples()]
        plt.xticks(x, xt, rotation=20)
        plt.ylabel("Hit-rate promedio (%)")
        plt.title("Hit-rate por política y tamaño de caché")
        savefig_both(os.path.join(img_dir, "bar_hitrate_by_policy_size.png"))

    # Guardar resumen CSV
    out_csv = os.path.join(args.out, "cache_dynamics_summary.csv")
    agg.to_csv(out_csv, index=False)
    print(f"[OK] Figuras en: {img_dir}")
    print(f"[OK] Resumen: {out_csv}")

if __name__ == "__main__":
    main()
