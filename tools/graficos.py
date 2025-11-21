#!/usr/bin/env python3
# tools/t3_extra_plots.py
#
# Genera visualizaciones adicionales a partir de:
#   - experiments_results/Tarea3/wordfreq_yahoo.csv
#   - experiments_results/Tarea3/wordfreq_llm.csv
#   - experiments_results/Tarea3/top50_yahoo.csv
#   - experiments_results/Tarea3/top50_llm.csv
#
# Graficos:
#   1) Barras Top 20 Yahoo (ya lo tienes, pero lo dejamos por claridad)
#   2) Barras Top 20 LLM
#   3) Barras agrupadas Yahoo vs LLM para palabras comunes
#   4) Ratio LLM/Yahoo (Top 20 donde el modelo se sobre-usa)
#   5) Curvas de frecuencia vs ranking (Zipf-like)

import csv
from pathlib import Path

import matplotlib.pyplot as plt


BASE = Path("experiments_results") / "Tarea3"


# --------- Utilidades de carga ---------

def load_top(path: Path):
    """
    Carga CSV topN: word,count
    """
    words = []
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            w = row["word"].strip()
            if not w:
                continue
            c = int(row["count"])
            words.append((w, c))
    return words


def load_wordfreq(path: Path, expected_source: str):
    """
    Carga CSV wordfreq: source,word,cnt (salida de Pig)
    """
    words = []
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) != 3:
                continue
            source, word, cnt = row
            if source != expected_source:
                continue
            word = word.strip()
            if not word:
                continue
            words.append((word, int(cnt)))
    # ya vienen ordenadas por Pig, pero por si acaso:
    words.sort(key=lambda x: x[1], reverse=True)
    return words


def ensure_dir(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)


# --------- 1 & 2: Barras Top 20 Yahoo / LLM ---------

def plot_top_bar(words, title: str, out_path: Path, top_k: int = 20):
    ensure_dir(out_path)
    subset = words[:top_k]
    labels = [w for (w, _) in subset]
    counts = [c for (_, c) in subset]

    plt.figure(figsize=(12, 6))
    plt.bar(range(len(subset)), counts)
    plt.xticks(range(len(subset)), labels, rotation=45, ha="right")
    plt.ylabel("Frequency")
    plt.title(title)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


# --------- 3: Barras agrupadas Yahoo vs LLM ---------

def plot_grouped_bars(yahoo_top, llm_top, out_path: Path, top_k: int = 20):
    """
    Toma los top50 de Yahoo y LLM y construye un gráfico
    de barras agrupadas para palabras comunes.
    Las palabras se ordenan por (freq_yahoo + freq_llm) desc.
    """
    ensure_dir(out_path)

    y_dict = {w: c for (w, c) in yahoo_top}
    l_dict = {w: c for (w, c) in llm_top}

    common = sorted(
        set(y_dict.keys()) & set(l_dict.keys()),
        key=lambda w: y_dict[w] + l_dict[w],
        reverse=True,
    )

    common = common[:top_k]

    y_vals = [y_dict[w] for w in common]
    l_vals = [l_dict[w] for w in common]

    x = range(len(common))
    width = 0.4

    plt.figure(figsize=(12, 6))
    # dos barras: Yahoo desplazado a la izquierda, LLM a la derecha
    x_yahoo = [i - width/2 for i in x]
    x_llm = [i + width/2 for i in x]

    plt.bar(x_yahoo, y_vals, width=width, label="Yahoo")
    plt.bar(x_llm, l_vals, width=width, label="LLM")

    plt.xticks(x, common, rotation=45, ha="right")
    plt.ylabel("Frequency")
    plt.title("Comparación Yahoo vs LLM (palabras comunes, Top {})".format(top_k))
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


# --------- 4: Ratio LLM/Yahoo ---------

def plot_ratio(words_yahoo, words_llm, out_path: Path, min_count: int = 50, top_k: int = 20):
    """
    Construye un gráfico de barras con el ratio LLM/Yahoo para
    palabras comunes. Filtra palabras con muy baja frecuencia
    para evitar ratios locos.
    """
    ensure_dir(out_path)
    y_dict = {w: c for (w, c) in words_yahoo}
    l_dict = {w: c for (w, c) in words_llm}

    ratios = []
    for w in set(y_dict.keys()) & set(l_dict.keys()):
        y = y_dict[w]
        l = l_dict[w]
        if y < min_count and l < min_count:
            continue
        ratio = l / y if y > 0 else 0.0
        ratios.append((w, ratio, y, l))

    # ordenar por ratio desc (donde el LLM usa mucho más la palabra)
    ratios.sort(key=lambda x: x[1], reverse=True)
    top = ratios[:top_k]

    labels = [w for (w, _, _, _) in top]
    r_vals = [r for (_, r, _, _) in top]

    plt.figure(figsize=(12, 6))
    plt.bar(range(len(top)), r_vals)
    plt.xticks(range(len(top)), labels, rotation=45, ha="right")
    plt.ylabel("Ratio LLM / Yahoo")
    plt.title("Palabras donde el LLM se sobre-usa (ratio LLM/Yahoo)")
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


# --------- 5: Curva frecuencia vs ranking (Zipf-like) ---------

def plot_zipf_curve(words_yahoo, words_llm, out_path: Path, max_rank: int = 200):
    """
    Grafica frecuencia vs ranking (log-log) para Yahoo y LLM.
    Solo usa los primeros max_rank términos de cada uno.
    """
    ensure_dir(out_path)

    y_subset = words_yahoo[:max_rank]
    l_subset = words_llm[:max_rank]

    y_ranks = list(range(1, len(y_subset) + 1))
    y_freqs = [c for (_, c) in y_subset]

    l_ranks = list(range(1, len(l_subset) + 1))
    l_freqs = [c for (_, c) in l_subset]

    plt.figure(figsize=(8, 6))
    plt.loglog(y_ranks, y_freqs, marker="o", linestyle="-", label="Yahoo")
    plt.loglog(l_ranks, l_freqs, marker="s", linestyle="--", label="LLM")
    plt.xlabel("Rank (log)")
    plt.ylabel("Frequency (log)")
    plt.title("Curva de frecuencia vs ranking (Zipf)")
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


# --------- main ---------

def main():
    # Cargar Top 50 (ya generados por tu otro script)
    top_y_path = BASE / "top50_yahoo.csv"
    top_l_path = BASE / "top50_llm.csv"

    if not top_y_path.exists() or not top_l_path.exists():
        raise SystemExit(
            f"Faltan {top_y_path} o {top_l_path}. "
            "Primero ejecuta el script que genera los top50."
        )

    top_y = load_top(top_y_path)
    top_l = load_top(top_l_path)

    # Cargar wordfreq completos también (para Zipf y ratios)
    wordfreq_y_path = BASE / "wordfreq_yahoo.csv"
    wordfreq_l_path = BASE / "wordfreq_llm.csv"

    words_yahoo = load_wordfreq(wordfreq_y_path, expected_source="yahoo")
    words_llm   = load_wordfreq(wordfreq_l_path, expected_source="llm")

    # 1) Barras Top 20 Yahoo
    plot_top_bar(
        top_y,
        title="Top 20 palabras Yahoo! (T3)",
        out_path=BASE / "top20_yahoo.png",
        top_k=20,
    )

    # 2) Barras Top 20 LLM
    plot_top_bar(
        top_l,
        title="Top 20 palabras LLM (T3)",
        out_path=BASE / "top20_llm.png",
        top_k=20,
    )

    # 3) Barras agrupadas Yahoo vs LLM (palabras comunes)
    plot_grouped_bars(
        top_y,
        top_l,
        out_path=BASE / "top20_grouped_yahoo_llm.png",
        top_k=20,
    )

    # 4) Ratio LLM/Yahoo
    plot_ratio(
        words_yahoo,
        words_llm,
        out_path=BASE / "ratio_llm_yahoo.png",
        min_count=50,
        top_k=20,
    )

    # 5) Curva Zipf-like
    plot_zipf_curve(
        words_yahoo,
        words_llm,
        out_path=BASE / "zipf_yahoo_llm.png",
        max_rank=200,
    )

    print("[DONE] Gráficos extra generados en", BASE)


if __name__ == "__main__":
    main()
