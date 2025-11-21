#!/usr/bin/env python3
# tools/t3_top_words_plots.py
#
# Lee los wordcount de Pig (yahoo/llm),
# genera Top 50 y gráficos de barras.

import csv
from pathlib import Path

import matplotlib.pyplot as plt


def load_wordfreq(path: Path, expected_source: str):
    """
    Carga un CSV generado por Pig (source,word,cnt) y devuelve una lista
    de tuplas (word, count) solo para el source especificado.
    """
    words = []
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) != 3:
                continue
            source, word, cnt_str = row
            if source != expected_source:
                continue
            word = word.strip()
            if not word:
                continue
            try:
                cnt = int(cnt_str)
            except ValueError:
                continue
            words.append((word, cnt))
    return words


def top_n(words, n=50):
    """
    Retorna las top N palabras por frecuencia (Pig ya las entrega ordenadas,
    pero reordenamos por si acaso).
    """
    words_sorted = sorted(words, key=lambda x: x[1], reverse=True)
    return words_sorted[:n]


def save_top_csv(path: Path, words):
    """
    Guarda un CSV simple: word,count
    """
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["word", "count"])
        writer.writerows(words)


def plot_top_bar(words, title: str, out_path: Path, top_k: int = 20):
    """
    Genera un gráfico de barras con las top_k palabras.
    """
    subset = words[:top_k]
    labels = [w for (w, _) in subset]
    counts = [c for (_, c) in subset]

    plt.figure(figsize=(12, 6))
    plt.bar(range(len(subset)), counts)
    plt.xticks(range(len(subset)), labels, rotation=45, ha="right")
    plt.tight_layout()
    plt.title(title)
    plt.ylabel("Frequency")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, dpi=150)
    plt.close()


def main():
    base = Path("experiments_results") / "Tarea3"
    yahoo_path = base / "wordfreq_yahoo.csv"
    llm_path = base / "wordfreq_llm.csv"

    if not yahoo_path.exists() or not llm_path.exists():
        raise SystemExit(
            f"ERROR: No se encontraron wordfreq_yahoo.csv / wordfreq_llm.csv en {base}"
        )

    # 1) Cargar
    print(f"[INFO] Cargando {yahoo_path}")
    yahoo_words = load_wordfreq(yahoo_path, expected_source="yahoo")

    print(f"[INFO] Cargando {llm_path}")
    llm_words = load_wordfreq(llm_path, expected_source="llm")

    print(f"[INFO] Yahoo total palabras distintas: {len(yahoo_words)}")
    print(f"[INFO] LLM   total palabras distintas: {len(llm_words)}")

    # 2) Top 50
    top50_yahoo = top_n(yahoo_words, 50)
    top50_llm = top_n(llm_words, 50)

    # 3) Guardar CSV
    out_yahoo_csv = base / "top50_yahoo.csv"
    out_llm_csv = base / "top50_llm.csv"
    save_top_csv(out_yahoo_csv, top50_yahoo)
    save_top_csv(out_llm_csv, top50_llm)

    print(f"[DONE] Guardado Top 50 Yahoo en {out_yahoo_csv}")
    print(f"[DONE] Guardado Top 50 LLM   en {out_llm_csv}")

    # 4) Gráficos de barras (Top 20 para que sean legibles)
    out_yahoo_png = base / "top50_yahoo.png"
    out_llm_png = base / "top50_llm.png"

    plot_top_bar(
        top50_yahoo,
        title="Top 20 palabras Yahoo! (T3)",
        out_path=out_yahoo_png,
        top_k=20,
    )

    plot_top_bar(
        top50_llm,
        title="Top 20 palabras LLM (T3)",
        out_path=out_llm_png,
        top_k=20,
    )

    print(f"[DONE] Gráfico Yahoo guardado en {out_yahoo_png}")
    print(f"[DONE] Gráfico LLM   guardado en {out_llm_png}")

    # 5) Mostrar un resumen rápido por consola
    print("\n=== Top 10 Yahoo ===")
    for w, c in top50_yahoo[:10]:
        print(f"{w:20} {c}")

    print("\n=== Top 10 LLM ===")
    for w, c in top50_llm[:10]:
        print(f"{w:20} {c}")


if __name__ == "__main__":
    main()
