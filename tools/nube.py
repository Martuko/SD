#!/usr/bin/env python3
# tools/t3_wordclouds.py
#
# Genera nubes de palabras para Yahoo y LLM a partir de:
#   experiments_results/Tarea3/wordfreq_yahoo.csv
#   experiments_results/Tarea3/wordfreq_llm.csv
#
# Formato de esos CSV (salida de Pig): source,word,cnt

import csv
from pathlib import Path

import matplotlib.pyplot as plt
from wordcloud import WordCloud


BASE = Path("experiments_results") / "Tarea3"


def load_freq_dict(path: Path, expected_source: str, max_words: int | None = None):
    """
    Carga un CSV con formato source,word,cnt y devuelve un dict {word: freq}
    solo para el 'expected_source'.

    max_words: si se especifica, nos quedamos con las palabras más frecuentes
    hasta llegar a ese número (para que la nube no quede saturada).
    """
    rows = []
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
            rows.append((word, cnt))

    # Ordenar por frecuencia descendente
    rows.sort(key=lambda x: x[1], reverse=True)

    if max_words is not None:
        rows = rows[:max_words]

    return {w: c for (w, c) in rows}


def make_wordcloud(freqs: dict[str, int], title: str, out_path: Path):
    """
    Genera una nube de palabras y la guarda como PNG.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)

    wc = WordCloud(
        width=1200,
        height=600,
        background_color="white",
    ).generate_from_frequencies(freqs)

    plt.figure(figsize=(12, 6))
    plt.imshow(wc, interpolation="bilinear")
    plt.axis("off")
    plt.title(title)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


def main():
    # Rutas a los CSV de frecuencias completos
    yahoo_path = BASE / "wordfreq_yahoo.csv"
    llm_path   = BASE / "wordfreq_llm.csv"

    if not yahoo_path.exists() or not llm_path.exists():
        raise SystemExit(
            f"ERROR: No se encontraron {yahoo_path} o {llm_path}. "
            "Asegúrate de haber exportado desde Pig."
        )

    # Cargamos, por ejemplo, las 300 palabras más frecuentes de cada uno
    yahoo_freq = load_freq_dict(yahoo_path, expected_source="yahoo", max_words=300)
    llm_freq   = load_freq_dict(llm_path,   expected_source="llm",   max_words=300)

    # Generar nubes
    out_yahoo = BASE / "wc_yahoo.png"
    out_llm   = BASE / "wc_llm.png"

    make_wordcloud(yahoo_freq, "Word Cloud Yahoo! Answers (T3)", out_yahoo)
    make_wordcloud(llm_freq,   "Word Cloud LLM (T3)",            out_llm)

    print(f"[DONE] Nube Yahoo guardada en {out_yahoo}")
    print(f"[DONE] Nube LLM   guardada en {out_llm}")


if __name__ == "__main__":
    main()
