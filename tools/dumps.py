#!/usr/bin/env python3
# tools/t3_build_dataset_from_dumps.py

import csv
import sys
import argparse
from pathlib import Path


def build_dataset(output_path: Path, input_paths):
    output_path.parent.mkdir(parents=True, exist_ok=True)

    total_rows = 0
    total_yahoo = 0
    total_llm = 0

    with output_path.open("w", newline="", encoding="utf-8") as fout:
        writer = csv.writer(
            fout,
            delimiter=",",
            quotechar='"',
            quoting=csv.QUOTE_MINIMAL,
            escapechar="\\",
        )
        # Cabecera estándar para Pig
        writer.writerow(["source", "text"])

        for in_path in input_paths:
            in_path = Path(in_path)
            if not in_path.exists():
                print(f"[WARN] No existe input: {in_path}", file=sys.stderr)
                continue

            print(f"[INFO] Leyendo {in_path}", file=sys.stderr)

            with in_path.open("r", newline="", encoding="utf-8") as fin:
                reader = csv.DictReader(fin)
                for row in reader:
                    total_rows += 1

                    # Campos tal como aparecen en tu interactions.csv
                    ref = (row.get("reference_answer") or "").strip()
                    final = (row.get("final_answer") or "").strip()
                    llm_ans = (row.get("llm_answer") or "").strip()

                    # Yahoo!: referencia del dataset
                    if ref:
                        writer.writerow(["yahoo", ref])
                        total_yahoo += 1

                    # LLM: usamos final_answer si existe; si no, caemos a llm_answer
                    llm_text = final if final else llm_ans
                    if llm_text:
                        writer.writerow(["llm", llm_text])
                        total_llm += 1

    print(
        f"[DONE] Filas leídas: {total_rows} | "
        f"yahoo: {total_yahoo} | llm: {total_llm} | salida: {output_path}",
        file=sys.stderr,
    )


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Construye respuestas_t3.csv a partir de uno o más dumps "
            "interactions.csv de la Tarea 1/2."
        )
    )
    parser.add_argument(
        "output",
        help="Ruta de salida del CSV (ej: data/t3/respuestas_t3.csv)",
    )
    parser.add_argument(
        "inputs",
        nargs="+",
        help="Uno o más archivos interactions.csv de tus experimentos",
    )
    args = parser.parse_args()

    out_path = Path(args.output)
    build_dataset(out_path, args.inputs)


if __name__ == "__main__":
    main()
