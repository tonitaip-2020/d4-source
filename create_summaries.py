#!/usr/bin/env python3

import argparse
import math
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd


LANGUAGE_FILE_ORDER = [
    "cpp.csv",
    "c.csv",
    "php.csv",
    "java.csv",
    "go.csv",
    "ruby.csv",
    "python.csv",
    "javascript.csv",
    "c#.csv",
]


def wilson_ci(k: int, n: int, z: float = 1.96) -> Tuple[float, float]:
    """
    Wilson score interval for a binomial proportion.
    Returns (lower, upper) as proportions in [0, 1].
    """
    if n == 0:
        return (0.0, 0.0)

    phat = k / n
    denom = 1.0 + (z * z) / n
    center = (phat + (z * z) / (2.0 * n)) / denom
    margin = (
        z
        * math.sqrt((phat * (1.0 - phat) / n) + ((z * z) / (4.0 * n * n)))
        / denom
    )
    return (max(0.0, center - margin), min(1.0, center + margin))


def format_pct(x: float, decimals: int = 1) -> str:
    return f"{100.0 * x:.{decimals}f}\\%"


def latex_escape(text: str) -> str:
    """
    Minimal LaTeX escaping.
    """
    replacements = {
        "\\": r"\textbackslash{}",
        "&": r"\&",
        "%": r"\%",
        "$": r"\$",
        "#": r"\#",
        "_": r"\_",
        "{": r"\{",
        "}": r"\}",
        "~": r"\textasciitilde{}",
        "^": r"\textasciicircum{}",
    }
    out = text
    for old, new in replacements.items():
        out = out.replace(old, new)
    return out


def language_label_from_filename(filename: str) -> str:
    stem = Path(filename).stem
    mapping = {
        "c++": "C++",
        "cpp": "C++",
        "c": "C",
        "php": "PHP",
        "java": "Java",
        "go": "Go",
        "ruby": "Ruby",
        "python": "Python",
        "javascript": "JavaScript",
        "c#": "C#",
    }
    return mapping.get(stem.lower(), stem)


def load_language_csv(csv_path: Path) -> Tuple[List[str], pd.DataFrame]:
    df = pd.read_csv(csv_path)

    expected_prefix = ["repo_name", "num_files", "languages"]
    missing = [c for c in expected_prefix if c not in df.columns]
    if missing:
        raise ValueError(
            f"{csv_path.name}: missing required columns: {', '.join(missing)}"
        )

    lang_col_idx = list(df.columns).index("languages")
    method_cols = list(df.columns)[lang_col_idx + 1 :]

    if not method_cols:
        raise ValueError(f"{csv_path.name}: no data access method columns found")

    df["num_files"] = pd.to_numeric(df["num_files"], errors="coerce").fillna(0).astype(int)

    # Force numeric 0/1 interpretation for method columns
    for col in method_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
        # Clamp anything nonzero to 1, just in case
        df[col] = (df[col] != 0).astype(int)

    return method_cols, df


def analyze_language(csv_path: Path) -> Dict:
    method_cols, df = load_language_csv(csv_path)

    has_any_method = df[method_cols].sum(axis=1) > 0
    eligible_df = df.loc[has_any_method].copy()
    total_with_any_method = len(eligible_df)

    total_repositories = len(df)
    total_files = int(df["num_files"].sum())
    eligible_files = int(eligible_df["num_files"].sum())

    counts = {}
    for method in method_cols:
        counts[method] = int(eligible_df[method].sum())

    rows = []
    for method, count in counts.items():
        proportion = (count / total_with_any_method) if total_with_any_method else 0.0
        ci_low, ci_high = wilson_ci(count, total_with_any_method)
        rows.append(
            {
                "method": method,
                "count": count,
                "proportion": proportion,
                "ci_low": ci_low,
                "ci_high": ci_high,
            }
        )

    # Sort by descending count, then descending proportion, then name
    rows.sort(key=lambda x: (-x["count"], -x["proportion"], x["method"].lower()))

    return {
        "language": language_label_from_filename(csv_path.name),
        "filename": csv_path.name,
        "n_total_rows": total_repositories,
        "n_total_files": total_files,
        "n_with_any_method": total_with_any_method,
        "n_files_with_any_method": eligible_files,
        "methods": rows,
    }


def emit_comment_block(lang_result: Dict) -> str:
    lines = []
    lines.append(f"% {lang_result['language']}:")
    for row in lang_result["methods"]:
        if row["count"] == 0:
            continue
        pct_int = round(100.0 * row["proportion"])
        lines.append(f"% {row['method']}, {row['count']}, {pct_int}%")
    lines.append(f"% total repositories: {lang_result['n_total_rows']}")
    lines.append(f"% total files: {lang_result['n_total_files']}")
    lines.append(f"% repositories with any method: {lang_result['n_with_any_method']}")
    lines.append(f"% files in repositories with any method: {lang_result['n_files_with_any_method']}")
    return "\n".join(lines)


def build_table_rows(
    per_language: Dict[str, Dict], threshold: float = 0.04
) -> List[str]:
    """
    Keep methods that exceed the threshold in at least one language.
    """
    selected_methods = set()
    for _, result in per_language.items():
        for row in result["methods"]:
            if row["proportion"] > threshold:
                selected_methods.add(row["method"])

    return sorted(selected_methods, key=lambda s: s.lower())


def emit_latex_table(
    per_language: Dict[str, Dict],
    ordered_filenames: List[str],
    threshold: float = 0.04,
) -> str:
    selected_methods = build_table_rows(per_language, threshold=threshold)

    col_labels = [
        latex_escape(language_label_from_filename(filename))
        for filename in ordered_filenames
        if filename in per_language
    ]

    lines = []
    lines.append(r"\begin{table*}[t]")
    lines.append(r"\centering")
    lines.append(
        r"\caption{Prevalence of data access methods by language. "
        r"Cells report prevalence among repositories with at least one detected "
        r"data access method, followed by 95\% Wilson confidence intervals. "
        r"Only methods exceeding 4\% prevalence in at least one language are shown.}"
    )
    lines.append(r"\label{tab:data_access_methods_by_language}")
    lines.append(r"\footnotesize")
    lines.append(r"\setlength{\tabcolsep}{4pt}")
    lines.append(r"\renewcommand{\arraystretch}{1.15}")
    lines.append(r"\begin{tabular}{l" + "c" * len(col_labels) + r"}")
    lines.append(r"\hline")
    lines.append("Method & " + " & ".join(col_labels) + r" \\")
    lines.append(r"\hline")

    for method in selected_methods:
        row_cells = [latex_escape(method)]
        for filename in ordered_filenames:
            if filename not in per_language:
                continue

            result = per_language[filename]
            n = result["n_with_any_method"]

            method_row = next((x for x in result["methods"] if x["method"] == method), None)
            if method_row is None or n == 0:
                row_cells.append("--")
                continue

            pct = round(100.0 * method_row["proportion"])
            ci_low = 100.0 * method_row["ci_low"]
            ci_high = 100.0 * method_row["ci_high"]

            cell = f"{pct}\\% [{ci_low:.1f}, {ci_high:.1f}]"
            row_cells.append(cell)

        lines.append(" & ".join(row_cells) + r" \\")
    lines.append(r"\hline")
    lines.append(r"\end{tabular}")
    lines.append(r"\end{table*}")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Summarize data access method prevalence per language from CSV files "
            "and emit LaTeX comments plus a LaTeX table."
        )
    )
    parser.add_argument(
        "directory",
        type=Path,
        help="Directory containing the language CSV files",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.04,
        help="Minimum prevalence threshold for inclusion in the LaTeX table (default: 0.04 = 4%%)",
    )
    args = parser.parse_args()

    input_dir = args.directory
    if not input_dir.is_dir():
        raise SystemExit(f"Not a directory: {input_dir}")

    per_language = {}
    missing_files = []

    for filename in LANGUAGE_FILE_ORDER:
        csv_path = input_dir / filename
        if not csv_path.exists():
            missing_files.append(filename)
            continue
        per_language[filename] = analyze_language(csv_path)

    if missing_files:
        print("% Warning: the following expected files were not found:")
        for filename in missing_files:
            print(f"%   {filename}")
        print()

    # First: comment blocks, language by language
    for filename in LANGUAGE_FILE_ORDER:
        if filename not in per_language:
            continue
        print(emit_comment_block(per_language[filename]))
        print()

    # Then: LaTeX table
    print(emit_latex_table(per_language, LANGUAGE_FILE_ORDER, threshold=args.threshold))


if __name__ == "__main__":
    main()
