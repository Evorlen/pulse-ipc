# SPDX-FileCopyrightText: 2026 Evorlen
# SPDX-License-Identifier: Apache-2.0

import argparse
import csv
import json
import math
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from statistics import median
from typing import TypeVar


Cell = str | int | float | None
Table = tuple[list[str], list[list[Cell]]]
CaseKey = TypeVar("CaseKey")


@dataclass(frozen=True)
class Result:
    values: tuple[float, ...]
    label: str


@dataclass
class Samples:
    name: str
    raw: list[tuple[float, ...]] = field(default_factory=list)
    aggregate: tuple[float, ...] | None = None
    label: str = ""


def metric(row: dict, field_name: str) -> float:
    value = row.get(field_name)
    if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
        raise ValueError(f"expected a finite nonnegative {field_name}")
    return float(value)


def load_report(
    path: Path,
    prefix: str,
    parse_case: Callable[[str], CaseKey],
    read_values: Callable[[dict], tuple[float, ...]],
) -> dict[CaseKey, Result]:
    with path.open(encoding="utf-8") as source:
        try:
            report = json.load(source)
        except ValueError as error:
            raise ValueError(f"{path}: {error}") from error

    if not isinstance(report, dict) or not isinstance(report.get("benchmarks"), list):
        raise ValueError(f"{path}: expected a Google Benchmark JSON report")

    groups: dict[CaseKey, Samples] = {}
    skipped = set()
    for row in report["benchmarks"]:
        if not isinstance(row, dict):
            raise ValueError(f"{path}: expected an object for each benchmark row")

        aggregate_name = row.get("aggregate_name")
        run_type = row.get("run_type", "aggregate" if aggregate_name else "iteration")
        name = row.get("run_name") or row.get("name")
        if not isinstance(name, str) or not name.startswith(prefix):
            continue
        if not row.get("run_name") and isinstance(aggregate_name, str):
            name = name.removesuffix(f"_{aggregate_name}")
        if row.get("skipped") or row.get("error_occurred"):
            skipped.add(name)
            continue
        if run_type != "iteration" and (run_type, aggregate_name) != ("aggregate", "median"):
            continue

        try:
            key = parse_case(name)
            values = read_values(row)
            label = row.get("label", "")
            if not isinstance(label, str):
                raise ValueError("expected a string label")
            group = groups.setdefault(key, Samples(name))
            if group.name != name or (label and group.label and group.label != label):
                raise ValueError("multiple benchmark variants have the same case parameters")
            if label:
                group.label = label
            if run_type == "iteration":
                group.raw.append(values)
            else:
                if group.aggregate is not None:
                    raise ValueError("duplicate median aggregate")
                group.aggregate = values
        except (ValueError, OverflowError) as error:
            raise ValueError(f"{path}: {name}: {error}") from error

    if skipped:
        print(f"{path}: excluded skipped or failed rows from {len(skipped)} case(s).", file=sys.stderr)
    if not groups:
        raise ValueError(f"{path}: no usable results for {prefix.rstrip('/')}")

    results = {}
    for key, group in groups.items():
        values = group.aggregate
        if values is None:
            values = tuple(median(series) for series in zip(*group.raw))
        results[key] = Result(values, group.label)
    return results


def percent_change(current: float | None, baseline: float | None) -> float | None:
    if current is None or baseline is None or baseline == 0:
        return None
    return (current / baseline - 1) * 100


def print_table(headers: list[str], rows: list[list[Cell]]) -> None:
    def format_cell(value: Cell) -> str:
        if value is None:
            return "--"
        if isinstance(value, float):
            return f"{value:.3f}"
        return str(value)

    formatted = [headers, *([format_cell(cell) for cell in row] for row in rows)]
    widths = [max(map(len, column)) for column in zip(*formatted)]
    for index, row in enumerate(formatted):
        line = "  ".join(cell.ljust(width) for cell, width in zip(row, widths))
        print(line.rstrip())
        if index == 0:
            print("  ".join("-" * width for width in widths))


def run_cli(
    description: str, build_table: Callable[..., Table], *, allow_baseline: bool = False
) -> None:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("report", type=Path, help="Google Benchmark JSON report")
    if allow_baseline:
        parser.add_argument("--baseline", type=Path, help="optional baseline JSON for comparison")
    parser.add_argument("--csv", type=Path, help="write the summary table to this CSV file")
    args = parser.parse_args()

    sources = [args.report]
    if allow_baseline and args.baseline is not None:
        sources.append(args.baseline)
    try:
        headers, rows = build_table(*sources)
        if args.csv is not None:
            if args.csv.exists() and any(args.csv.samefile(path) for path in sources):
                raise ValueError("CSV output must be a different file from the input reports")
            with args.csv.open("w", encoding="utf-8", newline="") as destination:
                writer = csv.writer(destination)
                writer.writerow(headers)
                writer.writerows(rows)
        print_table(headers, rows)
    except (OSError, ValueError) as error:
        parser.exit(1, f"{parser.prog}: {error}\n")
