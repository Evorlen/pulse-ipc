# SPDX-FileCopyrightText: 2026 Evorlen
# SPDX-License-Identifier: Apache-2.0

import re
from pathlib import Path

from _report import Table, load_report, metric, percent_change, run_cli


PREFIX = "spsc_queue/throughput/"
CASE_PATTERN = re.compile(
    rf"{PREFIX}([^/]+)/capacity:(\d+)/payload:(\d+)/batch:(\d+)(?:/.*)?"
)


def parse_case(name: str) -> tuple[str, int, int, int]:
    match = CASE_PATTERN.fullmatch(name)
    if match is None:
        raise ValueError("expected capacity, payload and batch in the benchmark name")
    mode, capacity, payload, batch = match.groups()
    return mode, int(capacity), int(payload), int(batch)


def read_rates(row: dict) -> tuple[float, float]:
    return metric(row, "items_per_second") / 1_000_000, metric(row, "bytes_per_second") / (1 << 20)


def make_table(report_path: Path, baseline_path: Path | None = None) -> Table:
    report = load_report(report_path, PREFIX, parse_case, read_rates)
    baseline = load_report(baseline_path, PREFIX, parse_case, read_rates) if baseline_path else {}

    fastest = {}
    for case in sorted(report):
        group = case[:3]
        if group not in fastest or report[case].values[0] > report[fastest[group]].values[0]:
            fastest[group] = case

    headers = ["Mode", "Capacity", "Payload (B)", "Batch", "M msg/s", "MiB/s", "Best batch"]
    if baseline_path:
        headers += ["Baseline M msg/s", "Baseline MiB/s", "Msg/s change (%)"]

    rows = []
    for case in sorted(report.keys() | baseline.keys()):
        current = report.get(case)
        base = baseline.get(case)
        messages, bandwidth = current.values if current else (None, None)
        best = "yes" if fastest.get(case[:3]) == case else None
        row = [*case, messages, bandwidth, best]
        if baseline_path:
            base_messages, base_bandwidth = base.values if base else (None, None)
            row += [base_messages, base_bandwidth, percent_change(messages, base_messages)]
        rows.append(row)
    return headers, rows


if __name__ == "__main__":
    run_cli(
        "Summarize SPSC queue throughput from a Google Benchmark JSON report.",
        make_table,
        allow_baseline=True,
    )
