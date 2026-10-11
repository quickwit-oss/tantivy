"""Aggregates the benchmark runs into a single JSON document used by the HTML report.

Usage: python3 report/aggregate.py <output.json> <run_dir>...

Each run dir contains `results.jsonl` (timings) and `verify.jsonl` (result checks).
If the same (input, group, variant) appears several times in a run dir (a rerun), the last
measurement of that run dir wins.

For each (input, request):
- the time of each variant is the median over runs of the binggan median,
- the ratios (opaque/columnar, vortex/columnar, vortex/opaque) are computed within each run
  (the variants of a request are interleaved in the same binggan group), then the median over
  runs is taken,
- `spread` is (max - min) / median of the per-run vortex/columnar ratio.
"""

import json
import math
import statistics
import sys
from collections import defaultdict

VARIANTS = ["columnar", "opaque", "vortex"]


def load_jsonl(path):
    try:
        with open(path) as f:
            return [json.loads(line) for line in f if line.strip()]
    except FileNotFoundError:
        return []


def main():
    output_path = sys.argv[1]
    run_dirs = sys.argv[2:]
    # (input, group) -> variant -> [median_ns per run]
    timings = defaultdict(lambda: defaultdict(list))
    # (input, group) -> ratio name -> [ratio per run]
    ratios = defaultdict(lambda: defaultdict(list))
    memory = defaultdict(lambda: defaultdict(list))
    verify = {}
    for run_dir in run_dirs:
        per_run = {}
        for row in load_jsonl(f"{run_dir}/results.jsonl"):
            per_run[(row["input"], row["group"], row["variant"])] = row
        by_request = defaultdict(dict)
        for (input_name, group, variant), row in per_run.items():
            by_request[(input_name, group)][variant] = row
        for key, rows in by_request.items():
            for variant, row in rows.items():
                timings[key][variant].append(row["median_ns"])
                memory[key][variant].append(row["avg_memory"])
            if "columnar" in rows:
                base = rows["columnar"]["median_ns"]
                for variant in ("opaque", "vortex"):
                    if variant in rows:
                        ratios[key][f"{variant}/columnar"].append(rows[variant]["median_ns"] / base)
            if "opaque" in rows and "vortex" in rows:
                ratios[key]["vortex/opaque"].append(
                    rows["vortex"]["median_ns"] / rows["opaque"]["median_ns"]
                )
        for row in load_jsonl(f"{run_dir}/verify.jsonl"):
            verify[(row["input"], row["group"], row["variant"])] = row

    requests = []
    for key in sorted(timings.keys()):
        input_name, group = key
        top_group, _, request = group.partition("/")
        entry = {
            "input": input_name,
            "group": top_group,
            "request": request,
            "times_ms": {},
            "runs": {},
            "memory": {},
            "ratios": {},
            "ratio_runs": {},
            "verify": {},
        }
        for variant in VARIANTS:
            values = timings[key].get(variant)
            if values:
                entry["times_ms"][variant] = statistics.median(values) / 1e6
                entry["runs"][variant] = [value / 1e6 for value in values]
                entry["memory"][variant] = statistics.median(memory[key][variant])
        for ratio_name, values in ratios[key].items():
            entry["ratios"][ratio_name] = statistics.median(values)
            entry["ratio_runs"][ratio_name] = values
        vortex_ratios = ratios[key].get("vortex/columnar", [])
        if len(vortex_ratios) >= 2:
            median = statistics.median(vortex_ratios)
            entry["spread"] = (max(vortex_ratios) - min(vortex_ratios)) / median
        for variant in VARIANTS:
            row = verify.get((input_name, group, variant))
            if row is None:
                continue
            status = row["status"]
            if status == "different":
                opaque_row = verify.get((input_name, group, "opaque"))
                if (
                    variant == "vortex"
                    and opaque_row is not None
                    and opaque_row.get("result_hash") == row.get("result_hash")
                ):
                    status = "different_same_as_opaque"
            entry["verify"][variant] = {"status": status, "error": row.get("error")}
        requests.append(entry)

    # Requests that never ran for some variants (errors) still need their verify status.
    known = {(entry["input"], entry["group"] + "/" + entry["request"]) for entry in requests}
    for (input_name, group, variant), row in verify.items():
        if (input_name, group) in known:
            continue
        top_group, _, request = group.partition("/")
        requests.append(
            {
                "input": input_name,
                "group": top_group,
                "request": request,
                "times_ms": {},
                "runs": {},
                "memory": {},
                "ratios": {},
                "ratio_runs": {},
                "verify": {},
            }
        )
        known.add((input_name, group))
    for entry in requests:
        if entry["verify"]:
            continue
        group = entry["group"] + "/" + entry["request"]
        for variant in VARIANTS:
            row = verify.get((entry["input"], group, variant))
            if row is not None:
                entry["verify"][variant] = {"status": row["status"], "error": row.get("error")}

    def geomean(values):
        values = [value for value in values if value > 0]
        if not values:
            return None
        return math.exp(sum(math.log(value) for value in values) / len(values))

    summary = defaultdict(dict)
    for ratio_name in ("opaque/columnar", "vortex/columnar", "vortex/opaque"):
        groups = defaultdict(list)
        for entry in requests:
            ratio = entry["ratios"].get(ratio_name)
            if ratio is None:
                continue
            groups[(entry["input"], entry["group"])].append(ratio)
            groups[(entry["input"], "*")].append(ratio)
        for (input_name, group), values in groups.items():
            summary[f"{input_name}|{group}"][ratio_name] = {
                "geomean": geomean(values),
                "count": len(values),
                "min": min(values),
                "max": max(values),
            }

    with open(output_path, "w") as f:
        json.dump({"requests": requests, "summary": summary, "num_runs": len(run_dirs)}, f)


if __name__ == "__main__":
    main()
