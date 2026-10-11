"""Assembles the English report: narrative parts, code snippets and program outputs, data.

Usage: python3 report/build_en.py

Code snippets come from `examples/vortex_client_usage.rs`, between `// --- [name]` and
`// --- [/name]` markers. Outputs come from `runs/client_usage_output.txt`.
"""

import html
import re
import subprocess
import textwrap

ROOT = "/Users/paul.masurel/git/tantivy_vortex/vortex-bench"
EXAMPLE = f"{ROOT}/examples/vortex_client_usage.rs"
OUTPUT = f"{ROOT}/runs/client_usage_output.txt"


def snippets():
    source = open(EXAMPLE).read()
    result = {}
    for match in re.finditer(r"// --- \[(\w+)\]\n(.*?)\n\s*// --- \[/\1\]", source, re.S):
        result[match.group(1)] = textwrap.dedent(match.group(2)).strip("\n")
    return result


def output_lines():
    return open(OUTPUT).read().splitlines()


def take_output(lines, spec):
    """`prefix#n`: n lines starting at the first line starting with prefix."""
    prefix, _, count = spec.partition("#")
    count = int(count or "1")
    for i, line in enumerate(lines):
        if line.startswith(prefix):
            return lines[i : i + count]
    raise KeyError(spec)


def sparse_sweep_rows(lines):
    rows = []
    for line in lines:
        match = re.match(r"\s+density\s+([\d.]+) %:\s+(\d+) bytes \(\s*([\d.]+) bytes per value\), tree (.*)", line)
        if match:
            rows.append((match.group(1) + " % (random)", match.group(2), match.group(3), match.group(4)))
    regular = next(line for line in lines if line.startswith("regular 1%:"))
    match = re.match(r"regular 1%: (\d+) bytes, tree (.*)", regular)
    num_values = 656
    rows.insert(2, ("1.0 % (one every 100 rows)", match.group(1), f"{int(match.group(1)) / num_values:.2f}", match.group(2)))
    out = []
    for density, size, per_value, tree in rows:
        out.append(
            f'<tr><td class="l">{html.escape(density)}</td><td>{int(size):,} B</td><td>{per_value}</td>'
            f'<td class="l mono" style="font-size:0.75rem;white-space:normal">{html.escape(tree)}</td></tr>'
        )
    return "\n".join(out)


def sparse_trees(lines):
    blocks = []
    def block(title, start_prefix, count):
        chunk = take_output(lines, f"{start_prefix}#{count}")
        body = [l.strip() if i == 0 else l for i, l in enumerate(chunk)]
        blocks.append(f"{title}\n" + "\n".join(textwrap.dedent("\n".join(chunk[1:])).splitlines()))
    # 1 % random: the print_tree block right after the "sparse 1%:" line.
    i = next(i for i, l in enumerate(lines) if l.startswith("sparse 1%:"))
    blocks.append("1 % of rows, random positions\n" + textwrap.dedent("\n".join(lines[i + 1 : i + 5])))
    i = next(i for i, l in enumerate(lines) if l.startswith("regular 1%:"))
    blocks.append("1 % of rows, one every 100 rows\n" + textwrap.dedent("\n".join(lines[i + 1 : i + 5])))
    i = next(i for i, l in enumerate(lines) if "density   0.1 %" in l)
    blocks.append("0.1 % of rows, random positions\n" + textwrap.dedent("\n".join(lines[i + 1 : i + 6])))
    i = next(i for i, l in enumerate(lines) if "density  30.0 %" in l)
    blocks.append("30 % of rows, random positions\n" + textwrap.dedent("\n".join(lines[i + 1 : i + 3])))
    return html.escape("\n\n".join(blocks))


def main():
    parts = [open(f"{ROOT}/report/{name}").read() for name in ["en_summary.html", "en_part1.html", "en_sizes.html", "en_perf.html", "en_part3.html"]]
    extra = open(f"{ROOT}/report/en_vortex_extra.html").read()
    body = "".join(parts)
    marker = '<section id="mapping">'
    body = body.replace("</section>\n\n" + marker, extra + "</section>\n\n" + marker, 1)
    assert extra[:40] in body, "vortex extra section not inserted"
    code = snippets()
    body = re.sub(r"<!--SNIPPET:(\w+)-->", lambda m: html.escape(code[m.group(1)]), body)
    lines = output_lines()
    def output(match):
        specs = match.group(1).split("|")
        if len(specs) > 1 and "#" not in match.group(1):
            # Each prefix contributes the first matching line.
            return html.escape("\n".join(take_output(lines, spec)[0] for spec in specs))
        return html.escape("\n".join(l for spec in specs for l in take_output(lines, spec)))
    body = re.sub(r"<!--OUTPUT:(.*?)-->", output, body)
    body = body.replace('<tbody id="sparse-sweep"></tbody>', f'<tbody id="sparse-sweep">{sparse_sweep_rows(lines)}</tbody>')
    body = body.replace('<pre id="sparse-trees"></pre>', f'<pre id="sparse-trees">{sparse_trees(lines)}</pre>')
    open(f"{ROOT}/report/body_en.html", "w").write(body)
    data = [
        "meta=runs/meta_en.json", "labels=runs/labels_en.json", "phase1=runs/agg_v2.json",
        "phase2=runs/agg_phase2.json", "phase3=runs/agg_v3.json", "v3a=runs/agg_v3a.json",
        "v1=runs/agg_v1.json", "sizes=runs/sizes.json", "decode=runs/final_decode.json",
        "point_decode=runs/point5_decode.json", "take4=runs/final_decode_take_4.json",
        "struct=runs/struct_sizes.json", "tantivy_codecs=runs/tantivy_codecs.json", "ab=runs/ab_table.json",
    ]
    subprocess.run(
        ["python3", "report/gen_report.py", "report/template_en.html", "report/body_en.html", "report/vortex_study_en.html"] + data,
        cwd=ROOT,
        check=True,
    )


if __name__ == "__main__":
    main()
