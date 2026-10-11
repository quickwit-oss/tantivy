"""Builds the HTML report from the aggregated benchmark data.

Usage: python3 report/gen_report.py <template.html> <body.html> <output.html> key=path.json ...

`body.html` (the narrative) replaces the `<!--__BODY__-->` marker of the template.
Every `key=path.json` pair is embedded in the page as `DATA[key]`.
"""

import json
import sys


def main():
    template_path, body_path, output_path = sys.argv[1], sys.argv[2], sys.argv[3]
    data = {}
    for pair in sys.argv[4:]:
        key, _, path = pair.partition("=")
        with open(path) as f:
            data[key] = json.load(f)
    with open(template_path) as f:
        template = f.read()
    payload = json.dumps(data, separators=(",", ":")).replace("</", "<\\/")
    with open(body_path) as f:
        body = f.read()
    html = template.replace("<!--__BODY__-->", body).replace("/*__DATA__*/{}", payload)
    with open(output_path, "w") as f:
        f.write(html)


if __name__ == "__main__":
    main()
