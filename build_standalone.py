#!/usr/bin/env python3
"""
Build a standalone index.html with data.json embedded.
This version works when opened directly as a file:// URL.

Run: python3 build_standalone.py
Output: standalone.html (open directly in browser, no server needed)
"""

import json
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
DATA_PATH = SCRIPT_DIR / "data.json"
INDEX_PATH = SCRIPT_DIR / "index.html"
OUTPUT_PATH = SCRIPT_DIR / "standalone.html"

def main():
    if not DATA_PATH.exists():
        print("✗ data.json not found. Run collect.py first.")
        return

    with open(DATA_PATH) as f:
        data = json.load(f)

    with open(INDEX_PATH) as f:
        html = f.read()

    # Inject data as a script tag before the LAST closing </body>
    # (Must use rfind, not replace, because </body> also appears inside JS strings)
    data_script = f'<script>window.SUPERNEWS_DATA = {json.dumps(data)};</script>\n'
    last_body = html.rfind('</body>')
    if last_body == -1:
        html += data_script
    else:
        html = html[:last_body] + data_script + html[last_body:]

    with open(OUTPUT_PATH, 'w') as f:
        f.write(html)

    count = data.get('article_count', 0)
    print(f"✅ Built standalone.html with {count} articles embedded")
    print(f"   Open directly in browser — no server needed.")

if __name__ == "__main__":
    main()
