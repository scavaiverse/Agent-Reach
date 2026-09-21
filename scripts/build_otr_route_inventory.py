"""Build a truthful public-route inventory from the live OTR site."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urldefrag, urljoin, urlsplit
from urllib.request import Request, urlopen


class _Links(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: set[str] = set()

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        for key, value in attrs:
            if key.lower() == "href" and value:
                self.links.add(value)


def _fetch(base: str, path: str) -> tuple[str, int]:
    url = urljoin(base.rstrip("/") + "/", path.lstrip("/"))
    request = Request(url, headers={"User-Agent": "otr-route-inventory/1.0"})
    with urlopen(request, timeout=15) as response:
        return response.read(2 * 1024 * 1024).decode("utf-8", errors="replace"), int(response.status)


def _route(path: str) -> dict[str, object]:
    lower = path.casefold()
    if lower in {"/robots.txt", "/sitemap.xml", "/semantic-manifest.json"} or lower.endswith(".json"):
        classification = "NOT_APPROPRIATE"
        data_contract = "machine-readable or crawler control; no social panel"
    elif lower in {"/", "/about", "/trust", "/editorial-standard", "/login", "/signup", "/site-map"}:
        classification = "OPTIONAL"
        data_contract = "editorial or utility page; only show chatter when an explicit item match exists"
    else:
        classification = "SOCIAL_LAYER_REQUIRED" if lower in {
            "/headlines", "/today", "/predictions", "/predictions-index", "/predictions-revisited",
            "/connections", "/graph", "/next", "/learning", "/learning-archive", "/proof", "/sources",
            "/business", "/risks", "/top-3", "/long-term", "/new-insight", "/lessons", "/investibles",
        } else "OPTIONAL"
        data_contract = "only semantically matched Top-30 topics; unmatched topics go to research queue"
    return {
        "route": path,
        "classification": classification,
        "data_contract": data_contract,
        "rendering_contract": "collapsed native details/summary, keyboard accessible, no raw JSON, empty-safe",
        "seo_contract": "preserve existing title, canonical, robots, sitemap, and structured metadata",
        "evidence_contract": "show representative social-buzz URLs separately from factual verification URLs",
    }


def build(base: str, output: Path, production_version: str | None) -> dict[str, object]:
    discovered: set[str] = {"/", "/robots.txt", "/sitemap.xml"}
    source_status: dict[str, int] = {}
    for path in ("/", "/site-map/", "/sitemap.xml", "/robots.txt"):
        try:
            body, status = _fetch(base, path)
            source_status[path] = status
            if path.endswith("/") or path == "/":
                parser = _Links()
                parser.feed(body)
                for href in parser.links:
                    absolute = urljoin(base.rstrip("/") + "/", href)
                    split = urlsplit(urldefrag(absolute)[0])
                    if split.netloc and split.netloc != urlsplit(base).netloc:
                        continue
                    route = split.path or "/"
                    if split.query and route not in {"/search"}:
                        continue
                    discovered.add(route.rstrip("/") or "/")
            elif path == "/sitemap.xml":
                for token in body.split("<loc>")[1:]:
                    absolute = token.split("</loc>", 1)[0].strip()
                    split = urlsplit(absolute)
                    if split.path:
                        discovered.add(split.path.rstrip("/") or "/")
        except Exception as exc:
            source_status[path] = 0
            source_status[f"{path}#error"] = str(exc)[:300]
    routes = sorted({_route(path)["route"] for path in discovered})
    payload = {
        "schema_version": "otr-public-route-inventory-1",
        "base_url": base.rstrip("/"),
        "crawled_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "production_version": production_version,
        "crawl_sources": source_status,
        "routes": [_route(path) for path in routes],
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="https://new.ontherice.org")
    parser.add_argument("--output", default="output/OTR_PUBLIC_ROUTE_INVENTORY.json")
    parser.add_argument("--production-version", default=None)
    args = parser.parse_args()
    payload = build(args.base, Path(args.output), args.production_version)
    print(json.dumps({"route_count": len(payload["routes"]), "output": args.output}, ensure_ascii=False))


if __name__ == "__main__":
    main()
