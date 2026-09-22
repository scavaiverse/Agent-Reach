"""Targeted channel execution for one frozen OTR anchor."""

from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime, timezone
from typing import Any

import yaml

from agent_reach.channels.v2ex import V2EXChannel


def _run(command: list[str], timeout: float) -> tuple[int, str, str, str]:
    env = dict(os.environ)
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"
    try:
        proc = subprocess.run(command, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout, env=env)
        return proc.returncode, proc.stdout, proc.stderr, "COMPLETE"
    except subprocess.TimeoutExpired as exc:
        return 124, (exc.stdout or ""), (exc.stderr or ""), "TIMEOUT"
    except OSError as exc:
        return 127, "", str(exc), "ERROR"


def _bili(query: str, timeout: float) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    code, stdout, stderr, state = _run(["bili", "search", query, "--type", "video", "-n", "5"], timeout)
    try:
        payload = yaml.safe_load(stdout) if stdout else {}
    except yaml.YAMLError:
        payload = {}
    rows = (payload or {}).get("data", []) if isinstance(payload, dict) else []
    records = []
    for row in rows if isinstance(rows, list) else []:
        bvid = row.get("bvid") or row.get("id")
        records.append({
            "platform": "bilibili", "backend": "bili-cli", "title": row.get("title"), "original_text": row.get("title"),
            "author": row.get("author"), "url": f"https://www.bilibili.com/video/{bvid}" if bvid else None,
            "published_at_utc": row.get("pubdate"), "views": row.get("play"), "source_type": "video", "evidence_type": "social_video",
            "metadata": {"bvid": bvid, "duration": row.get("duration"), "date_quality": "provided" if row.get("pubdate") else "not_provided"},
        })
    return records, {"exit_code": code, "stdout": stdout, "stderr": stderr, "state": state, "parser": "yaml"}


def _youtube(query: str, timeout: float) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    code, stdout, stderr, state = _run(["yt-dlp", "--flat-playlist", "--dump-single-json", f"ytsearch5:{query}"], timeout)
    try:
        payload = json.loads(stdout) if stdout else {}
    except json.JSONDecodeError:
        payload = {}
    records = []
    for row in (payload or {}).get("entries", []) if isinstance(payload, dict) else []:
        if not isinstance(row, dict):
            continue
        url = row.get("webpage_url") or row.get("url")
        if url and not str(url).startswith("http") and row.get("id"):
            url = f"https://www.youtube.com/watch?v={row['id']}"
        records.append({
            "platform": "youtube", "backend": "yt-dlp", "title": row.get("title"), "original_text": row.get("title"),
            "description": row.get("description"), "author": row.get("channel") or row.get("uploader"), "url": url,
            "published_at_utc": row.get("timestamp") or row.get("release_timestamp"), "views": row.get("view_count"),
            "source_type": "video", "evidence_type": "social_video",
            "metadata": {"content_fields_checked": [k for k in ("title", "description", "timestamp") if row.get(k) is not None], "direct_content": bool(row.get("description"))},
        })
    return records, {"exit_code": code, "stdout": stdout, "stderr": stderr, "state": state, "parser": "json"}


def _github(query: str, timeout: float) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    fields = "title,body,url,author,createdAt,commentsCount"
    code, stdout, stderr, state = _run(["gh", "search", "issues", query, "--limit", "10", "--json", fields], timeout)
    try:
        rows = json.loads(stdout) if stdout else []
    except json.JSONDecodeError:
        rows = []
    records = []
    for row in rows if isinstance(rows, list) else []:
        author = row.get("author") or {}
        records.append({
            "platform": "github", "backend": "gh CLI", "title": row.get("title"), "original_text": row.get("body") or row.get("title"),
            "author": author.get("login") if isinstance(author, dict) else author, "url": row.get("url"),
            "published_at_utc": row.get("createdAt"), "comments": row.get("commentsCount"), "source_type": "issue", "evidence_type": "community_issue",
        })
    return records, {"exit_code": code, "stdout": stdout, "stderr": stderr, "state": state, "parser": "json"}


def _v2ex(query: str, timeout: float) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    # The public V2EX API has no full-text search. A live hot-topic read is
    # still useful, but relevance is decided later and the limitation is explicit.
    try:
        rows = V2EXChannel().get_hot_topics(limit=30)
        raw = {"method": "get_hot_topics", "query": query, "records": rows}
        records = []
        for row in rows:
            records.append({
                "platform": "v2ex", "backend": "V2EX API (public)", "title": row.get("title"), "original_text": row.get("content") or row.get("title"),
                "author": row.get("member", {}).get("username") if isinstance(row.get("member"), dict) else None, "url": row.get("url"),
                "published_at_utc": row.get("created"), "comments": row.get("replies"), "source_type": "topic", "evidence_type": "community_topic",
            })
        return records, {"exit_code": 0, "stdout": json.dumps(raw, ensure_ascii=False), "stderr": "", "state": "COMPLETE", "parser": "v2ex-api", "search_limitation": "hot topics read; no full-text API"}
    except Exception as exc:  # noqa: BLE001
        return [], {"exit_code": 1, "stdout": "", "stderr": str(exc), "state": "ERROR", "parser": "v2ex-api"}


def execute_lane(platform: str, query: str, *, doctor: dict[str, Any], timeout: float = 15.0) -> dict[str, Any]:
    started = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    status = doctor.get(platform, {})
    doctor_status = status.get("status")
    active = status.get("active_backend")
    backend = active or (status.get("backends") or ["unknown"])[0]
    if platform in {"twitter", "reddit", "facebook", "instagram", "xiaohongshu", "linkedin"} and doctor_status != "ok":
        terminal = "AUTH_REQUIRED" if doctor_status == "warn" else "UNAVAILABLE"
        return {"platform": platform, "backend": backend, "query": query, "started_at_utc": started, "finished_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"), "status": terminal, "records": [], "response": {"reason": status.get("message")}}
    if platform == "bilibili":
        records, response = _bili(query, timeout)
    elif platform == "youtube":
        records, response = _youtube(query, timeout)
    elif platform == "github":
        records, response = _github(query, timeout)
    elif platform == "v2ex":
        records, response = _v2ex(query, timeout)
    else:
        return {"platform": platform, "backend": backend, "query": query, "started_at_utc": started, "finished_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"), "status": "UNAVAILABLE", "records": [], "response": {"reason": "No documented targeted backend in this installation"}}
    terminal = response.get("state", "ERROR")
    if terminal == "COMPLETE" and not records:
        terminal = "SEARCHED_EMPTY"
    return {"platform": platform, "backend": backend, "query": query, "started_at_utc": started, "finished_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"), "status": terminal, "records": records, "response": response}
