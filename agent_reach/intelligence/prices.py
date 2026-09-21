# -*- coding: utf-8 -*-
"""Named-source price receipts for the investible sentiment tabs."""

from __future__ import annotations

import hashlib
import json
import urllib.request
import time
from datetime import datetime, timezone
from typing import Any, Iterable

from .investibles import Investible
from .schema import iso_sgt, iso_utc, parse_datetime

UTC = timezone.utc


def _symbol(investible: Investible) -> str:
    return investible.ticker.split("/")[0].strip().upper().replace(" ", "-")


def _receipt_id(investible_id: str, instrument: str, timestamp: str | None, price: Any) -> str:
    raw = f"{investible_id}|{instrument}|{timestamp}|{price}"
    return "price-" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]


def _fetch_yahoo(symbol: str, end: datetime, timeout: int = 8) -> tuple[float | None, datetime | None, str, str | None]:
    start = int((end.timestamp() - 7 * 86400))
    finish = int(end.timestamp() + 86400)
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?period1={start}&period2={finish}&interval=1d&events=history"
    try:
        request = urllib.request.Request(url, headers={"User-Agent": "agent-reach-price-receipt/1.0"})
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = json.loads(response.read(2 * 1024 * 1024).decode("utf-8"))
        result = payload["chart"]["result"][0]
        timestamps = result.get("timestamp") or []
        closes = (result.get("indicators", {}).get("quote", [{}])[0].get("close") or [])
        values = [(datetime.fromtimestamp(int(ts), tz=UTC), float(close)) for ts, close in zip(timestamps, closes) if close is not None]
        values = [item for item in values if item[0] <= end]
        if not values:
            return None, None, url, "no named-source observation at or before publish time"
        observed_at, price = values[-1]
        return price, observed_at, url, None
    except Exception as exc:
        return None, None, url, f"{type(exc).__name__}: {exc}"[:400]


def _fetch_coinbase(symbol: str, timeout: int = 8) -> tuple[float | None, datetime | None, str, str | None]:
    pair = "BTC-USD" if symbol == "BTC" else "ETH-USD"
    url = f"https://api.coinbase.com/v2/prices/{pair}/spot"
    try:
        request = urllib.request.Request(url, headers={"User-Agent": "agent-reach-price-receipt/1.0"})
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = json.loads(response.read(256 * 1024).decode("utf-8"))
        price = float(payload["data"]["amount"])
        return price, datetime.now(UTC), url, None
    except Exception as exc:
        return None, None, url, f"{type(exc).__name__}: {exc}"[:400]


def collect_price_receipts(
    investibles: Iterable[Investible],
    *,
    published_at_utc: datetime,
    retrieved_at_utc: datetime,
    timeout_seconds: int = 45,
) -> list[dict[str, Any]]:
    receipts: list[dict[str, Any]] = []
    deadline = time.monotonic() + max(1, int(timeout_seconds))
    for investible in investibles:
        instrument = _symbol(investible)
        if time.monotonic() >= deadline:
            receipts.append({"receipt_id": _receipt_id(investible.investible_id, instrument, None, None), "investible_id": investible.investible_id, "name": investible.name, "instrument": instrument, "venue": None, "currency": "USD", "unit": "USD per named instrument", "publish_timestamp_utc": iso_utc(published_at_utc), "entry_timestamp_utc": None, "entry_timestamp_sgt": None, "entry_price": None, "source_name": None, "source_url": None, "retrieved_at_utc": iso_utc(retrieved_at_utc), "timing_rule": None, "status": "ENTRY PRICE UNAVAILABLE", "error": "price-receipt budget exhausted", "immutable": True})
            continue
        request_timeout = max(1, min(8, int(deadline - time.monotonic())))
        if investible.investible_id in {"bitcoin", "ethereum"}:
            price, observed, url, error = _fetch_coinbase(instrument, timeout=request_timeout)
            timing_rule = "CONTINUOUS_FIRST_OBSERVATION_AT_OR_AFTER_PUBLISH"
            currency = "USD"
        else:
            price, observed, url, error = _fetch_yahoo(instrument, published_at_utc, timeout=request_timeout)
            timing_rule = "MARKET_CLOSED_BASELINE" if observed and observed < published_at_utc else "FIRST_OBSERVATION_AT_OR_AFTER_PUBLISH"
            currency = "USD"
        status = "OK" if price is not None and observed is not None else "ENTRY PRICE UNAVAILABLE"
        receipts.append({
            "receipt_id": _receipt_id(investible.investible_id, instrument, iso_utc(observed), price),
            "investible_id": investible.investible_id,
            "name": investible.name,
            "instrument": instrument,
            "venue": "Coinbase spot" if investible.investible_id in {"bitcoin", "ethereum"} else "Yahoo Finance chart",
            "currency": currency,
            "unit": "USD per named instrument",
            "publish_timestamp_utc": iso_utc(published_at_utc),
            "entry_timestamp_utc": iso_utc(observed),
            "entry_timestamp_sgt": iso_sgt(observed),
            "entry_price": price,
            "source_name": "Coinbase spot API" if investible.investible_id in {"bitcoin", "ethereum"} else "Yahoo Finance chart API",
            "source_url": url,
            "retrieved_at_utc": iso_utc(retrieved_at_utc),
            "timing_rule": timing_rule,
            "status": status,
            "error": error,
            "immutable": True,
        })
    return receipts


def resolve_previous_sentiments(
    previous: list[dict[str, Any]],
    previous_receipts: list[dict[str, Any]],
    *,
    retrieved_at_utc: datetime,
    timeout_seconds: int = 45,
) -> list[dict[str, Any]]:
    current_by_id = {str(row.get("investible_id")): row for row in previous_receipts}
    resolved: list[dict[str, Any]] = []
    deadline = time.monotonic() + max(1, int(timeout_seconds))
    for item in previous:
        investible_id = str(item.get("investible_id"))
        entry = current_by_id.get(investible_id)
        current_price = None
        current_timestamp = None
        source_url = None
        if entry and entry.get("entry_price") is not None and time.monotonic() < deadline:
            symbol = str(entry.get("instrument") or "")
            request_timeout = max(1, min(8, int(deadline - time.monotonic())))
            if investible_id in {"bitcoin", "ethereum"}:
                current_price, current_timestamp, source_url, _ = _fetch_coinbase(symbol, timeout=request_timeout)
            else:
                current_price, current_timestamp, source_url, _ = _fetch_yahoo(symbol, retrieved_at_utc, timeout=request_timeout)
        entry_price = entry.get("entry_price") if entry else None
        if entry_price is None or current_price is None:
            direction = "UNRESOLVED - PRICE RECEIPT MISSING"
            absolute = percentage = None
        else:
            absolute = round(float(current_price) - float(entry_price), 8)
            percentage = round(absolute / float(entry_price) * 100, 6) if float(entry_price) else None
            direction = "UP" if absolute > 0 else "DOWN" if absolute < 0 else "UNCHANGED"
        resolved.append({
            "investible_id": investible_id,
            "name": item.get("name"),
            "previous_positive_percent": item.get("positive_percent"),
            "previous_negative_percent": item.get("negative_percent"),
            "talk_volume": item.get("retained_post_count", 0),
            "entry_price": entry_price,
            "entry_timestamp_utc": entry.get("entry_timestamp_utc") if entry else None,
            "resolved_price": current_price,
            "resolved_timestamp_utc": iso_utc(current_timestamp),
            "absolute_move": absolute,
            "percentage_move": percentage,
            "direction": direction,
            "source_url": source_url or (entry.get("source_url") if entry else None),
            "explanation": "Price movement is an observation after the previous sentiment edition; it does not prove that sentiment caused the move.",
            "retrieved_at_utc": iso_utc(retrieved_at_utc),
        })
    return resolved
