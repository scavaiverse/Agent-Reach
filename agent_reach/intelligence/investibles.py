# -*- coding: utf-8 -*-
"""The OTR investible universe used by the social-sentiment lane."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Investible:
    investible_id: str
    name: str
    ticker: str
    aliases: tuple[str, ...] = ()
    tier: str = "watch"

    @property
    def query_variants(self) -> tuple[str, ...]:
        values = (self.name, self.ticker, *self.aliases)
        seen: set[str] = set()
        result: list[str] = []
        for value in values:
            cleaned = " ".join(str(value).split()).strip()
            if cleaned and cleaned.casefold() not in seen:
                seen.add(cleaned.casefold())
                result.append(cleaned)
        return tuple(result)

    def to_dict(self) -> dict[str, object]:
        return {
            "investible_id": self.investible_id,
            "name": self.name,
            "ticker": self.ticker,
            "aliases": list(self.aliases),
            "tier": self.tier,
            "query_variants": list(self.query_variants),
        }


# Mirrors the current OTR watchboard vocabulary.  A meter may still be
# INSUFFICIENT DATA; that is preferable to inventing a percentage.
WATCHED_INVESTIBLES: tuple[Investible, ...] = (
    Investible("sandisk", "SanDisk", "SNDK", ("NAND storage", "memory stocks"), "AI infrastructure"),
    Investible("micron", "Micron", "MU", ("Micron Technology", "memory chips", "DRAM"), "AI infrastructure"),
    Investible("western-digital", "Western Digital", "WDC", ("WD", "data storage", "hard drives"), "AI infrastructure"),
    Investible("seagate", "Seagate", "STX", ("Seagate Technology", "hard drives", "data storage"), "AI infrastructure"),
    Investible("technology-sector-etf", "Technology sector ETF", "XLK", ("technology ETF", "tech sector", "technology shares"), "the broadening"),
    Investible("russell-2000", "Russell 2000 small caps", "IWM", ("Russell 2000", "small caps", "small-cap stocks"), "the broadening"),
    Investible("us-large-cap-value", "US large-cap value", "VTV", ("large-cap value", "value stocks", "US value ETF"), "the broadening"),
    Investible("us-industrials", "US industrials", "XLI", ("industrial stocks", "industrials ETF"), "the broadening"),
    Investible("us-materials", "US materials", "XLB", ("materials stocks", "materials ETF"), "the broadening"),
    Investible("us-real-estate", "US real estate", "VNQ", ("REITs", "real estate ETF", "property stocks"), "the broadening"),
    Investible("emerging-markets", "Emerging markets", "VWO / EEM", ("EM equities", "developing markets"), "outside the US"),
    Investible("brazil-equities", "Brazil equities", "FLBR", ("Brazil stocks", "Brazil ETF", "Brazilian equities"), "outside the US"),
    Investible("developed-international-shares", "Developed international shares", "EFA", ("international shares", "developed markets", "global ex-US"), "outside the US"),
    Investible("india-southeast-asia", "India and Southeast Asia", "INDA / ASEA", ("India stocks", "Southeast Asia equities", "ASEAN markets"), "outside the US"),
    Investible("energy-and-oil", "Energy shares and oil", "XLE / USO", ("energy stocks", "oil", "crude oil", "WTI"), "energy"),
    Investible("oil-services", "Oil services", "OIH", ("oil-service stocks", "oilfield services"), "energy"),
    Investible("bitcoin", "Bitcoin", "BTC", ("Bitcoin USD", "BTC-USD", "比特币", "ビットコイン", "비트코인"), "crypto"),
    Investible("ethereum", "Ethereum", "ETH", ("Ethereum USD", "ETH-USD", "以太坊", "イーサリアム", "이더리움"), "crypto"),
    Investible("gold", "Gold", "GLDM", ("gold ETF", "precious metals", "黄金", "金"), "contrarian"),
    Investible("us-treasury-bills", "US Treasury bills", "SGOV / BIL", ("Treasury bills", "T-bills", "short-term Treasuries"), "capital defence"),
)


def selected_investibles() -> tuple[Investible, ...]:
    return WATCHED_INVESTIBLES
