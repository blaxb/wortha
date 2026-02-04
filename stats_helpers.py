from __future__ import annotations

from statistics import median
from typing import Any

from sqlmodel import Session, select

from models import DealContribution


def _safe_avg(values: list[float]) -> float | None:
    if not values:
        return None
    return float(sum(values) / len(values))


def _safe_median(values: list[float]) -> float | None:
    if not values:
        return None
    return float(median(values))


def _clip_outliers(values: list[float]) -> list[float]:
    if len(values) < 5:
        return values
    values_sorted = sorted(values)
    n = len(values_sorted)
    lower_idx = int(n * 0.1)
    upper_idx = int(n * 0.9)
    if upper_idx <= lower_idx:
        return values_sorted
    clipped = values_sorted[lower_idx:upper_idx]
    return clipped if clipped else values_sorted


def summarize_fees_outlier_safe(values: list[float]) -> dict[str, Any]:
    if not values:
        return {
            "count": 0,
            "avg": None,
            "median": None,
            "min": None,
            "max": None,
        }

    values_sorted = sorted(values)
    clipped = _clip_outliers(values_sorted)

    return {
        "count": len(values_sorted),
        "avg": _safe_avg(clipped),
        "median": _safe_median(clipped),
        "min": float(values_sorted[0]),
        "max": float(values_sorted[-1]),
    }


def summarize_cpm_outlier_safe(fees: list[float], views: list[int]) -> dict[str, Any]:
    cpms = []
    for fee, view in zip(fees, views):
        if view is None or view <= 0:
            continue
        if fee is None:
            continue
        cpms.append((float(fee) / float(view)) * 1000)

    if not cpms:
        return {
            "count": 0,
            "avg": None,
            "median": None,
            "min": None,
            "max": None,
        }

    cpms_sorted = sorted(cpms)
    clipped = _clip_outliers(cpms_sorted)

    return {
        "count": len(cpms_sorted),
        "avg": _safe_avg(clipped),
        "median": _safe_median(clipped),
        "min": float(cpms_sorted[0]),
        "max": float(cpms_sorted[-1]),
    }


def get_bucket_community_pricing(
    session: Session,
    platform: str,
    niche: str,
    follower_tier: str,
    geo_region: str | None = None,
    min_deals: int = 5,
) -> dict[str, Any] | None:
    statement = select(DealContribution).where(
        DealContribution.share_in_index == True,
        DealContribution.platform == platform,
        DealContribution.niche == niche,
        DealContribution.follower_tier == follower_tier,
        DealContribution.total_fee_usd > 0,
    )
    if geo_region and geo_region != "other":
        statement = statement.where(DealContribution.geo_region == geo_region)

    rows = session.exec(statement).all()
    fees = [float(row.total_fee_usd) for row in rows if row.total_fee_usd is not None]

    if len(fees) < min_deals:
        return None

    summary = summarize_fees_outlier_safe(fees)
    return {
        "deal_count": summary["count"],
        "avg_fee": summary["avg"],
        "median_fee": summary["median"],
        "min_fee": summary["min"],
        "max_fee": summary["max"],
    }
