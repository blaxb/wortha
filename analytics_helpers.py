from __future__ import annotations

from datetime import datetime
from statistics import median
from typing import Any

from sqlmodel import Session, select

from constants import PLATFORM_LABELS, DEAL_TYPE_LABELS
from models import DealContribution, NegotiationSession
from stats_helpers import summarize_cpm_outlier_safe, summarize_fees_outlier_safe


def _safe_avg(values: list[float]) -> float | None:
    if not values:
        return None
    return float(sum(values) / len(values))


def _safe_median(values: list[float]) -> float | None:
    if not values:
        return None
    return float(median(values))


def build_user_analytics(session: Session, user_id: int) -> dict[str, Any]:
    deals = session.exec(
        select(DealContribution).where(DealContribution.user_id == user_id)
    ).all()
    negotiations = session.exec(
        select(NegotiationSession).where(NegotiationSession.user_id == user_id)
    ).all()

    deal_fees = [float(d.total_fee_usd) for d in deals if d.total_fee_usd is not None]
    deal_fees_sorted = sorted(deal_fees)

    quoted_rows = [
        d
        for d in deals
        if d.quoted_fee_usd is not None
        and d.quoted_fee_usd > 0
        and d.total_fee_usd is not None
    ]
    quoted_fees = [float(d.quoted_fee_usd) for d in quoted_rows]
    closed_fees = [float(d.total_fee_usd) for d in quoted_rows]
    close_vs_quote = [
        ((float(d.total_fee_usd) - float(d.quoted_fee_usd)) / float(d.quoted_fee_usd))
        * 100
        for d in quoted_rows
    ]

    platform_buckets: dict[str, list[float]] = {}
    deal_type_buckets: dict[str, list[float]] = {}
    for deal in deals:
        if deal.platform:
            platform_buckets.setdefault(deal.platform, []).append(float(deal.total_fee_usd))
        if deal.deal_type:
            deal_type_buckets.setdefault(deal.deal_type, []).append(float(deal.total_fee_usd))

    platform_breakdown = []
    for code, fees in sorted(platform_buckets.items()):
        fees_sorted = sorted(fees)
        platform_breakdown.append(
            {
                "platform": code,
                "label": PLATFORM_LABELS.get(code, "Other"),
                "count": len(fees_sorted),
                "avg_fee": _safe_avg(fees_sorted),
            }
        )

    deal_type_breakdown = []
    for code, fees in sorted(deal_type_buckets.items()):
        fees_sorted = sorted(fees)
        deal_type_breakdown.append(
            {
                "deal_type": code,
                "label": DEAL_TYPE_LABELS.get(code, "Other"),
                "count": len(fees_sorted),
                "avg_fee": _safe_avg(fees_sorted),
            }
        )

    negotiations_by_id = {n.id: n for n in negotiations if n.id is not None}
    linked_deals = [d for d in deals if d.negotiation_session_id]
    uplifts = []
    for deal in linked_deals:
        negotiation = negotiations_by_id.get(deal.negotiation_session_id)
        if not negotiation:
            continue
        if negotiation.brand_offer is None or negotiation.brand_offer <= 0:
            continue
        if negotiation.final_agreed_fee_usd is None:
            continue
        uplifts.append(
            (
                (float(negotiation.final_agreed_fee_usd) - float(negotiation.brand_offer))
                / float(negotiation.brand_offer)
            )
            * 100
        )

    outcome_counts: dict[str, int] = {}
    for negotiation in negotiations:
        outcome = (negotiation.outcome or "in_progress").strip().lower()
        outcome_counts[outcome] = outcome_counts.get(outcome, 0) + 1

    monthly_buckets: dict[tuple[int, int], list[float]] = {}
    for deal in deals:
        created_at = deal.created_at or datetime.utcnow()
        key = (created_at.year, created_at.month)
        monthly_buckets.setdefault(key, []).append(float(deal.total_fee_usd))

    monthly_trend = []
    for (year, month) in sorted(monthly_buckets.keys()):
        fees = monthly_buckets[(year, month)]
        fees_sorted = sorted(fees)
        total = float(sum(fees_sorted))
        count = len(fees_sorted)
        monthly_trend.append(
            {
                "year": year,
                "month": month,
                "deal_count": count,
                "total_revenue": total if count else None,
                "avg_deal": (total / count) if count else None,
            }
        )

    deals_summary = {
        "deals_count": len(deals),
        "total_revenue": float(sum(deal_fees_sorted)) if deal_fees_sorted else None,
        "avg_deal": _safe_avg(deal_fees_sorted),
        "median_deal": _safe_median(deal_fees_sorted),
        "avg_quoted_fee": _safe_avg(quoted_fees),
        "avg_closed_fee": _safe_avg(closed_fees),
        "avg_close_vs_quote_pct": _safe_avg(close_vs_quote),
        "quoted_count": len(quoted_rows),
    }

    negotiation_summary = {
        "negotiation_count": len(negotiations),
        "avg_uplift_pct": _safe_avg(uplifts),
        "median_uplift_pct": _safe_median(uplifts),
        "outcomes": outcome_counts,
    }

    flags = {
        "has_deals": len(deals) > 0,
        "has_quoted_vs_closed": len(quoted_rows) > 0,
        "has_negotiation_uplift": len(uplifts) > 0,
    }

    return {
        "deals_summary": deals_summary,
        "platform_breakdown": platform_breakdown,
        "deal_type_breakdown": deal_type_breakdown,
        "negotiation_summary": negotiation_summary,
        "monthly_trend": monthly_trend,
        "flags": flags,
    }


def _quarter_date_range(year: int, quarter: int) -> tuple[datetime, datetime]:
    if quarter == 1:
        return datetime(year, 1, 1), datetime(year, 3, 31, 23, 59, 59)
    if quarter == 2:
        return datetime(year, 4, 1), datetime(year, 6, 30, 23, 59, 59)
    if quarter == 3:
        return datetime(year, 7, 1), datetime(year, 9, 30, 23, 59, 59)
    return datetime(year, 10, 1), datetime(year, 12, 31, 23, 59, 59)


def _summarize_deals(rows: list[DealContribution]) -> dict[str, Any]:
    fees = [float(row.total_fee_usd) for row in rows if row.total_fee_usd is not None]
    views = [row.reported_views or 0 for row in rows]

    fee_summary = summarize_fees_outlier_safe(fees)
    cpm_summary = summarize_cpm_outlier_safe(fees, views)

    return {
        "deal_count": len(rows),
        "avg_fee": fee_summary["avg"],
        "median_fee": fee_summary["median"],
        "min_fee": fee_summary["min"],
        "max_fee": fee_summary["max"],
        "avg_cpm": cpm_summary["avg"],
        "median_cpm": cpm_summary["median"],
    }


def build_quarterly_niche_stats(
    session: Session,
    niche_code: str,
    platform_code: str | None,
    year: int,
    quarter: int,
) -> dict[str, Any]:
    if quarter not in {1, 2, 3, 4}:
        raise ValueError("quarter must be between 1 and 4")

    min_deals_for_report = 5
    start_date, end_date = _quarter_date_range(year, quarter)

    statement = select(DealContribution).where(
        DealContribution.share_in_index == True,
        DealContribution.niche == niche_code,
        DealContribution.created_at >= start_date,
        DealContribution.created_at <= end_date,
    )
    if platform_code and platform_code != "all":
        statement = statement.where(DealContribution.platform == platform_code)

    rows = session.exec(statement).all()
    fee_values = [float(row.total_fee_usd) for row in rows if row.total_fee_usd is not None]
    fee_values_sorted = sorted(fee_values)

    enough_data_for_report = len(rows) >= min_deals_for_report

    if enough_data_for_report:
        summary = _summarize_deals(rows)
    else:
        summary = {
            "deal_count": len(rows),
            "avg_fee": None,
            "median_fee": None,
            "min_fee": float(fee_values_sorted[0]) if fee_values_sorted else None,
            "max_fee": float(fee_values_sorted[-1]) if fee_values_sorted else None,
            "avg_cpm": None,
            "median_cpm": None,
        }

    prev_quarter = None
    prev_year = year
    prev_quarter_num = quarter - 1
    if prev_quarter_num == 0:
        prev_quarter_num = 4
        prev_year = year - 1

    prev_start, prev_end = _quarter_date_range(prev_year, prev_quarter_num)
    prev_statement = select(DealContribution).where(
        DealContribution.share_in_index == True,
        DealContribution.niche == niche_code,
        DealContribution.created_at >= prev_start,
        DealContribution.created_at <= prev_end,
    )
    if platform_code and platform_code != "all":
        prev_statement = prev_statement.where(DealContribution.platform == platform_code)

    prev_rows = session.exec(prev_statement).all()
    if prev_rows:
        prev_fee_values = [
            float(row.total_fee_usd) for row in prev_rows if row.total_fee_usd is not None
        ]
        prev_fee_values_sorted = sorted(prev_fee_values)
        prev_enough = len(prev_rows) >= min_deals_for_report
        if prev_enough:
            prev_quarter = _summarize_deals(prev_rows)
        else:
            prev_quarter = {
                "deal_count": len(prev_rows),
                "avg_fee": None,
                "median_fee": None,
                "min_fee": float(prev_fee_values_sorted[0])
                if prev_fee_values_sorted
                else None,
                "max_fee": float(prev_fee_values_sorted[-1])
                if prev_fee_values_sorted
                else None,
                "avg_cpm": None,
                "median_cpm": None,
            }
        prev_quarter.update({"year": prev_year, "quarter": prev_quarter_num})

    return {
        "niche": niche_code,
        "platform": platform_code or "all",
        "year": year,
        "quarter": quarter,
        **summary,
        "enough_data_for_report": enough_data_for_report,
        "min_deals_for_report": min_deals_for_report,
        "prev_quarter": prev_quarter,
    }
