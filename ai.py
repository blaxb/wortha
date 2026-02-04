from __future__ import annotations

import json
import logging
from statistics import median
from datetime import datetime
import os
from typing import Any

from openai import OpenAI
from sqlmodel import Session, select

from models import (
    Calculation,
    CreatorProfile,
    DealContribution,
    NegotiationSession,
    AiUsage,
)

logger = logging.getLogger(__name__)

client = OpenAI()


def calculator_ai_enabled() -> bool:
    return os.environ.get("CALCULATOR_AI_ENABLED", "true").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def reserve_calculator_ai_call(
    session: Session, feature: str = "calculator", daily_cap: int | None = None
) -> bool:
    cap = daily_cap
    if cap is None:
        try:
            cap = int(os.environ.get("CALCULATOR_AI_DAILY_CAP", "500"))
        except ValueError:
            cap = 500
    if cap <= 0:
        return False

    usage_date = datetime.utcnow().date().isoformat()
    statement = select(AiUsage).where(
        AiUsage.usage_date == usage_date, AiUsage.feature == feature
    )
    usage_row = session.exec(statement).first()
    if not usage_row:
        usage_row = AiUsage(usage_date=usage_date, feature=feature, call_count=0)
        session.add(usage_row)
        session.commit()
        session.refresh(usage_row)

    if usage_row.call_count >= cap:
        return False

    usage_row.call_count += 1
    session.add(usage_row)
    session.commit()
    return True


def _safe_median(values: list[float]) -> float | None:
    if not values:
        return None
    return float(median(values))


def _safe_avg(values: list[float]) -> float | None:
    if not values:
        return None
    return float(sum(values) / len(values))


def _fees_by_key(rows: list[DealContribution], key: str) -> dict[str, dict[str, Any]]:
    buckets: dict[str, list[float]] = {}
    for row in rows:
        value = getattr(row, key)
        if not value:
            continue
        buckets.setdefault(value, []).append(float(row.total_fee_usd or 0))

    summary: dict[str, dict[str, Any]] = {}
    for bucket, fees in buckets.items():
        fees_sorted = sorted(fees)
        summary[bucket] = {
            "count": len(fees_sorted),
            "avg_fee": _safe_avg(fees_sorted),
            "median_fee": _safe_median(fees_sorted),
        }
    return summary


def build_creator_stats(session: Session, user_id: int) -> dict[str, Any]:
    profile = session.exec(
        select(CreatorProfile).where(CreatorProfile.user_id == user_id)
    ).first()

    deals = session.exec(
        select(DealContribution).where(DealContribution.user_id == user_id)
    ).all()

    negotiations = session.exec(
        select(NegotiationSession).where(NegotiationSession.user_id == user_id)
    ).all()

    calculations = session.exec(
        select(Calculation).where(Calculation.user_id == user_id)
    ).all()

    deal_fees = [float(d.total_fee_usd) for d in deals if d.total_fee_usd is not None]
    deal_fees_sorted = sorted(deal_fees)
    avg_deal = _safe_avg(deal_fees_sorted)
    median_deal = _safe_median(deal_fees_sorted)

    cpms = []
    for row in deals:
        if row.reported_views and row.reported_views > 0 and row.total_fee_usd:
            cpms.append((row.total_fee_usd / row.reported_views) * 1000)

    negotiation_offer_diffs = [
        n.offer_vs_market_pct for n in negotiations if n.offer_vs_market_pct is not None
    ]
    avg_offer_diff = _safe_avg([float(x) for x in negotiation_offer_diffs])

    counter_above_offer = 0
    for n in negotiations:
        if n.recommended_counter_min is None or n.brand_offer is None:
            continue
        if n.recommended_counter_min > n.brand_offer:
            counter_above_offer += 1

    calc_ranges: dict[tuple[str, str], list[tuple[float, float]]] = {}
    for calc in calculations:
        key = (calc.platform, calc.niche)
        calc_ranges.setdefault(key, []).append(
            (float(calc.recommended_min), float(calc.recommended_max))
        )

    main_calc_key = None
    if calc_ranges:
        main_calc_key = max(calc_ranges.items(), key=lambda item: len(item[1]))[0]

    typical_range = None
    if main_calc_key:
        mins = [pair[0] for pair in calc_ranges[main_calc_key]]
        maxes = [pair[1] for pair in calc_ranges[main_calc_key]]
        typical_range = {
            "platform": main_calc_key[0],
            "niche": main_calc_key[1],
            "avg_min": _safe_avg(mins),
            "avg_max": _safe_avg(maxes),
        }

    deal_summary = {
        "total_deals": len(deals),
        "count": len(deals),
        "avg_fee": avg_deal,
        "median_fee": median_deal,
        "avg_cpm": _safe_avg(cpms),
        "platform_breakdown": _fees_by_key(deals, "platform"),
        "niche_breakdown": _fees_by_key(deals, "niche"),
    }

    negotiation_summary = {
        "total_negotiations": len(negotiations),
        "count": len(negotiations),
        "avg_offer_vs_market_pct": avg_offer_diff,
        "counter_above_offer_count": counter_above_offer,
    }

    calculator_summary = {
        "total_calculations": len(calculations),
        "count": len(calculations),
        "typical_range": typical_range,
    }

    profile_summary = None
    if profile:
        profile_summary = {
            "display_name": profile.display_name,
            "primary_platform": profile.primary_platform,
            "niche": profile.niche,
            "followers": profile.followers,
            "avg_views": profile.avg_views,
            "engagement_rate": profile.engagement_rate,
        }

    total_signals = len(deals) + len(negotiations) + len(calculations)
    data_richness = {
        "has_any_deals": len(deals) > 0,
        "has_any_negotiations": len(negotiations) > 0,
        "has_any_calculations": len(calculations) > 0,
        "total_signals": total_signals,
    }

    return {
        "profile": profile_summary,
        "deal_summary": deal_summary,
        "negotiation_summary": negotiation_summary,
        "calculator_summary": calculator_summary,
        "data_richness": data_richness,
    }


def generate_creator_insights(
    stats: dict[str, Any], preview: bool = False
) -> dict[str, Any]:
    total_signals = (stats.get("data_richness") or {}).get("total_signals", 0)
    if total_signals < 3:
        return {
            "status": "no_data",
            "message": (
                "We need at least a few logged deals or negotiation sessions "
                "(or several pricing calculations) before we can analyze your data."
            ),
            "insights_text": None,
        }

    try:
        instructions = (
            "You are a pricing and negotiation coach for content creators. You will be "
            "given aggregated stats about one creator's sponsorship deals, pricing "
            "recommendations, and negotiation outcomes. You must return:\n"
            "1) 3-5 bullet-point insights about how they are currently pricing and "
            "performing (be specific and numeric where possible).\n"
            "2) 3 concrete action recommendations for the next 30 days, tailored to "
            "their main platform and niche.\n"
            "Avoid generic advice, and only reference patterns that are actually "
            "supported by the stats. If the data is thin, acknowledge that explicitly."
        )

        input_payload = {
            "profile": stats.get("profile"),
            "deal_summary": stats.get("deal_summary"),
            "negotiation_summary": stats.get("negotiation_summary"),
            "calculator_summary": stats.get("calculator_summary"),
            "data_richness": stats.get("data_richness"),
            "preview": preview,
        }

        response = client.responses.create(
            model="gpt-5.2",
            instructions=instructions,
            input=json.dumps(input_payload, ensure_ascii=False),
        )
        return {
            "status": "ok",
            "message": "",
            "insights_text": response.output_text.strip(),
        }
    except Exception:
        logger.exception("OpenAI insights generation failed")
        return {
            "status": "error",
            "message": (
                "We couldn’t reach the AI insights service right now. Please try again "
                "in a few minutes."
            ),
            "insights_text": None,
        }


def generate_niche_report(stats: dict[str, Any]) -> str:
    min_deals = stats.get("min_deals_for_report", 5)
    deal_count = stats.get("deal_count", 0)
    if deal_count < min_deals or not stats.get("enough_data_for_report", False):
        return (
            "We don't yet have enough community deals in this niche and quarter to "
            "generate a meaningful report. Try a different quarter or niche, or "
            "encourage more creators to contribute deals."
        )

    try:
        instructions = (
            "You are an analyst for a tool that helps content creators price brand deals. "
            "Given quarterly aggregated deal stats for a specific niche and platform, write "
            "a concise report with: (1) a summary of typical deal sizes and CPMs, "
            "(2) how this quarter compares to the previous quarter if data is available, "
            "and (3) 3–5 practical recommendations for creators in this niche. Avoid generic "
            "advice; base everything on the numbers."
        )
        response = client.responses.create(
            model="gpt-5.2",
            instructions=instructions,
            input=json.dumps(stats, ensure_ascii=False),
        )
        return response.output_text.strip()
    except Exception:
        logger.exception("OpenAI niche report generation failed")
        return (
            "We couldn’t generate a full AI report right now. Here are the raw numbers instead."
        )


def generate_pricing_explanation(
    platform: str,
    niche: str,
    deal_type: str,
    follower_count: int,
    avg_views: int,
    engagement_rate: float,
    geo: str,
    base_cpm: float,
    niche_multiplier: float,
    engagement_multiplier: float,
    geo_multiplier: float,
    effective_cpm: float,
    recommended_price: float,
    low_price: float,
    high_price: float,
) -> str:
    instructions = (
        "You are a pricing assistant for creators. Write a single, concise paragraph "
        "explaining the recommended rate. Requirements:\n"
        "- 1 paragraph, 1-3 sentences, no lists.\n"
        "- Include platform, niche, and deal type explicitly.\n"
        "- Include the recommended price and range, with the recommended price wrapped "
        "in <strong> tags.\n"
        "- Mention base CPM and the effective CPM after multipliers.\n"
        "- If follower count or avg views are unusually low or high, briefly mention it.\n"
        "- No extra formatting beyond <strong> tags."
    )

    payload = {
        "platform": platform,
        "niche": niche,
        "deal_type": deal_type,
        "follower_count": follower_count,
        "avg_views": avg_views,
        "engagement_rate_pct": engagement_rate,
        "geo": geo,
        "base_cpm": base_cpm,
        "niche_multiplier": niche_multiplier,
        "engagement_multiplier": engagement_multiplier,
        "geo_multiplier": geo_multiplier,
        "effective_cpm": effective_cpm,
        "recommended_price": recommended_price,
        "low_price": low_price,
        "high_price": high_price,
    }

    response = client.responses.create(
        model="gpt-4o-mini",
        instructions=instructions,
        input=json.dumps(payload, ensure_ascii=False),
        max_output_tokens=140,
        temperature=0.2,
    )
    return response.output_text.strip()
