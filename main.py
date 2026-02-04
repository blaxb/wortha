from __future__ import annotations

from datetime import datetime, timedelta
import io
import logging
from typing import Optional

import os

from fastapi import Depends, FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlmodel import Session, select
from starlette.middleware.sessions import SessionMiddleware

from auth import authenticate_user, get_current_user, hash_password, login_user, logout_user
from security import require_plan_or_redirect
from constants import (
    CONTENT_FORMATS,
    CONTENT_FORMAT_LABELS,
    DEAL_TYPES,
    DEAL_TYPE_LABELS,
    FOLLOWER_TIERS,
    FOLLOWER_TIER_LABELS,
    GEO_REGIONS,
    GEO_REGION_LABELS,
    NICHES,
    NICHE_LABELS,
    PLATFORMS,
    PLATFORM_LABELS,
    content_format_label,
    deal_type_label,
    follower_tier_label,
    geo_region_label,
    niche_label,
    normalize_content_format,
    normalize_deal_type,
    normalize_follower_tier,
    normalize_geo_region,
    normalize_niche,
    normalize_platform,
    platform_label,
)
from models import (
    Calculation,
    CreatorProfile,
    DealContribution,
    MediaKitPackage,
    NegotiationSession,
    User,
    create_db_and_tables,
    create_deal_contribution_from_form,
    bucket_follower_count,
    get_or_create_creator_profile,
    get_or_initialize_default_packages,
    get_recent_negotiations,
    get_session,
    link_negotiation_to_deal,
)
from analytics_helpers import build_quarterly_niche_stats, build_user_analytics
from ai import build_creator_stats, generate_creator_insights, generate_niche_report
from stats_helpers import (
    get_bucket_community_pricing,
    summarize_cpm_outlier_safe,
    summarize_fees_outlier_safe,
)
from xhtml2pdf import pisa

logger = logging.getLogger(__name__)

app = FastAPI()

# Note: If file upload endpoints are added in the future, enforce size limits,
# content-type validation, and safe storage outside public web roots.

session_secret = os.environ.get("SESSION_SECRET_KEY")
if not session_secret:
    # SESSION_SECRET_KEY is expected in the environment for production deployments.
    session_secret = "dev-insecure-session-key"

app.add_middleware(
    SessionMiddleware,
    secret_key=session_secret,
    same_site="lax",
)

app.mount("/static", StaticFiles(directory="static"), name="static")

templates = Jinja2Templates(directory="templates")
templates.env.globals.update(
    {
        "PLATFORMS": PLATFORMS,
        "NICHES": NICHES,
        "GEO_REGIONS": GEO_REGIONS,
        "FOLLOWER_TIERS": FOLLOWER_TIERS,
        "DEAL_TYPES": DEAL_TYPES,
        "CONTENT_FORMATS": CONTENT_FORMATS,
        "PLATFORM_LABELS": PLATFORM_LABELS,
        "NICHE_LABELS": NICHE_LABELS,
        "GEO_REGION_LABELS": GEO_REGION_LABELS,
        "FOLLOWER_TIER_LABELS": FOLLOWER_TIER_LABELS,
        "DEAL_TYPE_LABELS": DEAL_TYPE_LABELS,
        "CONTENT_FORMAT_LABELS": CONTENT_FORMAT_LABELS,
        "platform_label": platform_label,
        "niche_label": niche_label,
        "geo_region_label": geo_region_label,
        "follower_tier_label": follower_tier_label,
        "deal_type_label": deal_type_label,
        "content_format_label": content_format_label,
    }
)


def calculate_rate(
    platform: str,
    niche: str,
    followers: Optional[int],
    avg_views: Optional[int],
    engagement_rate: Optional[float],
    geo_region: str,
):
    platform_cpm = {
        "youtube": 15,
        "instagram": 12,
        "tiktok": 10,
        "linkedin": 18,
        "twitter": 11,
        "twitch": 14,
        "podcast": 20,
        "newsletter": 16,
        "other": 8,
    }
    niche_multipliers = {
        "finance": 1.4,
        "investing": 1.4,
        "business": 1.4,
        "beauty": 1.2,
        "fashion": 1.2,
        "tech": 1.3,
        "gaming": 1.3,
        "fitness": 1.15,
        "health": 1.15,
    }

    platform_key = normalize_platform(platform)
    niche_key = normalize_niche(niche)

    base_cpm = platform_cpm.get(platform_key, platform_cpm["other"])
    niche_multiplier = 1.0
    for key, value in niche_multipliers.items():
        if key in niche_key:
            niche_multiplier = value
            break

    engagement_multiplier = 1.0
    if engagement_rate is not None:
        if engagement_rate < 1:
            engagement_multiplier = 0.8
        elif engagement_rate < 3:
            engagement_multiplier = 1.0
        elif engagement_rate < 5:
            engagement_multiplier = 1.15
        else:
            engagement_multiplier = 1.3

    geo_key = normalize_geo_region(geo_region)
    if geo_key in {"us", "canada"}:
        geo_multiplier = 1.1
    elif geo_key in {"uk", "eu"}:
        geo_multiplier = 1.05
    else:
        geo_multiplier = 1.0

    views = avg_views if avg_views and avg_views > 0 else 0
    if views == 0 and followers and followers > 0:
        views = int(followers * 0.1)

    effective_cpm = base_cpm * niche_multiplier * engagement_multiplier * geo_multiplier
    base_rate = (views / 1000) * effective_cpm if views else 0
    recommended_min = base_rate * 0.8
    recommended_max = base_rate * 1.2

    return {
        "recommended_min": recommended_min,
        "recommended_max": recommended_max,
        "base_cpm": base_cpm,
        "niche_multiplier": niche_multiplier,
        "engagement_multiplier": engagement_multiplier,
        "geo_multiplier": geo_multiplier,
        "effective_cpm": effective_cpm,
        "views": views,
    }


@app.on_event("startup")
def on_startup() -> None:
    create_db_and_tables()


@app.get("/", response_class=HTMLResponse)
def index(request: Request, session: Session = Depends(get_session)):
    user_id = request.session.get("user_id")
    user = session.get(User, user_id) if user_id else None
    return templates.TemplateResponse("index.html", {"request": request, "user": user})


@app.get("/signup", response_class=HTMLResponse)
def signup_form(request: Request, session: Session = Depends(get_session)):
    user_id = request.session.get("user_id")
    user = session.get(User, user_id) if user_id else None
    return templates.TemplateResponse("signup.html", {"request": request, "user": user, "error": None})


@app.post("/signup")
def signup(
    request: Request,
    email: str = Form(...),
    username: str = Form(...),
    password: str = Form(...),
    confirm_password: str = Form(...),
    session: Session = Depends(get_session),
):
    if password != confirm_password:
        return templates.TemplateResponse(
            "signup.html",
            {"request": request, "user": None, "error": "Passwords do not match."},
            status_code=400,
        )

    existing = session.exec(
        select(User).where((User.email == email) | (User.username == username))
    ).first()
    if existing:
        return templates.TemplateResponse(
            "signup.html",
            {"request": request, "user": None, "error": "Email or username already exists."},
            status_code=400,
        )

    user = User(email=email, username=username, hashed_password=hash_password(password))
    session.add(user)
    session.commit()
    session.refresh(user)

    login_user(request, user)
    return RedirectResponse(url="/dashboard", status_code=303)


@app.get("/login", response_class=HTMLResponse)
def login_form(request: Request, session: Session = Depends(get_session)):
    user_id = request.session.get("user_id")
    user = session.get(User, user_id) if user_id else None
    return templates.TemplateResponse("login.html", {"request": request, "user": user, "error": None})


@app.post("/login")
def login(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
    session: Session = Depends(get_session),
):
    user = authenticate_user(session, email, password)
    if not user:
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "user": None, "error": "Invalid email or password."},
            status_code=400,
        )

    login_user(request, user)
    return RedirectResponse(url="/dashboard", status_code=303)


@app.post("/logout")
def logout(request: Request):
    logout_user(request)
    return RedirectResponse(url="/", status_code=303)


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(
    request: Request,
    user: User = Depends(get_current_user),
):
    return templates.TemplateResponse(
        "dashboard.html", {"request": request, "user": user}
    )


@app.get("/insights", response_class=HTMLResponse)
def insights(
    request: Request,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
):
    stats = build_creator_stats(session, user.id)
    is_free = (user.plan or "free").lower() == "free"
    insights_result = generate_creator_insights(stats, preview=is_free)

    deal_summary = stats.get("deal_summary") or {}
    platform_breakdown = deal_summary.get("platform_breakdown") or {}
    most_common_platform = None
    if platform_breakdown:
        most_common_platform = max(
            platform_breakdown.items(), key=lambda item: item[1].get("count", 0)
        )[0]

    overview = {
        "deals_logged": deal_summary.get("total_deals"),
        "avg_deal": deal_summary.get("avg_fee"),
        "median_deal": deal_summary.get("median_fee"),
        "avg_cpm": deal_summary.get("avg_cpm"),
        "most_common_platform": most_common_platform,
    }

    return templates.TemplateResponse(
        "insights.html",
        {
            "request": request,
            "user": user,
            "stats": stats,
            "overview": overview,
            "insights_result": insights_result,
        },
    )


@app.get("/calculator", response_class=HTMLResponse)
def calculator(
    request: Request,
    user: User = Depends(get_current_user),
):
    is_pro_or_premium = (user.plan or "free").lower() in {"pro", "premium"}
    return templates.TemplateResponse(
        "calculator.html",
        {
            "request": request,
            "user": user,
            "is_pro_or_premium": is_pro_or_premium,
            "result": None,
            "limit_reached": False,
            "message": None,
            "platform": None,
            "niche": None,
            "niche_other": None,
            "deal_type": None,
            "deal_type_other": None,
            "followers": None,
            "avg_views": None,
            "engagement_rate": None,
            "geo_region": "us",
            "community_note": None,
        },
    )


@app.get("/upgrade", response_class=HTMLResponse)
def upgrade(
    request: Request,
    user: User = Depends(get_current_user),
):
    return templates.TemplateResponse(
        "upgrade.html",
        {
            "request": request,
            "user": user,
            "reason": request.query_params.get("reason"),
        },
    )


@app.get("/media-kit", response_class=HTMLResponse)
def media_kit_form(
    request: Request,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
):
    plan_redirect = require_plan_or_redirect(user, ["pro", "premium"], "media-kit")
    if plan_redirect:
        return plan_redirect
    profile = get_or_create_creator_profile(session, user.id)
    updated = False
    normalized_platform = normalize_platform(profile.primary_platform)
    if normalized_platform != profile.primary_platform:
        profile.primary_platform = normalized_platform
        updated = True
    if profile.niche:
        normalized_niche = normalize_niche(profile.niche)
        if normalized_niche != profile.niche:
            profile.niche = normalized_niche
            updated = True
    if profile.audience_location:
        normalized_geo = normalize_geo_region(profile.audience_location)
        if normalized_geo != profile.audience_location:
            profile.audience_location = normalized_geo
            updated = True
    if updated:
        session.add(profile)
        session.commit()
        session.refresh(profile)
    packages = get_or_initialize_default_packages(session, user.id)
    return templates.TemplateResponse(
        "media_kit_form.html",
        {
            "request": request,
            "user": user,
            "profile": profile,
            "packages": packages,
            "saved": request.query_params.get("saved") == "1",
        },
    )


@app.post("/media-kit")
async def media_kit_submit(
    request: Request,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
):
    plan_redirect = require_plan_or_redirect(user, ["pro", "premium"], "media-kit")
    if plan_redirect:
        return plan_redirect

    def to_int(value: str | None) -> int | None:
        if value is None or value.strip() == "":
            return None
        try:
            return int(value)
        except ValueError:
            return None

    def to_float(value: str | None) -> float | None:
        if value is None or value.strip() == "":
            return None
        try:
            return float(value)
        except ValueError:
            return None

    form = await request.form()
    profile = get_or_create_creator_profile(session, user.id)

    profile.display_name = (form.get("display_name") or "").strip()
    profile.tagline = (form.get("tagline") or "").strip() or None
    primary_platform = normalize_platform(form.get("primary_platform"))
    primary_platform_other = (form.get("primary_platform_other") or "").strip() or None
    profile.primary_platform = primary_platform
    profile.primary_platform_other = (
        primary_platform_other if primary_platform == "other" else None
    )
    profile.followers = to_int(form.get("followers"))
    profile.avg_views = to_int(form.get("avg_views"))
    profile.engagement_rate = to_float(form.get("engagement_rate"))
    raw_niche = (form.get("niche") or "").strip()
    niche_other = (form.get("niche_other") or "").strip() or None
    if raw_niche:
        niche_code = normalize_niche(raw_niche)
        profile.niche = niche_code
        profile.niche_other = niche_other if niche_code == "other" else None
    else:
        profile.niche = None
        profile.niche_other = None

    raw_audience_location = (form.get("audience_location") or "").strip()
    audience_location_notes = (form.get("audience_location_notes") or "").strip() or None
    if raw_audience_location:
        profile.audience_location = normalize_geo_region(raw_audience_location)
        profile.audience_location_notes = audience_location_notes
    else:
        profile.audience_location = None
        profile.audience_location_notes = audience_location_notes
    profile.audience_age_range = (form.get("audience_age_range") or "").strip() or None
    profile.audience_gender_split = (form.get("audience_gender_split") or "").strip() or None
    profile.bio = (form.get("bio") or "").strip() or None
    profile.website_url = (form.get("website_url") or "").strip() or None
    profile.contact_email = (form.get("contact_email") or "").strip() or None

    packages = get_or_initialize_default_packages(session, user.id)
    packages_by_id = {package.id: package for package in packages if package.id is not None}
    default_names = ["Basic", "Standard", "Premium"]

    for index in range(3):
        package_id = form.get(f"packages-{index}-id")
        name = (form.get(f"packages-{index}-name") or default_names[index]).strip()
        headline = (form.get(f"packages-{index}-headline") or "").strip() or None
        price = to_float(form.get(f"packages-{index}-price"))
        deliverables = (form.get(f"packages-{index}-deliverables") or "").strip() or None
        notes = (form.get(f"packages-{index}-notes") or "").strip() or None

        package = None
        if package_id:
            try:
                package_id_int = int(package_id)
            except ValueError:
                package_id_int = None
            if package_id_int is not None:
                package = packages_by_id.get(package_id_int)

        if package is None:
            package = MediaKitPackage(user_id=user.id, sort_order=index)
            session.add(package)

        package.name = name or default_names[index]
        package.headline = headline
        package.price = price
        package.deliverables = deliverables
        package.notes = notes
        package.sort_order = index

    session.add(profile)
    session.commit()
    return RedirectResponse(url="/media-kit?saved=1", status_code=303)


@app.get("/media-kit/pdf")
def media_kit_pdf(
    request: Request,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
):
    plan_redirect = require_plan_or_redirect(user, ["pro", "premium"], "media-kit")
    if plan_redirect:
        return plan_redirect

    profile = get_or_create_creator_profile(session, user.id)
    packages = get_or_initialize_default_packages(session, user.id)

    html_content = templates.get_template("media_kit_pdf.html").render(
        {
            "request": request,
            "user": user,
            "profile": profile,
            "packages": packages,
        }
    )

    pdf_buffer = io.BytesIO()
    result = pisa.CreatePDF(io.StringIO(html_content), dest=pdf_buffer)

    if result.err:
        logger.exception("Media kit PDF generation failed.")
        return HTMLResponse(
            "Sorry, we couldn’t generate your PDF right now. Please try again later.",
            status_code=500,
        )

    pdf_buffer.seek(0)
    headers = {"Content-Disposition": 'attachment; filename="wortha-media-kit.pdf"'}
    return StreamingResponse(pdf_buffer, media_type="application/pdf", headers=headers)


@app.get("/negotiation", response_class=HTMLResponse)
def negotiation_form(
    request: Request,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
):
    plan_redirect = require_plan_or_redirect(user, ["premium"], "negotiation")
    if plan_redirect:
        return plan_redirect

    profile = session.exec(
        select(CreatorProfile).where(CreatorProfile.user_id == user.id)
    ).first()
    recent_negotiations = get_recent_negotiations(session, user.id, limit=5)

    form_data = {}
    if profile:
        form_data = {
            "platform": normalize_platform(profile.primary_platform),
            "niche": normalize_niche(profile.niche) if profile.niche else "",
            "followers": profile.followers or "",
            "avg_views": profile.avg_views or "",
            "engagement_rate": profile.engagement_rate or "",
            "geo_region": normalize_geo_region(profile.audience_location or "us"),
        }

    return templates.TemplateResponse(
        "negotiation_form.html",
        {
            "request": request,
            "user": user,
            "profile": profile,
            "recent_negotiations": recent_negotiations,
            "result": None,
            "form_data": form_data,
        },
    )


@app.post("/negotiation", response_class=HTMLResponse)
async def negotiation_submit(
    request: Request,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
):
    plan_redirect = require_plan_or_redirect(user, ["premium"], "negotiation")
    if plan_redirect:
        return plan_redirect

    def to_int(value: str | None) -> int | None:
        if value is None or value.strip() == "":
            return None
        try:
            return int(value)
        except ValueError:
            return None

    def to_float(value: str | None) -> float | None:
        if value is None or value.strip() == "":
            return None
        try:
            return float(value)
        except ValueError:
            return None

    form = await request.form()
    profile = session.exec(
        select(CreatorProfile).where(CreatorProfile.user_id == user.id)
    ).first()

    brand_name = (form.get("brand_name") or "").strip()
    platform = normalize_platform(form.get("platform"))
    niche = normalize_niche(form.get("niche"))

    raw_deal_type = (form.get("deal_type") or "").strip()
    deal_type = normalize_deal_type(raw_deal_type) if raw_deal_type else None
    deal_type_other = (form.get("deal_type_other") or "").strip() or None

    raw_content_format = (form.get("content_format") or "").strip()
    content_format = (
        normalize_content_format(raw_content_format) if raw_content_format else None
    )
    content_format_other = (form.get("content_format_other") or "").strip() or None
    geo_region = normalize_geo_region(form.get("geo_region") or "us")

    followers_value = to_int(form.get("followers"))
    if followers_value is None and profile:
        followers_value = profile.followers
    followers_value = followers_value or 0

    avg_views_value = to_int(form.get("avg_views"))
    if avg_views_value is None and profile:
        avg_views_value = profile.avg_views
    if avg_views_value is None and followers_value:
        avg_views_value = int(followers_value * 0.1)

    engagement_value = to_float(form.get("engagement_rate"))
    if engagement_value is None and profile:
        engagement_value = profile.engagement_rate
    if engagement_value is None:
        engagement_value = 3.0

    brand_offer_value = to_float(form.get("brand_offer")) or 0.0

    result = calculate_rate(
        platform=platform,
        niche=niche,
        followers=followers_value,
        avg_views=avg_views_value,
        engagement_rate=engagement_value,
        geo_region=geo_region,
    )

    market_min = result["recommended_min"]
    market_max = result["recommended_max"]
    market_mid = (market_min + market_max) / 2 if (market_min + market_max) > 0 else 0

    offer_vs_market_pct = None
    if market_mid > 0:
        offer_vs_market_pct = ((brand_offer_value - market_mid) / market_mid) * 100

    if offer_vs_market_pct is None:
        assessment_text = "We couldn't estimate a fair market range from the provided metrics."
    elif offer_vs_market_pct <= -30:
        assessment_text = (
            "This offer is more than 30% below our estimate of fair market value "
            "for your metrics."
        )
    elif offer_vs_market_pct <= -10:
        assessment_text = (
            "This offer is below market value; there’s room to negotiate up."
        )
    elif offer_vs_market_pct <= 10:
        assessment_text = (
            "This offer is roughly in line with market value for your niche and audience."
        )
    else:
        assessment_text = (
            "This offer is above typical market value for your metrics."
        )

    # Simple counter strategy: anchor at mid-market and give a modest upper bound.
    if market_mid > 0:
        if offer_vs_market_pct is not None and offer_vs_market_pct <= -10:
            recommended_counter_min = market_mid
            recommended_counter_max = market_max * 1.1
        else:
            recommended_counter_min = market_mid * 0.9
            recommended_counter_max = market_max * 1.05
    else:
        recommended_counter_min = None
        recommended_counter_max = None

    counter_mid = None
    if recommended_counter_min is not None and recommended_counter_max is not None:
        counter_mid = (recommended_counter_min + recommended_counter_max) / 2

    email_subject = f"Re: Partnership with {brand_name}"
    counter_line = (
        f"A fair range for this collaboration would be ${recommended_counter_min:,.2f}–"
        f"${recommended_counter_max:,.2f}, with a counter proposal of ${counter_mid:,.2f}."
        if counter_mid is not None
        else "I’d love to discuss a fair rate based on the scope and my audience metrics."
    )
    email_body = (
        f"Hi {brand_name} team,\n\n"
        "Thanks so much for the offer and for considering a partnership. "
        f"Based on my audience metrics ({followers_value:,} followers, "
        f"{avg_views_value or 0:,} average views) and my niche in {niche_label(niche) or 'this space'}, "
        "typical market rates suggest a higher range.\n\n"
        f"{counter_line}\n\n"
        "Happy to discuss options and find a structure that works for both sides.\n\n"
        "Best,\n"
        f"{profile.display_name if profile else 'Creator'}"
    )

    negotiation = NegotiationSession(
        user_id=user.id,
        brand_name=brand_name or None,
        platform=platform or None,
        niche=niche or None,
        deal_type=deal_type or None,
        deal_type_other=deal_type_other if deal_type == "other" else None,
        content_format=content_format or None,
        content_format_other=content_format_other if content_format == "other" else None,
        followers=followers_value or None,
        avg_views=avg_views_value or None,
        engagement_rate=engagement_value,
        geo_region=geo_region or None,
        brand_offer=brand_offer_value,
        market_min=market_min,
        market_max=market_max,
        offer_vs_market_pct=offer_vs_market_pct,
        recommended_counter_min=recommended_counter_min,
        recommended_counter_max=recommended_counter_max,
        assessment_text=assessment_text,
        email_subject=email_subject,
        email_body=email_body,
        status="draft",
    )
    session.add(negotiation)
    session.commit()
    session.refresh(negotiation)

    recent_negotiations = get_recent_negotiations(session, user.id, limit=5)
    form_data = dict(form)
    form_data["platform"] = platform
    form_data["niche"] = niche
    form_data["deal_type"] = deal_type or ""
    form_data["geo_region"] = geo_region
    form_data["content_format"] = content_format or ""

    return templates.TemplateResponse(
        "negotiation_form.html",
        {
            "request": request,
            "user": user,
            "profile": profile,
            "recent_negotiations": recent_negotiations,
            "result": negotiation,
            "form_data": form_data,
            "market_range": (market_min, market_max),
            "counter_range": (recommended_counter_min, recommended_counter_max),
            "assessment_text": assessment_text,
            "email_subject": email_subject,
            "email_body": email_body,
        },
    )


@app.post("/calculator", response_class=HTMLResponse)
def calculator_submit(
    request: Request,
    platform: str = Form(...),
    niche: str = Form(...),
    deal_type: str = Form(...),
    niche_other: Optional[str] = Form(None),
    deal_type_other: Optional[str] = Form(None),
    followers: Optional[str] = Form(None),
    avg_views: Optional[str] = Form(None),
    engagement_rate: Optional[str] = Form(None),
    geo_region: Optional[str] = Form("us"),
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
):
    def to_int(value: Optional[str]) -> Optional[int]:
        if value is None or value.strip() == "":
            return None
        try:
            return int(value)
        except ValueError:
            return None

    def to_float(value: Optional[str]) -> Optional[float]:
        if value is None or value.strip() == "":
            return None
        try:
            return float(value)
        except ValueError:
            return None

    followers_value = to_int(followers)
    avg_views_value = to_int(avg_views)
    engagement_value = to_float(engagement_rate)

    is_pro_or_premium = (user.plan or "free").lower() in {"pro", "premium"}

    now = datetime.utcnow()
    month_start = datetime(now.year, now.month, 1)
    if now.month == 12:
        next_month = datetime(now.year + 1, 1, 1)
    else:
        next_month = datetime(now.year, now.month + 1, 1)

    if not is_pro_or_premium:
        statement = select(Calculation).where(
            Calculation.user_id == user.id,
            Calculation.created_at >= month_start,
            Calculation.created_at < next_month,
        )
        month_count = len(session.exec(statement).all())

        if month_count >= 3:
            return templates.TemplateResponse(
                "calculator.html",
                {
                    "request": request,
                    "user": user,
                    "is_pro_or_premium": is_pro_or_premium,
                    "result": None,
                    "limit_reached": True,
                    "message": "You’ve reached your 3 free calculations for this month. Upgrade to Pro to unlock unlimited pricing calculations.",
                },
            )

    platform_code = normalize_platform(platform)
    niche_code = normalize_niche(niche)
    deal_type_code = normalize_deal_type(deal_type)
    geo_region_code = normalize_geo_region(geo_region or "us")

    niche_other_value = (niche_other or "").strip() or None if niche_code == "other" else None
    deal_type_other_value = (deal_type_other or "").strip() or None if deal_type_code == "other" else None

    result = calculate_rate(
        platform=platform_code,
        niche=niche_code,
        followers=followers_value,
        avg_views=avg_views_value,
        engagement_rate=engagement_value,
        geo_region=geo_region_code,
    )

    community_note = None
    follower_tier = None
    if followers_value is not None:
        follower_tier = normalize_follower_tier(bucket_follower_count(followers_value))

    if follower_tier:
        community_pricing = get_bucket_community_pricing(
            session=session,
            platform=platform_code,
            niche=niche_code,
            follower_tier=follower_tier,
            geo_region=geo_region_code,
            min_deals=5,
        )
    else:
        community_pricing = None

    baseline_min = float(result["recommended_min"])
    baseline_max = float(result["recommended_max"])
    baseline_mid = (baseline_min + baseline_max) / 2 if (baseline_min + baseline_max) > 0 else 0

    if (
        community_pricing
        and community_pricing.get("deal_count", 0) >= 5
        and community_pricing.get("median_fee") is not None
        and baseline_mid > 0
    ):
        community_mid = float(community_pricing["median_fee"])
        ratio = community_mid / baseline_mid if baseline_mid > 0 else 0
        if 0.5 <= ratio <= 2.0:
            final_mid = (0.6 * baseline_mid) + (0.4 * community_mid)
            result["recommended_min"] = final_mid * 0.8
            result["recommended_max"] = final_mid * 1.2
            community_note = (
                f"Community data: Based on {community_pricing['deal_count']} deals in your niche/tier, "
                f"typical closed deals are around ${community_mid:,.0f}. We’ve adjusted your recommendation slightly toward this."
            )

    calculation = Calculation(
        user_id=user.id,
        platform=platform_code,
        niche=niche_code,
        niche_other=niche_other_value,
        deal_type=deal_type_code,
        deal_type_other=deal_type_other_value,
        geo_region=geo_region_code,
        followers=followers_value,
        avg_views=avg_views_value,
        engagement_rate=engagement_value,
        recommended_min=result["recommended_min"],
        recommended_max=result["recommended_max"],
        cpmm_base=result["base_cpm"],
        engagement_multiplier=result["engagement_multiplier"],
        geo_multiplier=result["geo_multiplier"],
    )
    session.add(calculation)
    session.commit()

    return templates.TemplateResponse(
        "calculator.html",
        {
            "request": request,
            "user": user,
            "is_pro_or_premium": is_pro_or_premium,
            "result": result,
            "limit_reached": False,
            "message": None,
            "platform": platform_code,
            "niche": niche_code,
            "niche_other": niche_other_value,
            "deal_type": deal_type_code,
            "deal_type_other": deal_type_other_value,
            "followers": followers_value,
            "avg_views": avg_views_value,
            "engagement_rate": engagement_value,
            "geo_region": geo_region_code,
            "community_note": community_note,
        },
    )


@app.get("/calculations", response_class=HTMLResponse)
def calculation_history(
    request: Request,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
):
    statement = (
        select(Calculation)
        .where(Calculation.user_id == user.id)
        .order_by(Calculation.created_at.desc())
        .limit(100)
    )
    calculations = session.exec(statement).all()
    return templates.TemplateResponse(
        "calculation_history.html",
        {
            "request": request,
            "user": user,
            "calculations": calculations,
        },
    )


@app.post("/dev/set-plan")
def dev_set_plan(
    request: Request,
    plan: str = Form(...),
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
):
    normalized_plan = plan.strip().lower()
    if normalized_plan not in {"free", "pro", "premium"}:
        return RedirectResponse(url="/dashboard", status_code=303)
    user.plan = normalized_plan
    session.add(user)
    session.commit()
    return RedirectResponse(url="/dashboard", status_code=303)


@app.get("/deals/new", response_class=HTMLResponse)
def deal_new_form(
    request: Request,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
):
    profile = session.exec(
        select(CreatorProfile).where(CreatorProfile.user_id == user.id)
    ).first()
    form_data = {}
    if profile:
        form_data = {
            "platform": normalize_platform(profile.primary_platform),
            "niche": normalize_niche(profile.niche) if profile.niche else "",
            "geo_region": normalize_geo_region(profile.audience_location or "us"),
            "follower_count": profile.followers or "",
        }

    return templates.TemplateResponse(
        "deal_new.html",
        {
            "request": request,
            "user": user,
            "profile": profile,
            "saved": request.query_params.get("saved") == "1",
            "error": None,
            "form_data": form_data,
        },
    )


@app.post("/deals/new")
async def deal_new_submit(
    request: Request,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
):
    form = await request.form()
    profile = session.exec(
        select(CreatorProfile).where(CreatorProfile.user_id == user.id)
    ).first()

    form_data = dict(form)
    if profile:
        if not form_data.get("follower_count") and profile.followers is not None:
            form_data["follower_count"] = str(profile.followers)
        if not form_data.get("niche") and profile.niche:
            form_data["niche"] = profile.niche
        if not form_data.get("geo_region") and profile.audience_location:
            form_data["geo_region"] = profile.audience_location
        if not form_data.get("platform") and profile.primary_platform:
            form_data["platform"] = profile.primary_platform

    if form_data.get("platform"):
        form_data["platform"] = normalize_platform(form_data.get("platform"))
    if form_data.get("niche"):
        form_data["niche"] = normalize_niche(form_data.get("niche"))
    if form_data.get("geo_region"):
        form_data["geo_region"] = normalize_geo_region(form_data.get("geo_region"))
    if form_data.get("deal_type"):
        form_data["deal_type"] = normalize_deal_type(form_data.get("deal_type"))
    if form_data.get("content_format"):
        form_data["content_format"] = normalize_content_format(form_data.get("content_format"))

    try:
        contribution = create_deal_contribution_from_form(session, user, form_data)
    except ValueError:
        return templates.TemplateResponse(
            "deal_new.html",
            {
                "request": request,
                "user": user,
                "profile": profile,
                "saved": False,
                "error": "Total fee is required and must be greater than 0.",
                "form_data": form_data,
            },
            status_code=400,
        )

    negotiation_id = form_data.get("negotiation_session_id")
    if negotiation_id:
        try:
            negotiation_id_int = int(negotiation_id)
        except ValueError:
            negotiation_id_int = None
        if negotiation_id_int:
            link_negotiation_to_deal(session, negotiation_id_int, contribution)

    return RedirectResponse(url="/deals/new?saved=1", status_code=303)


@app.get("/analytics", response_class=HTMLResponse)
def analytics_dashboard(
    request: Request,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
):
    plan_redirect = require_plan_or_redirect(user, ["pro", "premium"], "analytics")
    if plan_redirect:
        return plan_redirect

    analytics = build_user_analytics(session, user.id)
    return templates.TemplateResponse(
        "analytics.html",
        {
            "request": request,
            "user": user,
            "analytics": analytics,
        },
    )


def _current_year_quarter() -> tuple[int, int]:
    today = datetime.utcnow()
    quarter = ((today.month - 1) // 3) + 1
    return today.year, quarter


@app.get("/reports/niche", response_class=HTMLResponse)
def niche_report(
    request: Request,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
):
    plan_redirect = require_plan_or_redirect(user, ["premium"], "niche-reports")
    if plan_redirect:
        return plan_redirect

    niche = request.query_params.get("niche") or ""
    platform = request.query_params.get("platform") or "all"

    default_year, default_quarter = _current_year_quarter()
    try:
        year = int(request.query_params.get("year") or default_year)
    except ValueError:
        year = default_year
    try:
        quarter = int(request.query_params.get("quarter") or default_quarter)
    except ValueError:
        quarter = default_quarter
    if quarter not in {1, 2, 3, 4}:
        quarter = default_quarter

    stats = None
    report_text = None
    normalized_niche = normalize_niche(niche) if niche else ""
    normalized_platform = normalize_platform(platform) if platform != "all" else "all"

    if normalized_niche:
        stats = build_quarterly_niche_stats(
            session=session,
            niche_code=normalized_niche,
            platform_code=normalized_platform,
            year=year,
            quarter=quarter,
        )
        if stats.get("enough_data_for_report"):
            report_text = generate_niche_report(stats)

    return templates.TemplateResponse(
        "niche_report.html",
        {
            "request": request,
            "user": user,
            "stats": stats,
            "report_text": report_text,
            "selected_niche": normalized_niche,
            "selected_platform": normalized_platform,
            "selected_year": year,
            "selected_quarter": quarter,
            "current_year": default_year,
        },
    )


@app.get("/rate-index", response_class=HTMLResponse)
def rate_index(
    request: Request,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
):
    plan_redirect = require_plan_or_redirect(user, ["premium"], "rate-index")
    if plan_redirect:
        return plan_redirect

    shared_count = session.exec(
        select(DealContribution)
        .where(DealContribution.user_id == user.id, DealContribution.share_in_index == True)
        .limit(1)
    ).all()
    if not shared_count:
        return templates.TemplateResponse(
            "rate_index.html",
            {
                "request": request,
                "user": user,
                "needs_contribution": True,
            },
        )

    platform = normalize_platform(request.query_params.get("platform") or "") if request.query_params.get("platform") else ""
    niche = normalize_niche(request.query_params.get("niche") or "") if request.query_params.get("niche") else ""
    follower_tier = normalize_follower_tier(request.query_params.get("follower_tier") or "") if request.query_params.get("follower_tier") else ""
    geo_region = normalize_geo_region(request.query_params.get("geo_region") or "") if request.query_params.get("geo_region") else ""
    deal_type = normalize_deal_type(request.query_params.get("deal_type") or "") if request.query_params.get("deal_type") else ""
    timeframe = (request.query_params.get("timeframe") or "6m").strip()

    statement = select(DealContribution).where(
        DealContribution.share_in_index == True,
        DealContribution.total_fee_usd > 0,
    )

    if timeframe != "all":
        months_map = {"3m": 3, "6m": 6, "12m": 12}
        months = months_map.get(timeframe, 6)
        cutoff = datetime.utcnow() - timedelta(days=30 * months)
        statement = statement.where(DealContribution.created_at >= cutoff)

    if platform:
        statement = statement.where(DealContribution.platform == platform)
    if follower_tier:
        statement = statement.where(DealContribution.follower_tier == follower_tier)
    if geo_region:
        statement = statement.where(DealContribution.geo_region == geo_region)
    if niche:
        statement = statement.where(DealContribution.niche == niche)
    if deal_type:
        statement = statement.where(DealContribution.deal_type == deal_type)

    contributions = session.exec(statement.order_by(DealContribution.created_at.desc())).all()

    fees = [float(row.total_fee_usd) for row in contributions if row.total_fee_usd is not None]
    views = [row.reported_views or 0 for row in contributions]

    fee_summary = summarize_fees_outlier_safe(fees)
    cpm_summary = summarize_cpm_outlier_safe(fees, views)

    return templates.TemplateResponse(
        "rate_index.html",
        {
            "request": request,
            "user": user,
            "needs_contribution": False,
            "contributions": contributions,
            "fee_summary": fee_summary,
            "cpm_summary": cpm_summary if cpm_summary["count"] > 0 else None,
            "filters": {
                "platform": platform,
                "niche": niche,
                "follower_tier": follower_tier,
                "geo_region": geo_region,
                "deal_type": deal_type,
                "timeframe": timeframe,
            },
        },
    )
