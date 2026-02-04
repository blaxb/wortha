from __future__ import annotations

from datetime import datetime
import sqlite3
from typing import Optional

from sqlmodel import Field, SQLModel, Session, create_engine, select

from constants import (
    normalize_content_format,
    normalize_deal_type,
    normalize_follower_tier,
    normalize_geo_region,
    normalize_niche,
    normalize_platform,
)

DATABASE_URL = "sqlite:///wortha.db"
engine = create_engine(DATABASE_URL, echo=False)


class User(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    email: str = Field(index=True, unique=True)
    username: str = Field(index=True, unique=True)
    hashed_password: str
    plan: str = Field(default="free", nullable=False)
    stripe_customer_id: Optional[str] = Field(default=None, nullable=True)
    stripe_subscription_id: Optional[str] = Field(default=None, nullable=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)


class Calculation(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    user_id: int = Field(foreign_key="user.id")
    platform: str
    niche: str
    niche_other: Optional[str] = None
    deal_type: str
    deal_type_other: Optional[str] = None
    geo_region: Optional[str] = None
    followers: Optional[int] = None
    avg_views: Optional[int] = None
    engagement_rate: Optional[float] = None
    recommended_min: float
    recommended_max: float
    cpmm_base: float
    engagement_multiplier: float
    geo_multiplier: float
    created_at: datetime = Field(default_factory=datetime.utcnow)


class CreatorProfile(SQLModel, table=True):
    __tablename__ = "creator_profile"

    id: Optional[int] = Field(default=None, primary_key=True)
    user_id: int = Field(foreign_key="user.id", unique=True)
    display_name: str
    tagline: Optional[str] = None
    primary_platform: str
    primary_platform_other: Optional[str] = None
    followers: Optional[int] = None
    avg_views: Optional[int] = None
    engagement_rate: Optional[float] = None
    niche: Optional[str] = None
    niche_other: Optional[str] = None
    audience_location: Optional[str] = None
    audience_location_notes: Optional[str] = None
    audience_age_range: Optional[str] = None
    audience_gender_split: Optional[str] = None
    bio: Optional[str] = None
    website_url: Optional[str] = None
    contact_email: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(
        default_factory=datetime.utcnow,
        sa_column_kwargs={"onupdate": datetime.utcnow},
    )


class MediaKitPackage(SQLModel, table=True):
    __tablename__ = "media_kit_package"

    id: Optional[int] = Field(default=None, primary_key=True)
    user_id: int = Field(foreign_key="user.id")
    name: str
    headline: Optional[str] = None
    price: Optional[float] = None
    deliverables: Optional[str] = None
    notes: Optional[str] = None
    sort_order: int = Field(default=0)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(
        default_factory=datetime.utcnow,
        sa_column_kwargs={"onupdate": datetime.utcnow},
    )


class NegotiationSession(SQLModel, table=True):
    __tablename__ = "negotiation_session"

    id: Optional[int] = Field(default=None, primary_key=True)
    user_id: int = Field(foreign_key="user.id", index=True)
    brand_name: Optional[str] = None
    platform: Optional[str] = None
    niche: Optional[str] = None
    deal_type: Optional[str] = None
    deal_type_other: Optional[str] = None
    content_format: Optional[str] = None
    content_format_other: Optional[str] = None
    followers: Optional[int] = None
    avg_views: Optional[int] = None
    engagement_rate: Optional[float] = None
    geo_region: Optional[str] = None
    brand_offer: float
    market_min: Optional[float] = None
    market_max: Optional[float] = None
    offer_vs_market_pct: Optional[float] = None
    recommended_counter_min: Optional[float] = None
    recommended_counter_max: Optional[float] = None
    assessment_text: Optional[str] = None
    email_subject: Optional[str] = None
    email_body: Optional[str] = None
    status: Optional[str] = None
    final_agreed_fee_usd: Optional[float] = None
    outcome: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(
        default_factory=datetime.utcnow,
        sa_column_kwargs={"onupdate": datetime.utcnow},
    )


class DealContribution(SQLModel, table=True):
    __tablename__ = "deal_contribution"

    id: Optional[int] = Field(default=None, primary_key=True)
    user_id: int = Field(foreign_key="user.id", index=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(
        default_factory=datetime.utcnow,
        sa_column_kwargs={"onupdate": datetime.utcnow},
    )
    platform: str
    niche: Optional[str] = None
    niche_other: Optional[str] = None
    geo_region: Optional[str] = None
    follower_count: Optional[int] = None
    follower_tier: str
    deal_type: Optional[str] = None
    deal_type_other: Optional[str] = None
    content_format: Optional[str] = None
    content_format_other: Optional[str] = None
    deliverables: Optional[str] = None
    usage_rights: Optional[str] = None
    duration_days: Optional[int] = None
    total_fee_usd: float
    quoted_fee_usd: Optional[float] = None
    cash_fee_usd: Optional[float] = None
    non_cash_value_usd: Optional[float] = None
    is_exclusive: Optional[bool] = None
    reported_views: Optional[int] = None
    reported_clicks: Optional[int] = None
    brand_name: Optional[str] = None
    negotiation_session_id: Optional[int] = Field(
        default=None, foreign_key="negotiation_session.id"
    )
    outcome: Optional[str] = None
    share_in_index: bool = Field(default=True)


def create_db_and_tables() -> None:
    SQLModel.metadata.create_all(engine)
    ensure_plan_column_exists()
    ensure_billing_columns_exist()
    ensure_optional_columns_exist()


def ensure_plan_column_exists() -> None:
    db_path = DATABASE_URL.replace("sqlite:///", "")
    with sqlite3.connect(db_path) as connection:
        cursor = connection.cursor()
        cursor.execute("PRAGMA table_info(user)")
        columns = {row[1] for row in cursor.fetchall()}
        if "plan" not in columns:
            cursor.execute(
                "ALTER TABLE user ADD COLUMN plan TEXT NOT NULL DEFAULT 'free'"
            )
            connection.commit()


def ensure_billing_columns_exist() -> None:
    db_path = DATABASE_URL.replace("sqlite:///", "")
    with sqlite3.connect(db_path) as connection:
        cursor = connection.cursor()
        cursor.execute("PRAGMA table_info(user)")
        columns = {row[1] for row in cursor.fetchall()}
        if "stripe_customer_id" not in columns:
            cursor.execute("ALTER TABLE user ADD COLUMN stripe_customer_id TEXT")
        if "stripe_subscription_id" not in columns:
            cursor.execute("ALTER TABLE user ADD COLUMN stripe_subscription_id TEXT")
        connection.commit()


def ensure_optional_columns_exist() -> None:
    db_path = DATABASE_URL.replace("sqlite:///", "")
    table_columns = {
        "calculation": {
            "niche_other": "TEXT",
            "deal_type_other": "TEXT",
            "geo_region": "TEXT",
        },
        "creator_profile": {
            "primary_platform_other": "TEXT",
            "niche_other": "TEXT",
            "audience_location_notes": "TEXT",
        },
        "negotiation_session": {
            "deal_type_other": "TEXT",
            "content_format": "TEXT",
            "content_format_other": "TEXT",
            "final_agreed_fee_usd": "REAL",
            "outcome": "TEXT",
        },
        "deal_contribution": {
            "niche_other": "TEXT",
            "deal_type_other": "TEXT",
            "content_format_other": "TEXT",
            "quoted_fee_usd": "REAL",
            "negotiation_session_id": "INTEGER",
            "outcome": "TEXT",
        },
    }

    with sqlite3.connect(db_path) as connection:
        cursor = connection.cursor()
        for table, columns in table_columns.items():
            cursor.execute(f"PRAGMA table_info({table})")
            existing = {row[1] for row in cursor.fetchall()}
            for column, column_type in columns.items():
                if column in existing:
                    continue
                cursor.execute(
                    f"ALTER TABLE {table} ADD COLUMN {column} {column_type}"
                )
        connection.commit()


def get_session():
    with Session(engine) as session:
        yield session


def get_or_create_creator_profile(session: Session, user_id: int) -> CreatorProfile:
    statement = select(CreatorProfile).where(CreatorProfile.user_id == user_id)
    profile = session.exec(statement).first()
    if profile:
        return profile
    profile = CreatorProfile(
        user_id=user_id,
        display_name="Creator Name",
        primary_platform="youtube",
        tagline="Short creator tagline",
        niche="lifestyle",
        audience_location="us",
        audience_age_range="18-34",
        audience_gender_split="60% women / 40% men",
        bio="Add a short brand-facing bio here.",
        website_url="https://example.com",
        contact_email="contact@example.com",
    )
    session.add(profile)
    session.commit()
    session.refresh(profile)
    return profile


def get_or_initialize_default_packages(
    session: Session, user_id: int
) -> list[MediaKitPackage]:
    statement = (
        select(MediaKitPackage)
        .where(MediaKitPackage.user_id == user_id)
        .order_by(MediaKitPackage.sort_order)
    )
    existing = session.exec(statement).all()
    existing_by_name = {package.name.lower(): package for package in existing}
    defaults = [("Basic", 0), ("Standard", 1), ("Premium", 2)]
    created_any = False

    for name, sort_order in defaults:
        if name.lower() in existing_by_name:
            continue
        package = MediaKitPackage(
            user_id=user_id,
            name=name,
            headline=f"{name} Package",
            deliverables="List deliverables here.",
            notes="Add optional notes like usage rights or timelines.",
            sort_order=sort_order,
        )
        session.add(package)
        created_any = True

    if created_any:
        session.commit()

    return session.exec(statement).all()


def get_recent_negotiations(
    session: Session, user_id: int, limit: int = 20
) -> list[NegotiationSession]:
    statement = (
        select(NegotiationSession)
        .where(NegotiationSession.user_id == user_id)
        .order_by(NegotiationSession.created_at.desc())
        .limit(limit)
    )
    return session.exec(statement).all()


def bucket_follower_count(followers: int | None) -> str:
    if followers is None or followers <= 0:
        return "under_5k"
    if followers < 5_000:
        return "under_5k"
    if followers < 10_000:
        return "5k_10k"
    if followers < 25_000:
        return "10k_25k"
    if followers < 50_000:
        return "25k_50k"
    if followers < 100_000:
        return "50k_100k"
    return "100k_plus"


def create_deal_contribution_from_form(
    session: Session, user: User, form_data: dict
) -> DealContribution:
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

    total_fee = to_float(form_data.get("total_fee_usd"))
    if total_fee is None or total_fee <= 0:
        raise ValueError("total_fee_usd must be provided and greater than 0.")

    follower_count = to_int(form_data.get("follower_count"))
    negotiation_session_id = to_int(form_data.get("negotiation_session_id"))
    if negotiation_session_id is not None and negotiation_session_id <= 0:
        negotiation_session_id = None
    geo_region = normalize_geo_region(form_data.get("geo_region"))
    follower_tier = bucket_follower_count(follower_count)

    platform_code = normalize_platform(form_data.get("platform"))
    niche_code = normalize_niche(form_data.get("niche"))
    deal_type_code = normalize_deal_type(form_data.get("deal_type"))
    content_format_code = normalize_content_format(form_data.get("content_format"))

    niche_other = (form_data.get("niche_other") or "").strip() or None
    deal_type_other = (form_data.get("deal_type_other") or "").strip() or None
    content_format_other = (form_data.get("content_format_other") or "").strip() or None
    outcome_raw = (form_data.get("outcome") or "").strip().lower()
    if outcome_raw not in {"won", "lost", "pending", "other"}:
        outcome_raw = "won"

    contribution = DealContribution(
        user_id=user.id,
        platform=platform_code,
        niche=niche_code,
        niche_other=niche_other if niche_code == "other" else None,
        geo_region=geo_region,
        follower_count=follower_count,
        follower_tier=normalize_follower_tier(follower_tier),
        deal_type=deal_type_code,
        deal_type_other=deal_type_other if deal_type_code == "other" else None,
        content_format=content_format_code,
        content_format_other=content_format_other if content_format_code == "other" else None,
        deliverables=(form_data.get("deliverables") or "").strip() or None,
        usage_rights=(form_data.get("usage_rights") or "").strip() or None,
        duration_days=to_int(form_data.get("duration_days")),
        total_fee_usd=total_fee,
        quoted_fee_usd=to_float(form_data.get("quoted_fee_usd")),
        cash_fee_usd=to_float(form_data.get("cash_fee_usd")),
        non_cash_value_usd=to_float(form_data.get("non_cash_value_usd")),
        is_exclusive=form_data.get("is_exclusive") in {"true", "on", "1", True},
        reported_views=to_int(form_data.get("reported_views")),
        reported_clicks=to_int(form_data.get("reported_clicks")),
        brand_name=(form_data.get("brand_name") or "").strip() or None,
        negotiation_session_id=negotiation_session_id,
        outcome=outcome_raw,
        share_in_index=form_data.get("share_in_index") not in {"false", "0"},
    )
    session.add(contribution)
    session.commit()
    session.refresh(contribution)
    return contribution


def link_negotiation_to_deal(
    session: Session, negotiation_id: int, deal: DealContribution
) -> None:
    negotiation = session.exec(
        select(NegotiationSession).where(NegotiationSession.id == negotiation_id)
    ).first()
    if not negotiation:
        return
    deal.negotiation_session_id = negotiation_id
    negotiation.final_agreed_fee_usd = deal.total_fee_usd
    negotiation.outcome = negotiation.outcome or "accepted"
    session.add(deal)
    session.add(negotiation)
    session.commit()
