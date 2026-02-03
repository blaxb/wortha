from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlmodel import Field, SQLModel, Session, create_engine

DATABASE_URL = "sqlite:///wortha.db"
engine = create_engine(DATABASE_URL, echo=False)


class User(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    email: str = Field(index=True, unique=True)
    username: str = Field(index=True, unique=True)
    hashed_password: str
    created_at: datetime = Field(default_factory=datetime.utcnow)


class Calculation(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    user_id: int = Field(foreign_key="user.id")
    platform: str
    niche: str
    deal_type: str
    followers: Optional[int] = None
    avg_views: Optional[int] = None
    engagement_rate: Optional[float] = None
    recommended_min: float
    recommended_max: float
    cpmm_base: float
    engagement_multiplier: float
    geo_multiplier: float
    created_at: datetime = Field(default_factory=datetime.utcnow)


def create_db_and_tables() -> None:
    SQLModel.metadata.create_all(engine)


def get_session():
    with Session(engine) as session:
        yield session
