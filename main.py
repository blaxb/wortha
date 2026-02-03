from __future__ import annotations

from datetime import datetime
from typing import Optional

from fastapi import Depends, FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlmodel import Session, select
from starlette.middleware.sessions import SessionMiddleware

from auth import authenticate_user, get_current_user, hash_password, login_user, logout_user
from models import Calculation, User, create_db_and_tables, get_session

app = FastAPI()

app.add_middleware(
    SessionMiddleware,
    secret_key="CHANGE_ME_SESSION_SECRET",
    same_site="lax",
)

app.mount("/static", StaticFiles(directory="static"), name="static")

templates = Jinja2Templates(directory="templates")


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

    platform_key = (platform or "other").strip().lower()
    niche_key = (niche or "").strip().lower()

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

    geo_key = (geo_region or "").strip().lower()
    if geo_key in {"us", "usa", "canada"}:
        geo_multiplier = 1.1
    elif geo_key in {"uk", "eu", "europe"}:
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


@app.get("/calculator", response_class=HTMLResponse)
def calculator(
    request: Request,
    user: User = Depends(get_current_user),
):
    return templates.TemplateResponse(
        "calculator.html",
        {
            "request": request,
            "user": user,
            "result": None,
            "limit_reached": False,
            "message": None,
        },
    )


@app.post("/calculator", response_class=HTMLResponse)
def calculator_submit(
    request: Request,
    platform: str = Form(...),
    niche: str = Form(...),
    deal_type: str = Form(...),
    followers: Optional[str] = Form(None),
    avg_views: Optional[str] = Form(None),
    engagement_rate: Optional[str] = Form(None),
    geo_region: Optional[str] = Form("US"),
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

    now = datetime.utcnow()
    month_start = datetime(now.year, now.month, 1)
    if now.month == 12:
        next_month = datetime(now.year + 1, 1, 1)
    else:
        next_month = datetime(now.year, now.month + 1, 1)

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
                "result": None,
                "limit_reached": True,
                "message": "You’ve reached your 3 free calculations for this month. Upgrade to Pro to unlock unlimited pricing calculations.",
            },
        )

    result = calculate_rate(
        platform=platform,
        niche=niche,
        followers=followers_value,
        avg_views=avg_views_value,
        engagement_rate=engagement_value,
        geo_region=geo_region or "US",
    )

    calculation = Calculation(
        user_id=user.id,
        platform=platform,
        niche=niche,
        deal_type=deal_type,
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
            "result": result,
            "limit_reached": False,
            "message": None,
            "platform": platform,
            "niche": niche,
            "deal_type": deal_type,
            "followers": followers_value,
            "avg_views": avg_views_value,
            "engagement_rate": engagement_value,
            "geo_region": geo_region or "US",
        },
    )
