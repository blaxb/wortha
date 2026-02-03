from __future__ import annotations

from fastapi import Depends, Request
from fastapi.responses import RedirectResponse
from passlib.context import CryptContext
from sqlmodel import Session, select

from models import User, get_session

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)


def authenticate_user(session: Session, email: str, password: str) -> User | None:
    statement = select(User).where(User.email == email)
    user = session.exec(statement).first()
    if not user:
        return None
    if not verify_password(password, user.hashed_password):
        return None
    return user


def login_user(request: Request, user: User) -> None:
    request.session["user_id"] = user.id


def logout_user(request: Request) -> None:
    request.session.clear()


def get_current_user(
    request: Request,
    session: Session = Depends(get_session),
) -> User:
    user_id = request.session.get("user_id")
    if not user_id:
        raise RedirectResponse(url="/login", status_code=303)
    user = session.get(User, user_id)
    if not user:
        raise RedirectResponse(url="/login", status_code=303)
    return user
