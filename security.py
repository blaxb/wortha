from __future__ import annotations

from fastapi import HTTPException
from fastapi.responses import RedirectResponse


def require_plan(user, allowed_plans: list[str]) -> None:
    plan = (user.plan or "free").lower()
    if plan not in allowed_plans:
        raise HTTPException(
            status_code=403,
            detail="Your plan does not include access to this feature.",
        )


def require_plan_or_redirect(user, allowed_plans: list[str], reason: str) -> None | RedirectResponse:
    plan = (user.plan or "free").lower()
    if plan not in allowed_plans:
        return RedirectResponse(url=f"/upgrade?reason={reason}", status_code=303)
    return None
