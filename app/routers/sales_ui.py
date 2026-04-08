from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Depends, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.config import get_settings
from app.lead_management import normalize_lead_profile
from app.sales_lead_repository import get_sales_lead_repository
from app.sales_reporting import filter_leads, summarize_leads
from app.session_store import resolve_lead_session

router = APIRouter(prefix="/ui", tags=["sales-ui"])
templates = Jinja2Templates(directory="app/templates")
ROLE_ORDER = {"read": 1, "manager": 2, "admin": 3}
settings = get_settings()


def _redirect(path: str) -> RedirectResponse:
    return RedirectResponse(path, status_code=303)


def _token_role(token: str) -> str | None:
    token = str(token or "").strip()
    if not token:
        return None
    if settings.sales_dashboard_admin_token and token == settings.sales_dashboard_admin_token:
        return "admin"
    if settings.sales_dashboard_manager_token and token == settings.sales_dashboard_manager_token:
        return "manager"
    if settings.sales_dashboard_read_token and token == settings.sales_dashboard_read_token:
        return "read"
    if not settings.sales_dashboard_admin_token and token == settings.ai_agent_token:
        return "admin"
    if not settings.sales_dashboard_manager_token and token == settings.ai_agent_token:
        return "manager"
    if not settings.sales_dashboard_read_token and token == settings.ai_agent_token:
        return "read"
    return None


def _session_role(request: Request) -> str | None:
    role = str(request.session.get("sales_ui_role") or "").strip().casefold()
    return role if role in ROLE_ORDER else None


def _has_role(request: Request, required: str) -> bool:
    role = _session_role(request)
    if not role:
        return False
    return ROLE_ORDER.get(role, 0) >= ROLE_ORDER.get(required, 0)


def _require_role(request: Request, required: str = "read") -> RedirectResponse | None:
    if not _has_role(request, required):
        return _redirect("/ui/login")
    return None


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _selected_company_code(request: Request, explicit: str | None = None) -> str:
    if explicit:
        return explicit.strip()
    stored = str(request.session.get("sales_ui_company_code") or "").strip()
    return stored


async def _load_leads(company_code: str, limit: int) -> list[dict[str, Any]]:
    records = await get_sales_lead_repository().list_by_company(company_code=company_code, limit=limit)
    return [record["lead"] for record in records if isinstance(record, dict) and isinstance(record.get("lead"), dict)]


def _to_display_message(message: dict[str, Any]) -> dict[str, Any]:
    role = str(message.get("role") or "").strip() or "unknown"
    content = str(message.get("content") or "").strip()
    created_at = str(message.get("created_at") or "").strip()
    return {
        "role": role,
        "content": content,
        "created_at": created_at,
        "stage": message.get("stage"),
        "behavior_class": message.get("behavior_class"),
        "tool_name": message.get("tool_name"),
    }


def _parse_dt(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


@router.get("")
def ui_index(request: Request):
    if not _has_role(request, "read"):
        return _redirect("/ui/login")
    company_code = _selected_company_code(request)
    if company_code:
        return _redirect(f"/ui/leads?company_code={company_code}")
    return _redirect("/ui/leads")


@router.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    if _has_role(request, "read"):
        return _redirect("/ui/leads")
    return templates.TemplateResponse(
        "ui_login.html",
        {
            "request": request,
            "title": "Sales UI Login",
            "error": request.session.pop("sales_ui_error", None),
            "message": request.session.pop("sales_ui_message", None),
        },
    )


@router.post("/login")
async def login(request: Request, token: str = Form(...)):
    role = _token_role(token)
    if not role:
        request.session["sales_ui_error"] = "Invalid dashboard token"
        return _redirect("/ui/login")
    request.session["sales_ui_role"] = role
    request.session["sales_ui_logged_at"] = _now_iso()
    request.session["sales_ui_message"] = f"Logged in as {role}"
    return _redirect("/ui/leads")


@router.get("/logout")
def logout(request: Request):
    request.session.pop("sales_ui_role", None)
    request.session.pop("sales_ui_logged_at", None)
    request.session.pop("sales_ui_company_code", None)
    request.session.pop("sales_ui_message", None)
    request.session.pop("sales_ui_error", None)
    return _redirect("/ui/login")


@router.get("/leads", response_class=HTMLResponse)
async def leads_page(
    request: Request,
    company_code: str | None = Query(default=None),
    status: str | None = Query(default=None),
    temperature: str | None = Query(default=None),
    sales_owner_status: str | None = Query(default=None),
    source_channel: str | None = Query(default=None),
    q: str | None = Query(default=None),
    include_none: bool = Query(default=False),
    include_lost: bool = Query(default=True),
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=200),
):
    if not _has_role(request, "read"):
        return _redirect("/ui/login")
    resolved_company_code = _selected_company_code(request, company_code)
    if not resolved_company_code:
        return templates.TemplateResponse(
            "ui_dashboard.html",
            {
                "request": request,
                "title": "Sales Dashboard",
                "role": _session_role(request),
                "company_code": "",
                "message": request.session.pop("sales_ui_message", None),
                "error": request.session.pop("sales_ui_error", None),
                "summary": {},
                "leads": [],
                "filters": {
                    "status": status,
                    "temperature": temperature,
                    "sales_owner_status": sales_owner_status,
                    "source_channel": source_channel,
                    "q": q,
                    "include_none": include_none,
                    "include_lost": include_lost,
                },
                "offset": offset,
                "limit": limit,
                "total": 0,
                "company_codes": [],
                "selected_company_code": "",
            },
        )

    request.session["sales_ui_company_code"] = resolved_company_code
    leads = await _load_leads(resolved_company_code, limit=5000)
    filtered = filter_leads(
        leads,
        company_code=resolved_company_code,
        status=status,
        temperature=temperature,
        sales_owner_status=sales_owner_status,
        source_channel=source_channel,
        include_none=include_none,
        include_lost=include_lost,
        q=q,
    )
    paginated = filtered[offset : offset + limit]
    company_codes = sorted({str(lead.get("company_code") or "").strip() for lead in leads if str(lead.get("company_code") or "").strip()})
    return templates.TemplateResponse(
        "ui_dashboard.html",
        {
            "request": request,
            "title": "Sales Dashboard",
            "role": _session_role(request),
            "company_code": resolved_company_code,
            "message": request.session.pop("sales_ui_message", None),
            "error": request.session.pop("sales_ui_error", None),
            "summary": summarize_leads(filtered),
            "leads": paginated,
            "filters": {
                "status": status or "",
                "temperature": temperature or "",
                "sales_owner_status": sales_owner_status or "",
                "source_channel": source_channel or "",
                "q": q or "",
                "include_none": include_none,
                "include_lost": include_lost,
            },
            "offset": offset,
            "limit": limit,
            "total": len(filtered),
            "company_codes": company_codes,
            "selected_company_code": resolved_company_code,
        },
    )


@router.get("/leads/{lead_id}", response_class=HTMLResponse)
async def lead_detail(
    request: Request,
    lead_id: str,
    company_code: str | None = Query(default=None),
):
    if not _has_role(request, "read"):
        return _redirect("/ui/login")

    lead_record = await get_sales_lead_repository().get(lead_id)
    if not lead_record:
        raise HTTPException(status_code=404, detail="Lead not found")

    lead = lead_record.get("lead") if isinstance(lead_record.get("lead"), dict) else {}
    lead_profile = normalize_lead_profile(lead_record.get("lead_profile"))
    session = None
    resolved_session = await resolve_lead_session(lead_id)
    if resolved_session:
        _, _, session = resolved_session
    company = str(company_code or lead_record.get("company_code") or lead.get("company_code") or "").strip()
    if company:
        request.session["sales_ui_company_code"] = company

    messages = []
    if isinstance(session, dict):
        for message in session.get("messages", [])[-80:]:
            if isinstance(message, dict):
                messages.append(_to_display_message(message))

    timeline = []
    if isinstance(session, dict) and isinstance(session.get("lead_timeline"), list):
        timeline = list(session.get("lead_timeline") or [])
    elif isinstance(lead_record.get("timeline"), list):
        timeline = list(lead_record.get("timeline") or [])

    return templates.TemplateResponse(
        "ui_lead_detail.html",
        {
            "request": request,
            "title": f"Lead {lead_id}",
            "role": _session_role(request),
            "company_code": company,
            "lead": lead,
            "lead_profile": lead_profile,
            "lead_record": lead_record,
            "messages": messages,
            "timeline": timeline,
            "session": session or {},
            "session_loaded": bool(session),
            "lead_id": lead_id,
            "human_now": _now_iso(),
            "lead_created_at": _parse_dt(lead.get("created_at") or lead_profile.get("created_at")),
        },
    )
