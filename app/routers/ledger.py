"""
Unified ledger routes.

Bank rows are the counted money source. Discord, Shopify, and TikTok are
supporting context that help reviewers decide what to do with each row.
"""
from __future__ import annotations

import csv
import json
import logging
from io import StringIO
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import JSONResponse, RedirectResponse, Response
from sqlmodel import Session

from ..csrf import CSRFProtectedRoute
from ..db import get_session
from ..discord.corrections import save_review_correction, snapshot_message_parse
from ..financial_audit import record_financial_audit
from ..ledger import (
    LEDGER_ACTION_REASON_LABELS,
    LEDGER_STATUS_LABELS,
    apply_ledger_automation,
    apply_ledger_rule,
    build_ledger_page_data,
    create_ledger_rule,
    draft_ledger_rule_from_instruction,
    draft_ledger_rule_with_ai,
    expense_category_label,
    format_ledger_money,
    ledger_action_reason_for_bank_row,
    ledger_filters_from_values,
    ledger_source_for_bank_row,
    ledger_status_for_bank_row,
    LEDGER_AGENT_MAX_LIMIT,
    preview_ledger_automation,
    preview_ledger_review_agent,
    preview_ledger_rule,
    run_ledger_review_agent,
)
from ..models import BankTransaction, DiscordMessage, LedgerRule, PARSE_PARSED, PARSE_REVIEW_REQUIRED, utcnow
from ..shared import *  # noqa: F401,F403 -- templates, auth helpers, user labels

router = APIRouter(route_class=CSRFProtectedRoute)
logger = logging.getLogger(__name__)


def _deal_type_for_ledger_entry_kind(entry_kind: str) -> str | None:
    if entry_kind == "sale":
        return "sell"
    if entry_kind in {"buy", "trade", "expense", "loan_draw", "loan_repayment", "transfer", "unknown"}:
        return entry_kind
    return None


def _money_for_ledger_entry(entry_kind: str, amount: float) -> tuple[float, float]:
    if entry_kind in {"sale", "loan_draw"}:
        return amount, 0.0
    if entry_kind in {"buy", "expense", "loan_repayment", "transfer"}:
        return 0.0, amount
    return 0.0, 0.0


def _ledger_redirect_url(
    *,
    account: str = "",
    start: str = "",
    end: str = "",
    status: str = "all",
    category: str = "",
    source: str = "",
    action_reason: str = "",
    search: str = "",
    sort: str = "posted_at",
    direction: str = "desc",
    include_cash: bool | str = True,
    success: str = "",
    error: str = "",
) -> str:
    params: dict[str, str] = {}
    for key, value in {
        "account": account,
        "start": start,
        "end": end,
        "status": status,
        "category": category,
        "source": source,
        "action_reason": action_reason,
        "search": search,
        "sort": sort,
        "direction": direction,
        "include_cash": "true" if include_cash is True or str(include_cash).lower() in {"1", "true", "yes", "on"} else "false",
        "success": success,
        "error": error,
    }.items():
        if value:
            params[key] = str(value)
    return "/ledger" + (f"?{urlencode(params)}" if params else "")


def _wants_json(request: Request) -> bool:
    requested_with = request.headers.get("x-requested-with", "").lower()
    accept = request.headers.get("accept", "").lower()
    return requested_with in {"fetch", "xmlhttprequest"} or "application/json" in accept


def _ledger_row_json(row: BankTransaction) -> dict[str, object]:
    status = ledger_status_for_bank_row(row)
    action_reason = ledger_action_reason_for_bank_row(row)
    source = ledger_source_for_bank_row(row)
    category = row.expense_category or "uncategorized"
    return {
        "id": row.id,
        "amount": float(row.amount or 0.0),
        "amount_display": format_ledger_money(row.amount),
        "ledger_status": status,
        "ledger_status_label": LEDGER_STATUS_LABELS.get(status, status.replace("_", " ").title()),
        "action_reason": action_reason,
        "action_reason_label": LEDGER_ACTION_REASON_LABELS.get(action_reason, ""),
        "source": source,
        "expense_category": category,
        "expense_category_label": expense_category_label(category),
        "review_status": row.review_status or "open",
        "review_note": row.review_note or "",
        "category_confidence": row.category_confidence or "",
    }


def _bank_transaction_audit_snapshot(row: BankTransaction) -> dict[str, object]:
    return {
        "classification": row.classification,
        "confidence": row.confidence,
        "expense_category": row.expense_category,
        "expense_subcategory": row.expense_subcategory,
        "category_confidence": row.category_confidence,
        "category_reason": row.category_reason,
        "review_status": row.review_status,
        "review_note": row.review_note,
        "matched_transaction_id": row.matched_transaction_id,
        "matched_source_message_id": row.matched_source_message_id,
        "matched_platform": row.matched_platform,
        "match_reason": row.match_reason,
        "match_override_status": row.match_override_status,
        "match_override_note": row.match_override_note,
    }


@router.get("/ledger")
def ledger_page(
    request: Request,
    account: str = Query(default=""),
    start: str = Query(default=""),
    end: str = Query(default=""),
    status: str = Query(default="all"),
    category: str = Query(default=""),
    source: str = Query(default=""),
    action_reason: str = Query(default=""),
    search: str = Query(default=""),
    sort: str = Query(default="posted_at"),
    direction: str = Query(default="desc"),
    include_cash: bool = Query(default=True),
    success: str = Query(default=""),
    error: str = Query(default=""),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    filters = ledger_filters_from_values(
        account=account,
        start=start,
        end=end,
        status=status,
        category=category,
        source=source,
        action_reason=action_reason,
        search=search,
        sort=sort,
        direction=direction,
        include_cash=include_cash,
    )
    data = build_ledger_page_data(session, filters)
    data["automation_previews"] = [
        preview_ledger_automation(session, action_key="mark_needs_log_checked", filters=filters)
    ]
    return templates.TemplateResponse(
        request,
        "ledger.html",
        {
            "request": request,
            "title": "Unified Ledger",
            "current_user": getattr(request.state, "current_user", None),
            "success": success,
            "error": error,
            **data,
        },
    )


@router.post("/ledger/transactions/{source_message_id}/edit-form")
def ledger_transaction_edit_form(
    request: Request,
    source_message_id: int,
    entry_kind: str = Form(default="unknown"),
    amount: str = Form(default=""),
    payment_method: str = Form(default="unknown"),
    expense_category: str = Form(default="uncategorized"),
    notes: str = Form(default=""),
    selected_account: str = Form(default=""),
    selected_start: str = Form(default=""),
    selected_end: str = Form(default=""),
    selected_status: str = Form(default="all"),
    selected_category: str = Form(default=""),
    selected_source: str = Form(default=""),
    selected_action_reason: str = Form(default=""),
    selected_search: str = Form(default=""),
    selected_sort: str = Form(default="posted_at"),
    selected_direction: str = Form(default="desc"),
    selected_include_cash: str = Form(default="true"),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial

    row = session.get(DiscordMessage, source_message_id)
    if not row:
        return RedirectResponse(
            _ledger_redirect_url(
                account=selected_account,
                start=selected_start,
                end=selected_end,
                status=selected_status or "all",
                category=selected_category,
                source=selected_source,
                action_reason=selected_action_reason,
                search=selected_search,
                sort=selected_sort,
                direction=selected_direction,
                include_cash=selected_include_cash,
                error=f"Discord message {source_message_id} was not found.",
            ),
            status_code=303,
        )

    try:
        parsed_amount = parse_optional_float(amount)
    except ValueError:
        return RedirectResponse(
            _ledger_redirect_url(
                account=selected_account,
                start=selected_start,
                end=selected_end,
                status=selected_status or "all",
                category=selected_category,
                source=selected_source,
                action_reason=selected_action_reason,
                search=selected_search,
                sort=selected_sort,
                direction=selected_direction,
                include_cash=selected_include_cash,
                error="Amount must be a valid number.",
            ),
            status_code=303,
        )

    normalized_entry_kind = (entry_kind or "unknown").strip().lower() or "unknown"
    normalized_amount = round(abs(float(parsed_amount or 0.0)), 2)
    normalized_payment_method = (payment_method or "unknown").strip().lower() or "unknown"
    normalized_expense_category = (expense_category or "uncategorized").strip() or "uncategorized"
    money_in, money_out = _money_for_ledger_entry(normalized_entry_kind, normalized_amount)
    incomplete = (
        parsed_amount is None
        or normalized_entry_kind == "unknown"
        or normalized_expense_category in {"", "uncategorized"}
    )

    parsed_before = snapshot_message_parse(row)
    row.deal_type = _deal_type_for_ledger_entry_kind(normalized_entry_kind)
    row.entry_kind = normalized_entry_kind
    row.amount = normalized_amount if parsed_amount is not None else None
    row.payment_method = normalized_payment_method
    row.cash_direction = None
    row.category = normalized_expense_category
    row.expense_category = normalized_expense_category
    row.money_in = money_in
    row.money_out = money_out
    row.notes = (notes or "").strip() or row.notes
    row.confidence = max(float(row.confidence or 0.0), 0.99)
    row.parse_status = PARSE_REVIEW_REQUIRED if incomplete else PARSE_PARSED
    row.needs_review = incomplete
    if incomplete:
        row.reviewed_by = None
        row.reviewed_at = None
    else:
        row.reviewed_by = current_user_label(request)
        row.reviewed_at = utcnow()
    row.last_error = None

    session.add(row)
    save_review_correction(session, row, parsed_before=parsed_before)
    sync_transaction_from_message(session, row)
    parsed_after = snapshot_message_parse(row)
    if parsed_before != parsed_after:
        user = getattr(request.state, "current_user", None)
        record_financial_audit(
            session,
            action="financial.ledger_transaction.edit",
            resource_key=f"discordmessage:{row.id}",
            before=parsed_before,
            after=parsed_after,
            actor_user_id=getattr(user, "id", None),
            actor_label=current_user_label(request),
        )
    session.commit()
    invalidate_financial_report_caches()

    return RedirectResponse(
        _ledger_redirect_url(
            account=selected_account,
            start=selected_start,
            end=selected_end,
            status=selected_status or "all",
            category=selected_category,
            source=selected_source,
            action_reason=selected_action_reason,
            search=selected_search,
            sort=selected_sort,
            direction=selected_direction,
            include_cash=selected_include_cash,
            success=f"Updated Discord transaction {source_message_id}.",
        ),
        status_code=303,
    )


@router.post("/ledger/rows/{row_id}/status-form")
def ledger_row_status_form(
    request: Request,
    row_id: int,
    review_status: str = Form(default=""),
    classification: str = Form(default=""),
    expense_category: str = Form(default=""),
    note: str = Form(default=""),
    selected_account: str = Form(default=""),
    selected_start: str = Form(default=""),
    selected_end: str = Form(default=""),
    selected_status: str = Form(default="needs_action"),
    selected_category: str = Form(default=""),
    selected_source: str = Form(default=""),
    selected_action_reason: str = Form(default=""),
    selected_search: str = Form(default=""),
    selected_sort: str = Form(default="posted_at"),
    selected_direction: str = Form(default="desc"),
    selected_include_cash: str = Form(default=""),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    row = session.get(BankTransaction, row_id)
    if row:
        before = _bank_transaction_audit_snapshot(row)
        changed = False
        if classification:
            row.classification = classification
            changed = True
        if expense_category:
            row.expense_category = expense_category
            row.expense_subcategory = "Manual override"
            row.category_confidence = "manual"
            row.category_reason = "Manually changed from the ledger."
            changed = True
        if review_status in {"open", "reviewed", "ignored"}:
            row.review_status = review_status
            changed = True
        stripped_note = note.strip() if isinstance(note, str) else ""
        if stripped_note:
            row.review_note = stripped_note
            changed = True
        if changed:
            row.updated_at = utcnow()
            session.add(row)
            record_financial_audit(
                session,
                action="financial.ledger.row_edit",
                resource_key=f"bank_transactions:{row.id}",
                before=before,
                after=_bank_transaction_audit_snapshot(row),
                actor_user_id=getattr(getattr(request.state, "current_user", None), "id", None),
                actor_label=current_user_label(request),
            )
            session.commit()
            invalidate_financial_report_caches()
            session.refresh(row)
    if row and _wants_json(request):
        return JSONResponse({"ok": True, "row": _ledger_row_json(row)})
    if _wants_json(request):
        return JSONResponse({"ok": False, "error": "Ledger row not found"}, status_code=404)
    return RedirectResponse(
        url=_ledger_redirect_url(
            account=selected_account,
            start=selected_start,
            end=selected_end,
            status=selected_status,
            category=selected_category,
            source=selected_source,
            action_reason=selected_action_reason,
            search=selected_search,
            sort=selected_sort,
            direction=selected_direction,
            include_cash=selected_include_cash,
            success="Updated ledger row",
        ),
        status_code=303,
    )


@router.post("/ledger/rows/{row_id}/force-unmatch-form")
def ledger_row_force_unmatch_form(
    request: Request,
    row_id: int,
    mode: str = Form(default="force"),
    note: str = Form(default=""),
    selected_account: str = Form(default=""),
    selected_start: str = Form(default=""),
    selected_end: str = Form(default=""),
    selected_status: str = Form(default="needs_action"),
    selected_category: str = Form(default=""),
    selected_source: str = Form(default=""),
    selected_action_reason: str = Form(default=""),
    selected_search: str = Form(default=""),
    selected_sort: str = Form(default="posted_at"),
    selected_direction: str = Form(default="desc"),
    selected_include_cash: str = Form(default=""),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    row = session.get(BankTransaction, row_id)
    if row:
        before = _bank_transaction_audit_snapshot(row)
        if mode in {"clear", "none"}:
            row.match_override_status = None
            row.match_override_note = None
            row.match_override_at = None
            row.match_override_by = None
            success = "Cleared match override"
        else:
            row.match_override_status = "force_unmatched"
            row.match_override_note = (note or "").strip() or "Forced unmatched from the ledger."
            row.match_override_at = utcnow()
            row.match_override_by = current_user_label(request)
            row.matched_transaction_id = None
            row.matched_source_message_id = None
            row.matched_platform = None
            row.match_reason = "Manually forced unmatched from the ledger."
            success = "Forced row unmatched"
        row.updated_at = utcnow()
        session.add(row)
        record_financial_audit(
            session,
            action="financial.ledger.force_unmatch" if mode not in {"clear", "none"} else "financial.ledger.clear_unmatch",
            resource_key=f"bank_transactions:{row.id}",
            before=before,
            after=_bank_transaction_audit_snapshot(row),
            actor_user_id=getattr(getattr(request.state, "current_user", None), "id", None),
            actor_label=current_user_label(request),
            note=(note or "").strip(),
        )
        session.commit()
        invalidate_financial_report_caches()
    else:
        success = ""
    return RedirectResponse(
        url=_ledger_redirect_url(
            account=selected_account,
            start=selected_start,
            end=selected_end,
            status=selected_status,
            category=selected_category,
            source=selected_source,
            action_reason=selected_action_reason,
            search=selected_search,
            sort=selected_sort,
            direction=selected_direction,
            include_cash=selected_include_cash,
            success=success,
        ),
        status_code=303,
    )


@router.post("/ledger/agent/run-form")
def ledger_agent_run_form(
    request: Request,
    confirm: str = Form(default=""),
    selected_account: str = Form(default=""),
    selected_start: str = Form(default=""),
    selected_end: str = Form(default=""),
    selected_status: str = Form(default="needs_action"),
    selected_category: str = Form(default=""),
    selected_source: str = Form(default=""),
    selected_action_reason: str = Form(default=""),
    selected_search: str = Form(default=""),
    selected_sort: str = Form(default="posted_at"),
    selected_direction: str = Form(default="desc"),
    selected_include_cash: str = Form(default=""),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    if confirm != "run_agent":
        return RedirectResponse(
            url=_ledger_redirect_url(
                account=selected_account,
                start=selected_start,
                end=selected_end,
                status=selected_status,
                category=selected_category,
                source=selected_source,
                action_reason=selected_action_reason,
                search=selected_search,
                sort=selected_sort,
                direction=selected_direction,
                include_cash=selected_include_cash,
                error="Preview required before running the ledger agent",
            ),
            status_code=303,
        )
    filters = ledger_filters_from_values(
        account=selected_account,
        start=selected_start,
        end=selected_end,
        status=selected_status or "needs_action",
        category=selected_category,
        source=selected_source,
        action_reason=selected_action_reason,
        search=selected_search,
        sort=selected_sort,
        direction=selected_direction,
        include_cash=selected_include_cash,
        limit=LEDGER_AGENT_MAX_LIMIT,
    )
    result = run_ledger_review_agent(
        session,
        filters=filters,
        limit=LEDGER_AGENT_MAX_LIMIT,
        applied_by=current_user_label(request),
    )
    success = (
        f"Ledger agent updated {result['updated_count']} row(s): "
        f"{result['cleared_false_matches']} bad match(es) cleared, "
        f"{result['auto_reviewed']} row(s) auto-reviewed."
    )
    return RedirectResponse(
        url=_ledger_redirect_url(
            account=selected_account,
            start=selected_start,
            end=selected_end,
            status=selected_status,
            category=selected_category,
            source=selected_source,
            action_reason=selected_action_reason,
            search=selected_search,
            sort=selected_sort,
            direction=selected_direction,
            include_cash=selected_include_cash,
            success=success,
        ),
        status_code=303,
    )


@router.post("/ledger/agent/preview-form")
def ledger_agent_preview_form(
    request: Request,
    selected_account: str = Form(default=""),
    selected_start: str = Form(default=""),
    selected_end: str = Form(default=""),
    selected_status: str = Form(default="needs_action"),
    selected_category: str = Form(default=""),
    selected_source: str = Form(default=""),
    selected_action_reason: str = Form(default=""),
    selected_search: str = Form(default=""),
    selected_sort: str = Form(default="posted_at"),
    selected_direction: str = Form(default="desc"),
    selected_include_cash: str = Form(default=""),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    filters = ledger_filters_from_values(
        account=selected_account,
        start=selected_start,
        end=selected_end,
        status=selected_status or "needs_action",
        category=selected_category,
        source=selected_source,
        action_reason=selected_action_reason,
        search=selected_search,
        sort=selected_sort,
        direction=selected_direction,
        include_cash=selected_include_cash,
        limit=LEDGER_AGENT_MAX_LIMIT,
    )
    preview = preview_ledger_review_agent(
        session,
        filters=filters,
        limit=LEDGER_AGENT_MAX_LIMIT,
    )
    return templates.TemplateResponse(
        request,
        "ledger_agent_preview.html",
        {
            "request": request,
            "title": "Ledger Agent Preview",
            "current_user": getattr(request.state, "current_user", None),
            "preview": preview,
            "selected_account": selected_account,
            "selected_start": selected_start,
            "selected_end": selected_end,
            "selected_status": selected_status,
            "selected_category": selected_category,
            "selected_source": selected_source,
            "selected_action_reason": selected_action_reason,
            "selected_search": selected_search,
            "selected_sort": selected_sort,
            "selected_direction": selected_direction,
            "selected_include_cash": selected_include_cash,
        },
    )


@router.post("/ledger/automation/{action_key}/apply-form")
def ledger_automation_apply_form(
    request: Request,
    action_key: str,
    selected_account: str = Form(default=""),
    selected_start: str = Form(default=""),
    selected_end: str = Form(default=""),
    selected_status: str = Form(default="needs_action"),
    selected_category: str = Form(default=""),
    selected_source: str = Form(default=""),
    selected_action_reason: str = Form(default=""),
    selected_search: str = Form(default=""),
    selected_sort: str = Form(default="posted_at"),
    selected_direction: str = Form(default="desc"),
    selected_include_cash: str = Form(default=""),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    filters = ledger_filters_from_values(
        account=selected_account,
        start=selected_start,
        end=selected_end,
        status=selected_status or "needs_action",
        category=selected_category,
        source=selected_source,
        action_reason=selected_action_reason,
        search=selected_search,
        sort=selected_sort,
        direction=selected_direction,
        include_cash=selected_include_cash,
        limit=1000,
    )
    try:
        result = apply_ledger_automation(
            session,
            action_key=action_key,
            filters=filters,
            applied_by=current_user_label(request),
        )
        success = f"Automation updated {result['updated_count']} of {result['matched_count']} matching row(s)."
        error = ""
    except ValueError as exc:
        success = ""
        error = str(exc)
    except Exception:
        logger.exception("ledger automation apply failed")
        success = ""
        error = "An unexpected error occurred, please try again."
    return RedirectResponse(
        url=_ledger_redirect_url(
            account=selected_account,
            start=selected_start,
            end=selected_end,
            status=selected_status,
            category=selected_category,
            source=selected_source,
            action_reason=selected_action_reason,
            search=selected_search,
            sort=selected_sort,
            direction=selected_direction,
            include_cash=selected_include_cash,
            success=success,
            error=error,
        ),
        status_code=303,
    )


async def _preview_payload(request: Request) -> dict[str, str]:
    content_type = request.headers.get("content-type", "")
    if "application/json" in content_type:
        try:
            payload = await request.json()
            return {str(key): value for key, value in payload.items()}
        except Exception:
            return {}
    form = await request.form()
    return {str(key): value for key, value in form.items()}


@router.post("/ledger/rules/preview")
async def ledger_rule_preview(
    request: Request,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    payload = await _preview_payload(request)
    instruction = str(payload.get("instruction") or "").strip()
    try:
        if payload.get("conditions_json") and payload.get("actions_json"):
            draft = {
                "name": str(payload.get("name") or "Ledger rule"),
                "summary": "",
                "conditions": json.loads(str(payload.get("conditions_json") or "{}")),
                "actions": json.loads(str(payload.get("actions_json") or "{}")),
                "confidence": "manual",
                "warnings": [],
                "source": "submitted",
            }
        else:
            use_ai = str(payload.get("use_ai") or "true").lower() != "false"
            draft = draft_ledger_rule_with_ai(instruction) if use_ai else draft_ledger_rule_from_instruction(instruction)
        filters = ledger_filters_from_values(
            account=str(payload.get("account") or ""),
            start=str(payload.get("start") or ""),
            end=str(payload.get("end") or ""),
            status=str(payload.get("status") or "all"),
            category=str(payload.get("category") or ""),
            source=str(payload.get("source") or ""),
            action_reason=str(payload.get("action_reason") or ""),
            search=str(payload.get("search") or ""),
            sort=str(payload.get("sort") or "posted_at"),
            direction=str(payload.get("direction") or "desc"),
            include_cash=str(payload.get("include_cash") or ""),
        )
        preview = preview_ledger_rule(
            session,
            conditions=draft.get("conditions") or {},
            actions=draft.get("actions") or {},
            filters=filters,
        )
        warnings = list(draft.get("warnings") or []) + list(preview.get("warnings") or [])
        draft["warnings"] = warnings
        return JSONResponse({"ok": True, "draft": draft, "preview": preview})
    except Exception as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)


@router.post("/ledger/rules/create-form")
def ledger_rule_create_form(
    request: Request,
    name: str = Form(default="Ledger rule"),
    description: str = Form(default=""),
    conditions_json: str = Form(default="{}"),
    actions_json: str = Form(default="{}"),
    selected_account: str = Form(default=""),
    selected_start: str = Form(default=""),
    selected_end: str = Form(default=""),
    selected_status: str = Form(default="needs_action"),
    selected_category: str = Form(default=""),
    selected_source: str = Form(default=""),
    selected_action_reason: str = Form(default=""),
    selected_search: str = Form(default=""),
    selected_sort: str = Form(default="posted_at"),
    selected_direction: str = Form(default="desc"),
    selected_include_cash: str = Form(default=""),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    try:
        conditions = json.loads(conditions_json or "{}")
        actions = json.loads(actions_json or "{}")
        rule = create_ledger_rule(
            session,
            name=name,
            description=description,
            conditions=conditions if isinstance(conditions, dict) else {},
            actions=actions if isinstance(actions, dict) else {},
            created_by=current_user_label(request),
        )
        success = f"Saved ledger rule #{rule.id}"
        error = ""
    except Exception as exc:
        success = ""
        error = str(exc)
    return RedirectResponse(
        url=_ledger_redirect_url(
            account=selected_account,
            start=selected_start,
            end=selected_end,
            status=selected_status,
            category=selected_category,
            source=selected_source,
            action_reason=selected_action_reason,
            search=selected_search,
            sort=selected_sort,
            direction=selected_direction,
            include_cash=selected_include_cash,
            success=success,
            error=error,
        ),
        status_code=303,
    )


@router.post("/ledger/rules/{rule_id}/apply-form")
def ledger_rule_apply_form(
    request: Request,
    rule_id: int,
    selected_account: str = Form(default=""),
    selected_start: str = Form(default=""),
    selected_end: str = Form(default=""),
    selected_status: str = Form(default="all"),
    selected_category: str = Form(default=""),
    selected_source: str = Form(default=""),
    selected_action_reason: str = Form(default=""),
    selected_search: str = Form(default=""),
    selected_sort: str = Form(default="posted_at"),
    selected_direction: str = Form(default="desc"),
    selected_include_cash: str = Form(default=""),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    rule = session.get(LedgerRule, rule_id)
    if not rule:
        return RedirectResponse(url=_ledger_redirect_url(error="Ledger rule not found"), status_code=303)
    filters = ledger_filters_from_values(
        account=selected_account,
        start=selected_start,
        end=selected_end,
        status=selected_status or "all",
        category=selected_category,
        source=selected_source,
        action_reason=selected_action_reason,
        search=selected_search,
        sort=selected_sort,
        direction=selected_direction,
        include_cash=selected_include_cash,
    )
    result = apply_ledger_rule(session, rule, filters=filters, applied_by=current_user_label(request))
    return RedirectResponse(
        url=_ledger_redirect_url(
            account=selected_account,
            start=selected_start,
            end=selected_end,
            status=selected_status,
            category=selected_category,
            source=selected_source,
            action_reason=selected_action_reason,
            search=selected_search,
            sort=selected_sort,
            direction=selected_direction,
            include_cash=selected_include_cash,
            success=f"Applied {rule.name} to {result['updated_count']} row(s)",
        ),
        status_code=303,
    )


@router.get("/ledger/export.csv")
def ledger_export_csv(
    request: Request,
    account: str = Query(default=""),
    start: str = Query(default=""),
    end: str = Query(default=""),
    status: str = Query(default="all"),
    category: str = Query(default=""),
    source: str = Query(default=""),
    action_reason: str = Query(default=""),
    search: str = Query(default=""),
    sort: str = Query(default="posted_at"),
    direction: str = Query(default="desc"),
    include_cash: bool = Query(default=True),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    filters = ledger_filters_from_values(
        account=account,
        start=start,
        end=end,
        status=status,
        category=category,
        source=source,
        action_reason=action_reason,
        search=search,
        sort=sort,
        direction=direction,
        include_cash=include_cash,
        limit=1000,
    )
    filters.limit = 1_000_000
    data = build_ledger_page_data(session, filters)
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "row_id",
            "row_kind",
            "posted_at",
            "account",
            "amount",
            "ledger_status",
            "source",
            "category",
            "classification",
            "description",
            "matched_transaction_id",
            "match_reason",
            "review_status",
            "review_note",
        ]
    )
    for row in data["rows"]:
        writer.writerow(
            [
                row["id"],
                row.get("row_kind", "bank"),
                row["posted_at_display"],
                row["account_label"],
                row["amount"],
                row["ledger_status"],
                row["source"],
                row["expense_category"],
                row["classification"],
                row["description"],
                row.get("matched_transaction_id"),
                row.get("match_reason"),
                row.get("review_status"),
                row.get("review_note"),
            ]
        )
    return Response(
        content=output.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="ledger-export.csv"'},
    )
