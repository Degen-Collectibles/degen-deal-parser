"""Persistent bank/Discord overlap evidence; ordinary edits cannot resolve it.

Rollback to a legacy note-only reader requires the read-only gate in
scripts/check_overlap_rollback_safety.py BEFORE and AFTER deployment. Retain a
copy outside the checkout before reverting (the revert may remove the script).
A nonzero exit blocks rollback: keep/roll forward to this reader, or restore the
legacy marker/context through a backed-up audited correction and recheck.
Never drop these evidence columns or audit records during a code rollback.
"""
from sqlmodel import select

from .models import BankTransaction
from .financial_audit import record_financial_audit

LEGACY_OVERLAP_MARKER = "[unresolved-discord-overlap]"


def validate_reconciliation_note(note):
    if LEGACY_OVERLAP_MARKER in (note or ""):
        raise ValueError("The overlap marker is reserved for evidence exceptions; enter a normal reconciliation note.")


def set_reconciliation_note(session, row, note, *, actor_label):
    """Preserve even an unmigrated legacy warning before replacing its note."""
    validate_reconciliation_note(note)
    if not row.discord_overlap_unresolved and LEGACY_OVERLAP_MARKER in (row.match_override_note or ""):
        before = row.model_dump(mode="json")
        row.discord_overlap_unresolved = True
        row.discord_overlap_note = row.discord_overlap_note or row.match_override_note
        session.add(row)
        record_financial_audit(
            session, action="financial.bank_overlap.preserve",
            resource_key=f"bank_transactions:{row.id}", before=before,
            after=row.model_dump(mode="json"), actor_label=actor_label,
            note="Preserve legacy overlap before reconciliation note edit.",
        )
    row.match_override_note = note


def overlap_warning_note(row):
    if getattr(row, "discord_overlap_unresolved", False):
        return getattr(row, "discord_overlap_note", None) or "Unresolved bank/Discord overlap; payment identity requires evidence."
    # Compatibility for legacy records until startup migration completes.
    note = getattr(row, "match_override_note", None) or ""
    return note if LEGACY_OVERLAP_MARKER in note else None


def migrate_legacy_discord_overlaps(session):
    """Capture existing evidence once, with row locks and an audit per change.

    Caller owns the transaction. Never clear a flag or replace stored evidence.
    A note edit, new match, or allocation is not proof an overlap was resolved.
    """
    rows = session.exec(select(BankTransaction).where(
        BankTransaction.discord_overlap_unresolved.is_not(True),
        BankTransaction.match_override_note.contains(LEGACY_OVERLAP_MARKER),
    ).order_by(BankTransaction.id).with_for_update()).all()
    for row in rows:
        before = row.model_dump(mode="json")
        row.discord_overlap_unresolved = True
        row.discord_overlap_note = row.discord_overlap_note or row.match_override_note
        session.add(row)
        record_financial_audit(
            session, action="financial.bank_overlap.migrate",
            resource_key=f"bank_transactions:{row.id}", before=before,
            after=row.model_dump(mode="json"), actor_label="schema migration",
            note="Preserve unresolved overlap independently of editable reconciliation notes.",
        )
    session.flush()
    return len(rows)
