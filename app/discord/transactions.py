from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from sqlalchemy import and_, func, or_, update
from sqlmodel import Session, select

from ..financial_values import validate_optional_confidence, validate_optional_money
from ..models import (
    BankTransaction,
    BookkeepingEntry,
    DISCORD_SOURCE_REFRESH_REQUIRED_ERROR,
    DiscordMessage,
    DiscordMessageRevision,
    discord_source_refresh_blocked,
    GmailEvidenceLink,
    expand_parse_status_filter_values,
    PARSE_FAILED,
    PARSE_IGNORED,
    PARSE_PARSED,
    PARSE_PENDING,
    PARSE_PROCESSING,
    PARSE_REVIEW_REQUIRED,
    Transaction,
    TransactionItem,
    TransactionSourceRevision,
    normalize_parse_status,
    normalize_money_value,
    signed_money_delta,
    utcnow,
)
from .financials import compute_financials
from .message_revisions import ensure_message_revision


NON_OPERATING_ENTRY_KINDS = {"loan_draw", "loan_repayment", "transfer"}
NON_OPERATING_EXPENSE_CATEGORIES = {"loan_owner_payments", "loan_proceeds", "partner_paybacks", "transfers"}
MAX_TRANSACTION_SOURCE_ROWS = 32


class StaleSourceRevisionError(RuntimeError):
    """Raised when parse output no longer matches the persisted source projection."""


class SourceRefreshRequiredError(RuntimeError):
    """Raised when only a canonical Discord refresh may change the projection."""


@dataclass(frozen=True)
class SourceMutationSnapshot:
    """Immutable source fields protected across a manual mutation transaction."""

    id: int
    content: str
    attachment_urls_json: str
    current_revision_id: int | None
    is_deleted: bool
    source_refresh_required: bool
    last_error: str | None
    stitched_group_id: str | None
    stitched_primary: bool
    stitched_message_ids_json: str
    active_parse_attempt_id: int | None


def require_canonical_source_projection(session: Session, row: DiscordMessage) -> None:
    if not discord_source_refresh_blocked(
        row.last_error,
        row.source_refresh_required,
    ):
        return
    row.source_refresh_required = True
    session.add(row)
    raise SourceRefreshRequiredError(
        f"message_id={row.id} requires canonical Discord source refresh"
    )


def _is_non_operating_transaction(entry_kind: str, expense_category: str) -> bool:
    return entry_kind in NON_OPERATING_ENTRY_KINDS or expense_category in NON_OPERATING_EXPENSE_CATEGORIES


def is_non_operating_transaction(entry_kind: str | None, expense_category: str | None) -> bool:
    return _is_non_operating_transaction(
        (entry_kind or "").strip().lower(),
        (expense_category or "").strip().lower(),
    )


def is_transaction_message(row: DiscordMessage) -> bool:
    if row.is_deleted:
        return False
    if normalize_parse_status(row.parse_status, is_deleted=row.is_deleted, needs_review=row.needs_review) in {
        PARSE_IGNORED,
        PARSE_PENDING,
        PARSE_PROCESSING,
        PARSE_FAILED,
    }:
        return False
    if row.stitched_group_id and not row.stitched_primary:
        return False
    return True


def _safe_json_list(value: Optional[str]) -> list[str]:
    try:
        loaded = json.loads(value or "[]")
    except json.JSONDecodeError:
        return []
    if not isinstance(loaded, list):
        return []
    cleaned: list[str] = []
    seen: set[str] = set()
    for item in loaded:
        text = str(item or "").strip()
        if not text:
            continue
        key = text.casefold()
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(text)
    return cleaned


def cleanup_transaction_dependents(
    session: Session,
    transaction: Transaction,
    *,
    bank_unmatch_reason: str,
) -> None:
    if transaction.id is None:
        return
    bookkeeping_rows = session.exec(
        select(BookkeepingEntry).where(BookkeepingEntry.matched_transaction_id == transaction.id)
    ).all()
    for bookkeeping_row in bookkeeping_rows:
        bookkeeping_row.matched_transaction_id = None
        bookkeeping_row.match_status = "unmatched"
        session.add(bookkeeping_row)

    bank_rows = session.exec(
        select(BankTransaction).where(BankTransaction.matched_transaction_id == transaction.id)
    ).all()
    for bank_row in bank_rows:
        bank_row.matched_transaction_id = None
        bank_row.matched_source_message_id = None
        bank_row.matched_platform = None
        bank_row.match_reason = bank_unmatch_reason
        if (bank_row.classification or "").startswith("logged_in_discord"):
            bank_row.classification = "needs_review"
            bank_row.confidence = "low"
        bank_row.updated_at = utcnow()
        session.add(bank_row)

    evidence_links = session.exec(
        select(GmailEvidenceLink).where(GmailEvidenceLink.transaction_id == transaction.id)
    ).all()
    for evidence_link in evidence_links:
        evidence_link.transaction_id = None
        session.add(evidence_link)

    items = session.exec(
        select(TransactionItem).where(TransactionItem.transaction_id == transaction.id)
    ).all()
    for item in items:
        session.delete(item)
    session.flush()


def _load_direct_transactions_for_message(
    session: Session,
    row: DiscordMessage,
) -> list[Transaction]:
    direct_transactions = session.exec(
        select(Transaction)
        .where(Transaction.source_message_id == row.id)
        .order_by(Transaction.id)
        .limit(MAX_TRANSACTION_SOURCE_ROWS + 1)
    ).all()
    if len(direct_transactions) > MAX_TRANSACTION_SOURCE_ROWS:
        raise StaleSourceRevisionError(
            "direct transaction fanout exceeds the safe invalidation bound"
        )
    return direct_transactions


def _load_source_associations_for_message(
    session: Session,
    row: DiscordMessage,
) -> list[TransactionSourceRevision]:
    associations = session.exec(
        select(TransactionSourceRevision)
        .where(TransactionSourceRevision.message_id == row.id)
        .order_by(
            TransactionSourceRevision.transaction_id,
            TransactionSourceRevision.id,
        )
        .limit(MAX_TRANSACTION_SOURCE_ROWS + 1)
    ).all()
    if len(associations) > MAX_TRANSACTION_SOURCE_ROWS:
        raise StaleSourceRevisionError(
            "transaction association fanout exceeds the safe invalidation bound"
        )
    return associations


def _load_legacy_unassociated_transactions_for_message(
    session: Session,
    row: DiscordMessage,
) -> list[Transaction]:
    if not row.stitched_group_id:
        return []
    legacy_group_rows = session.exec(
        select(DiscordMessage)
        .where(DiscordMessage.stitched_group_id == row.stitched_group_id)
        .order_by(DiscordMessage.created_at, DiscordMessage.id)
        .limit(MAX_TRANSACTION_SOURCE_ROWS + 1)
    ).all()
    if len(legacy_group_rows) > MAX_TRANSACTION_SOURCE_ROWS:
        raise StaleSourceRevisionError("legacy stitched source group is oversized")
    legacy_primaries = [
        group_row for group_row in legacy_group_rows if group_row.stitched_primary
    ]
    if len(legacy_primaries) != 1:
        raise StaleSourceRevisionError(
            "legacy stitched source group has an ambiguous primary"
        )
    primary_id = legacy_primaries[0].id
    if primary_id is None:
        raise StaleSourceRevisionError("legacy stitched primary is unpersisted")
    has_source_associations = (
        select(TransactionSourceRevision.id)
        .where(TransactionSourceRevision.transaction_id == Transaction.id)
        .exists()
    )
    legacy_transactions = session.exec(
        select(Transaction)
        .where(Transaction.source_message_id == primary_id)
        .where(~has_source_associations)
        .order_by(Transaction.id)
        .limit(MAX_TRANSACTION_SOURCE_ROWS + 1)
    ).all()
    if len(legacy_transactions) > MAX_TRANSACTION_SOURCE_ROWS:
        raise StaleSourceRevisionError(
            "legacy stitched transaction fanout exceeds the safe invalidation bound"
        )
    return legacy_transactions


def _bounded_transaction_ids_for_message(
    session: Session,
    row: DiscordMessage,
) -> list[int]:
    transaction_ids = {
        association.transaction_id
        for association in _load_source_associations_for_message(session, row)
    }
    transaction_ids.update(
        transaction.id
        for transaction in _load_direct_transactions_for_message(session, row)
        if transaction.id is not None
    )
    transaction_ids.update(
        transaction.id
        for transaction in _load_legacy_unassociated_transactions_for_message(
            session,
            row,
        )
        if transaction.id is not None
    )
    if len(transaction_ids) > MAX_TRANSACTION_SOURCE_ROWS:
        raise StaleSourceRevisionError(
            "combined transaction invalidation fanout exceeds the safe bound"
        )
    return sorted(transaction_ids)


def _tombstone_transaction_projection(
    session: Session,
    transaction: Transaction,
    *,
    reason: str,
    parse_status: str,
    source_row: DiscordMessage | None = None,
    compatibility_revision_id: int | None = None,
) -> None:
    cleanup_transaction_dependents(
        session,
        transaction,
        bank_unmatch_reason=reason,
    )
    if (
        source_row is not None
        and transaction.source_message_id == source_row.id
        and transaction.source_revision_id is None
        and compatibility_revision_id is not None
    ):
        transaction.source_revision_id = compatibility_revision_id
    transaction.is_deleted = True
    transaction.needs_review = True
    transaction.parse_status = parse_status
    transaction.updated_at = utcnow()
    session.add(transaction)


def _lock_matching_source_projection(session: Session, row: DiscordMessage) -> None:
    if row.id is None:
        raise StaleSourceRevisionError("Discord message has no persisted source identity")

    current_revision_predicate = (
        DiscordMessage.current_revision_id.is_(None)
        if row.current_revision_id is None
        else DiscordMessage.current_revision_id == row.current_revision_id
    )
    with session.no_autoflush:
        result = session.exec(
            update(DiscordMessage)
            .where(DiscordMessage.id == row.id)
            .where(DiscordMessage.content == row.content)
            .where(DiscordMessage.attachment_urls_json == row.attachment_urls_json)
            .where(current_revision_predicate)
            .where(DiscordMessage.is_deleted == row.is_deleted)
            .values(last_seen_at=DiscordMessage.last_seen_at)
            .execution_options(synchronize_session=False)
        )

    if result.rowcount != 1:
        raise StaleSourceRevisionError(
            f"Discord message source changed while parsing; discarding stale result for message_id={row.id}"
        )


def _load_transaction_source_associations(
    session: Session,
    transaction_id: int,
) -> list[TransactionSourceRevision]:
    associations = session.exec(
        select(TransactionSourceRevision)
        .where(TransactionSourceRevision.transaction_id == transaction_id)
        .order_by(
            TransactionSourceRevision.source_position,
            TransactionSourceRevision.id,
        )
        .limit(MAX_TRANSACTION_SOURCE_ROWS + 1)
    ).all()
    if len(associations) > MAX_TRANSACTION_SOURCE_ROWS:
        raise StaleSourceRevisionError(
            "transaction source association set is oversized"
        )
    return associations


def _delete_source_associations(session: Session, transaction_id: int) -> None:
    associations = _load_transaction_source_associations(session, transaction_id)
    for association in associations:
        session.delete(association)
    session.flush()


def invalidate_transactions_for_message(
    session: Session,
    row: DiscordMessage,
    *,
    reason: str,
    compatibility_revision_id: int | None = None,
) -> list[Transaction]:
    """Tombstone every transaction derived from a message projection.

    The association lookup is authoritative. The stitch lookup keeps legacy
    transactions safe until every pre-association row has been reparsed.
    """

    associations = _load_source_associations_for_message(session, row)
    transaction_ids = {association.transaction_id for association in associations}

    direct_transactions = _load_direct_transactions_for_message(session, row)
    transaction_ids.update(
        transaction.id
        for transaction in direct_transactions
        if transaction.id is not None
    )

    transaction_ids.update(
        transaction.id
        for transaction in _load_legacy_unassociated_transactions_for_message(
            session,
            row,
        )
        if transaction.id is not None
    )

    if len(transaction_ids) > MAX_TRANSACTION_SOURCE_ROWS:
        raise StaleSourceRevisionError(
            "combined transaction invalidation fanout exceeds the safe bound"
        )

    invalidated: list[Transaction] = []
    for transaction_id in sorted(transaction_ids):
        transaction = session.get(Transaction, transaction_id)
        if transaction is None:
            continue
        _tombstone_transaction_projection(
            session,
            transaction,
            reason=reason,
            parse_status=PARSE_PENDING,
            source_row=row,
            compatibility_revision_id=compatibility_revision_id,
        )
        invalidated.append(transaction)
    return invalidated


def _replace_source_associations(
    session: Session,
    transaction: Transaction,
    source_rows: list[DiscordMessage],
) -> None:
    if transaction.id is None:
        raise RuntimeError("Transaction must be persisted before binding source revisions")
    _delete_source_associations(session, transaction.id)
    seen_message_ids: set[int] = set()
    for position, source_row in enumerate(source_rows):
        if source_row.id is None:
            raise RuntimeError("Discord source message must be persisted before transaction sync")
        if source_row.id in seen_message_ids:
            raise RuntimeError(f"Duplicate Discord source message_id={source_row.id}")
        seen_message_ids.add(source_row.id)
        revision = ensure_message_revision(session, source_row)
        session.add(
            TransactionSourceRevision(
                transaction_id=transaction.id,
                message_id=source_row.id,
                revision_id=revision.id,
                source_position=position,
            )
        )
    session.flush()


def _source_row_order(row: DiscordMessage) -> tuple[datetime, int]:
    return row.created_at, row.id or 0


def _decode_stitched_message_ids(value: str) -> list[int]:
    try:
        decoded = json.loads(value or "[]")
    except (TypeError, json.JSONDecodeError) as exc:
        raise StaleSourceRevisionError("stitched source ids are malformed") from exc
    if not isinstance(decoded, list) or len(decoded) > MAX_TRANSACTION_SOURCE_ROWS:
        raise StaleSourceRevisionError("stitched source group is missing or oversized")
    if any(type(message_id) is not int or message_id <= 0 for message_id in decoded):
        raise StaleSourceRevisionError("stitched source ids are malformed")
    if len(set(decoded)) != len(decoded):
        raise StaleSourceRevisionError("stitched source ids contain duplicates")
    return decoded


def _validate_complete_source_rows(
    row: DiscordMessage,
    source_rows: list[DiscordMessage],
) -> list[DiscordMessage]:
    if not source_rows or len(source_rows) > MAX_TRANSACTION_SOURCE_ROWS:
        raise StaleSourceRevisionError("transaction source group is missing or oversized")
    if any(source_row.id is None for source_row in source_rows):
        raise StaleSourceRevisionError("transaction source group contains an unpersisted row")
    if len({source_row.id for source_row in source_rows}) != len(source_rows):
        raise StaleSourceRevisionError("transaction source group contains duplicate rows")

    ordered_rows = sorted(source_rows, key=_source_row_order)
    if ordered_rows[0].id != row.id:
        raise StaleSourceRevisionError("primary transaction source must be first")

    for source_row in ordered_rows:
        if discord_source_refresh_blocked(
            source_row.last_error,
            source_row.source_refresh_required,
        ):
            raise SourceRefreshRequiredError(
                f"message_id={source_row.id} requires canonical Discord source refresh"
            )
        if source_row.is_deleted:
            raise StaleSourceRevisionError(
                f"transaction source message_id={source_row.id} is deleted"
            )

    if len(ordered_rows) == 1:
        if row.stitched_group_id or row.stitched_primary:
            raise StaleSourceRevisionError("stitched source group is incomplete")
        return ordered_rows

    group_id = row.stitched_group_id
    if not group_id or not row.stitched_primary:
        raise StaleSourceRevisionError("stitched source primary metadata is inconsistent")
    ordered_ids = [source_row.id for source_row in ordered_rows]
    if sum(bool(source_row.stitched_primary) for source_row in ordered_rows) != 1:
        raise StaleSourceRevisionError("stitched source group has an ambiguous primary")
    for source_row in ordered_rows:
        if source_row.stitched_group_id != group_id:
            raise StaleSourceRevisionError("stitched source group ids are inconsistent")
        if _decode_stitched_message_ids(source_row.stitched_message_ids_json) != ordered_ids:
            raise StaleSourceRevisionError("stitched source membership is inconsistent")
        status = normalize_parse_status(
            source_row.parse_status,
            is_deleted=source_row.is_deleted,
            needs_review=source_row.needs_review,
        )
        if source_row.id == row.id:
            if status not in {PARSE_PARSED, PARSE_REVIEW_REQUIRED}:
                raise StaleSourceRevisionError("stitched primary is not in a reportable parse state")
        elif status != PARSE_IGNORED:
            raise StaleSourceRevisionError("stitched child is not a finalized group constituent")
    return ordered_rows


def _require_projection_matches_revision(
    session: Session,
    source_row: DiscordMessage,
    revision_id: int,
    *,
    context: str,
) -> None:
    revision = session.get(DiscordMessageRevision, revision_id)
    if (
        revision is None
        or revision.message_id != source_row.id
        or source_row.content != revision.content
        or source_row.attachment_urls_json != revision.attachment_urls_json
    ):
        raise StaleSourceRevisionError(
            f"{context} source projection does not match revision for "
            f"message_id={source_row.id}"
        )


def _load_bound_source_rows(
    session: Session,
    row: DiscordMessage,
    transaction: Transaction,
) -> list[DiscordMessage] | None:
    if transaction.id is None:
        return None
    associations = _load_transaction_source_associations(session, transaction.id)
    if not associations:
        return None
    if [association.source_position for association in associations] != list(
        range(len(associations))
    ):
        raise StaleSourceRevisionError("bound transaction source positions are inconsistent")
    primary_association = associations[0]
    if (
        transaction.source_message_id != row.id
        or primary_association.message_id != row.id
        or primary_association.message_id != transaction.source_message_id
        or primary_association.revision_id != transaction.source_revision_id
    ):
        raise StaleSourceRevisionError(
            "bound transaction primary source association is inconsistent"
        )

    source_rows: list[DiscordMessage] = []
    for association in associations:
        source_row = session.get(DiscordMessage, association.message_id)
        if source_row is None:
            raise StaleSourceRevisionError("bound transaction source message is missing")
        if source_row.current_revision_id != association.revision_id:
            raise StaleSourceRevisionError(
                f"bound source revision changed for message_id={association.message_id}"
            )
        _require_projection_matches_revision(
            session,
            source_row,
            association.revision_id,
            context="bound",
        )
        source_rows.append(source_row)
    return _validate_complete_source_rows(row, source_rows)


def _load_current_stitched_source_rows(
    session: Session,
    row: DiscordMessage,
) -> list[DiscordMessage]:
    if not row.stitched_group_id:
        if row.current_revision_id is not None:
            _require_projection_matches_revision(
                session,
                row,
                row.current_revision_id,
                context="current",
            )
        return _validate_complete_source_rows(row, [row])
    expected_ids = _decode_stitched_message_ids(row.stitched_message_ids_json)
    source_rows = session.exec(
        select(DiscordMessage)
        .where(DiscordMessage.stitched_group_id == row.stitched_group_id)
        .order_by(DiscordMessage.created_at, DiscordMessage.id)
        .limit(MAX_TRANSACTION_SOURCE_ROWS + 1)
    ).all()
    if len(source_rows) > MAX_TRANSACTION_SOURCE_ROWS:
        raise StaleSourceRevisionError("stitched source group is oversized")
    if [source_row.id for source_row in source_rows] != expected_ids:
        raise StaleSourceRevisionError("stitched source membership is incomplete")
    for source_row in source_rows:
        if source_row.current_revision_id is not None:
            _require_projection_matches_revision(
                session,
                source_row,
                source_row.current_revision_id,
                context="current",
            )
    return _validate_complete_source_rows(row, source_rows)


def _raise_if_source_mutation_blocked(source_row: DiscordMessage) -> None:
    if discord_source_refresh_blocked(
        source_row.last_error,
        source_row.source_refresh_required,
    ):
        raise SourceRefreshRequiredError(
            f"message_id={source_row.id} requires canonical Discord source refresh"
        )
    if source_row.is_deleted:
        raise StaleSourceRevisionError(
            f"transaction source message_id={source_row.id} is deleted"
        )


def _load_current_source_group_for_mutation(
    session: Session,
    row: DiscordMessage,
) -> list[DiscordMessage]:
    """Load the complete current group without requiring finalized parse states.

    Manual retry is an intentional recovery path for stale parse output, so this
    guard validates bounded membership and quarantine state without requiring
    the currently bound source revision to still match.  Reportable sync paths
    retain the stricter revision/status validation below.
    """

    if row.id is None:
        raise StaleSourceRevisionError("transaction source row is unpersisted")
    if not row.stitched_group_id:
        if row.stitched_primary:
            raise StaleSourceRevisionError("stitched source primary metadata is incomplete")
        _raise_if_source_mutation_blocked(row)
        return [row]

    expected_ids = _decode_stitched_message_ids(row.stitched_message_ids_json)
    source_rows = session.exec(
        select(DiscordMessage)
        .where(DiscordMessage.stitched_group_id == row.stitched_group_id)
        .order_by(DiscordMessage.created_at, DiscordMessage.id)
        .limit(MAX_TRANSACTION_SOURCE_ROWS + 1)
    ).all()
    if len(source_rows) > MAX_TRANSACTION_SOURCE_ROWS:
        raise StaleSourceRevisionError("stitched source group is oversized")
    ordered_ids = [source_row.id for source_row in source_rows]
    if ordered_ids != expected_ids or row.id not in ordered_ids:
        raise StaleSourceRevisionError("stitched source membership is incomplete")
    primary_rows = [source_row for source_row in source_rows if source_row.stitched_primary]
    if len(primary_rows) != 1 or primary_rows[0].id != ordered_ids[0]:
        raise StaleSourceRevisionError("stitched source group has an ambiguous primary")
    for source_row in source_rows:
        if source_row.stitched_group_id != row.stitched_group_id:
            raise StaleSourceRevisionError("stitched source group ids are inconsistent")
        if _decode_stitched_message_ids(source_row.stitched_message_ids_json) != ordered_ids:
            raise StaleSourceRevisionError("stitched source membership is inconsistent")
        _raise_if_source_mutation_blocked(source_row)
    return source_rows


def _source_mutation_snapshot(source_row: DiscordMessage) -> SourceMutationSnapshot:
    if source_row.id is None:
        raise StaleSourceRevisionError("transaction source row is unpersisted")
    return SourceMutationSnapshot(
        id=source_row.id,
        content=source_row.content,
        attachment_urls_json=source_row.attachment_urls_json,
        current_revision_id=source_row.current_revision_id,
        is_deleted=source_row.is_deleted,
        source_refresh_required=source_row.source_refresh_required,
        last_error=source_row.last_error,
        stitched_group_id=source_row.stitched_group_id,
        stitched_primary=source_row.stitched_primary,
        stitched_message_ids_json=source_row.stitched_message_ids_json,
        active_parse_attempt_id=source_row.active_parse_attempt_id,
    )


def _raise_if_manual_source_claimed(source_row: DiscordMessage) -> None:
    if source_row.active_parse_attempt_id is not None:
        raise StaleSourceRevisionError(
            f"message_id={source_row.id} has active parse attempt "
            f"attempt_id={source_row.active_parse_attempt_id}"
        )


def _capture_source_group_mutation_state(
    session: Session,
    row: DiscordMessage,
) -> tuple[list[DiscordMessage], list[SourceMutationSnapshot]]:
    """Validate and snapshot all current and transaction-bound source rows."""

    current_rows = _load_current_source_group_for_mutation(session, row)
    current_ids = [source_row.id for source_row in current_rows]
    validated_rows = {source_row.id: source_row for source_row in current_rows}
    for source_row in current_rows:
        _raise_if_manual_source_claimed(source_row)

    association_hits = session.exec(
        select(TransactionSourceRevision)
        .where(TransactionSourceRevision.message_id.in_(current_ids))
        .order_by(TransactionSourceRevision.transaction_id, TransactionSourceRevision.id)
        .limit(MAX_TRANSACTION_SOURCE_ROWS + 1)
    ).all()
    if len(association_hits) > MAX_TRANSACTION_SOURCE_ROWS:
        raise StaleSourceRevisionError(
            "transaction association fanout exceeds the safe mutation bound"
        )

    direct_transactions = session.exec(
        select(Transaction)
        .where(Transaction.source_message_id.in_(current_ids))
        .order_by(Transaction.id)
        .limit(MAX_TRANSACTION_SOURCE_ROWS + 1)
    ).all()
    if len(direct_transactions) > MAX_TRANSACTION_SOURCE_ROWS:
        raise StaleSourceRevisionError(
            "direct transaction fanout exceeds the safe mutation bound"
        )

    transaction_ids = {
        association.transaction_id for association in association_hits
    }
    transaction_ids.update(
        transaction.id
        for transaction in direct_transactions
        if transaction.id is not None
    )
    if len(transaction_ids) > MAX_TRANSACTION_SOURCE_ROWS:
        raise StaleSourceRevisionError(
            "combined transaction mutation fanout exceeds the safe bound"
        )

    for transaction_id in sorted(transaction_ids):
        transaction = session.get(Transaction, transaction_id)
        if transaction is None:
            raise StaleSourceRevisionError("bound transaction is missing")
        associations = _load_transaction_source_associations(session, transaction_id)
        if not associations:
            continue
        if [association.source_position for association in associations] != list(
            range(len(associations))
        ):
            raise StaleSourceRevisionError(
                "bound transaction source positions are inconsistent"
            )
        bound_ids = [association.message_id for association in associations]
        if len(set(bound_ids)) != len(bound_ids):
            raise StaleSourceRevisionError(
                "bound transaction source membership contains duplicates"
            )
        primary_association = associations[0]
        if (
            transaction.source_message_id != primary_association.message_id
            or transaction.source_revision_id != primary_association.revision_id
        ):
            raise StaleSourceRevisionError(
                "bound transaction primary source association is inconsistent"
            )
        for association in associations:
            source_row = session.get(DiscordMessage, association.message_id)
            if source_row is None:
                raise StaleSourceRevisionError("bound transaction source message is missing")
            revision = session.get(DiscordMessageRevision, association.revision_id)
            if revision is None or revision.message_id != source_row.id:
                raise StaleSourceRevisionError(
                    "bound transaction source revision association is inconsistent"
                )
            _raise_if_source_mutation_blocked(source_row)
            _raise_if_manual_source_claimed(source_row)
            validated_rows[source_row.id] = source_row
    return current_rows, [
        _source_mutation_snapshot(validated_rows[source_id])
        for source_id in sorted(validated_rows)
    ]


def capture_source_group_mutation_guards(
    session: Session,
    row: DiscordMessage,
) -> list[SourceMutationSnapshot]:
    """Read-only manual preflight with immutable CAS snapshots."""

    _current_rows, guards = _capture_source_group_mutation_state(session, row)
    return guards


def lock_source_group_mutation_guards(
    session: Session,
    guards: list[SourceMutationSnapshot],
) -> list[SourceMutationSnapshot]:
    """Acquire globally ordered source locks only when every snapshot is current."""

    guards_by_id: dict[int, SourceMutationSnapshot] = {}
    for guard in guards:
        existing = guards_by_id.get(guard.id)
        if existing is not None and existing != guard:
            raise StaleSourceRevisionError(
                f"conflicting source mutation snapshots for message_id={guard.id}"
            )
        guards_by_id[guard.id] = guard

    ordered_guards = [guards_by_id[source_id] for source_id in sorted(guards_by_id)]
    with session.no_autoflush:
        for guard in ordered_guards:
            result = session.exec(
                update(DiscordMessage)
                .where(
                    DiscordMessage.id == guard.id,
                    DiscordMessage.content == guard.content,
                    DiscordMessage.attachment_urls_json == guard.attachment_urls_json,
                    DiscordMessage.current_revision_id == guard.current_revision_id,
                    DiscordMessage.is_deleted == guard.is_deleted,
                    DiscordMessage.source_refresh_required
                    == guard.source_refresh_required,
                    DiscordMessage.last_error == guard.last_error,
                    DiscordMessage.stitched_group_id == guard.stitched_group_id,
                    DiscordMessage.stitched_primary == guard.stitched_primary,
                    DiscordMessage.stitched_message_ids_json
                    == guard.stitched_message_ids_json,
                    DiscordMessage.active_parse_attempt_id
                    == guard.active_parse_attempt_id,
                )
                .values(
                    content=guard.content,
                    attachment_urls_json=guard.attachment_urls_json,
                    current_revision_id=guard.current_revision_id,
                    is_deleted=guard.is_deleted,
                    source_refresh_required=guard.source_refresh_required,
                    last_error=guard.last_error,
                    stitched_group_id=guard.stitched_group_id,
                    stitched_primary=guard.stitched_primary,
                    stitched_message_ids_json=guard.stitched_message_ids_json,
                    active_parse_attempt_id=guard.active_parse_attempt_id,
                )
                .execution_options(synchronize_session=False)
            )
            if result.rowcount != 1:
                raise StaleSourceRevisionError(
                    f"transaction source message_id={guard.id} changed during manual mutation"
                )
    return ordered_guards


def require_source_group_mutation_allowed(
    session: Session,
    row: DiscordMessage,
) -> list[DiscordMessage]:
    """Validate and lock every source used by a single manual mutation."""

    current_rows, guards = _capture_source_group_mutation_state(session, row)
    lock_source_group_mutation_guards(session, guards)
    return current_rows


def _resolve_transaction_source_rows(
    session: Session,
    row: DiscordMessage,
    transaction: Transaction | None,
    source_rows: Optional[list[DiscordMessage]],
) -> tuple[list[DiscordMessage], bool]:
    if source_rows is not None:
        if transaction is not None and transaction.id is not None:
            # Explicit parse output may replace provenance, but it must never
            # erase a corrupt/unbounded existing association set.
            _load_transaction_source_associations(session, transaction.id)
        return _validate_complete_source_rows(row, list(source_rows)), False
    if transaction is not None:
        bound_rows = _load_bound_source_rows(session, row, transaction)
        if bound_rows is not None:
            return bound_rows, True
    return _load_current_stitched_source_rows(session, row), False


def _is_current_finalized_stitched_child(
    session: Session,
    row: DiscordMessage,
) -> bool:
    if (
        row.is_deleted
        or row.stitched_primary
        or not row.stitched_group_id
        or normalize_parse_status(
            row.parse_status,
            is_deleted=row.is_deleted,
            needs_review=row.needs_review,
        )
        != PARSE_IGNORED
    ):
        return False

    try:
        child_membership = _decode_stitched_message_ids(row.stitched_message_ids_json)
        primary_rows = session.exec(
            select(DiscordMessage)
            .where(DiscordMessage.stitched_group_id == row.stitched_group_id)
            .where(DiscordMessage.stitched_primary == True)  # noqa: E712
            .order_by(DiscordMessage.created_at, DiscordMessage.id)
            .limit(2)
        ).all()
        if len(primary_rows) != 1:
            return False
        primary = primary_rows[0]
        current_rows = _load_current_stitched_source_rows(session, primary)
        current_ids = [source_row.id for source_row in current_rows]
        if current_ids != child_membership or row.id not in current_ids:
            return False

        transaction = session.exec(
            select(Transaction).where(Transaction.source_message_id == primary.id)
        ).first()
        if transaction is None or transaction.is_deleted:
            return False
        bound_rows = _load_bound_source_rows(session, primary, transaction)
        return (
            bound_rows is not None
            and [source_row.id for source_row in bound_rows] == current_ids
        )
    except (SourceRefreshRequiredError, StaleSourceRevisionError):
        return False


def _combined_source_content(source_rows: list[DiscordMessage]) -> str:
    if len(source_rows) == 1:
        return source_rows[0].content or ""
    return "\n\n".join(
        f"Message {position}: {(source_row.content or '').strip() or '[no text]'}"
        for position, source_row in enumerate(source_rows, start=1)
    )


def sync_transaction_from_message(
    session: Session,
    row: DiscordMessage,
    *,
    source_rows: Optional[list[DiscordMessage]] = None,
    source_content: Optional[str] = None,
) -> Optional[Transaction]:
    reportable = is_transaction_message(row)
    if reportable:
        # This is deliberately the first boundary in the sync path.  Reject
        # unsafe values before a query can autoflush them or a source guard can
        # add state to the session.
        validate_optional_money(row.amount, field_name="amount")
        validate_optional_money(row.money_in, field_name="money in")
        validate_optional_money(row.money_out, field_name="money out")
        validate_optional_confidence(row.confidence, field_name="confidence")

    require_canonical_source_projection(session, row)
    existing = session.exec(
        select(Transaction).where(Transaction.source_message_id == row.id)
    ).first()
    if not reportable:
        tombstone_status = normalize_parse_status(
            row.parse_status,
            is_deleted=row.is_deleted,
            needs_review=row.needs_review,
        )
        if _is_current_finalized_stitched_child(session, row):
            for direct_transaction in _load_direct_transactions_for_message(session, row):
                _tombstone_transaction_projection(
                    session,
                    direct_transaction,
                    reason=(
                        "Unmatched because the source Discord transaction is now a "
                        "finalized stitched child."
                    ),
                    parse_status=tombstone_status,
                )
            return None
        invalidated = invalidate_transactions_for_message(
            session,
            row,
            reason=(
                "Unmatched because the source Discord transaction is no longer importable."
            ),
        )
        for transaction in invalidated:
            transaction.parse_status = tombstone_status
            session.add(transaction)
        return None

    effective_source_rows, preserve_bound_content = _resolve_transaction_source_rows(
        session,
        row,
        existing,
        source_rows,
    )
    for source_row in sorted(effective_source_rows, key=lambda source: source.id or 0):
        _lock_matching_source_projection(session, source_row)
    source_revision = ensure_message_revision(session, row)

    if existing is None:
        transaction = Transaction(
            source_message_id=row.id,
            occurred_at=row.created_at,
        )
    else:
        transaction = existing

    financials = compute_financials(
        parsed_type=row.deal_type,
        parsed_category=row.category,
        amount=row.amount,
        cash_direction=row.cash_direction,
        message_text=row.content or "",
    )
    if financials.requires_review:
        row.parse_status = PARSE_REVIEW_REQUIRED
        row.needs_review = True
        session.add(row)
    if not row.entry_kind:
        row.entry_kind = financials.entry_kind
    if row.money_in is None:
        row.money_in = financials.money_in
    if row.money_out is None:
        row.money_out = financials.money_out
    if not row.expense_category and financials.expense_category:
        row.expense_category = financials.expense_category

    transaction.discord_message_id = row.discord_message_id
    transaction.guild_id = row.guild_id
    transaction.channel_id = row.channel_id
    transaction.channel_name = row.channel_name
    transaction.author_name = row.author_name
    transaction.occurred_at = row.created_at
    transaction.parse_status = row.parse_status
    transaction.deal_type = row.deal_type
    transaction.entry_kind = row.entry_kind
    transaction.payment_method = row.payment_method
    transaction.cash_direction = row.cash_direction
    transaction.category = row.category
    transaction.expense_category = row.expense_category
    normalized_money_in = normalize_money_value(
        validate_optional_money(row.money_in, field_name="money in")
    )
    normalized_money_out = normalize_money_value(
        validate_optional_money(row.money_out, field_name="money out")
    )
    normalized_amount = validate_optional_money(row.amount, field_name="amount")
    if normalized_amount is None:
        inferred_amount = max(normalized_money_in, normalized_money_out)
        normalized_amount = inferred_amount or None

    transaction.amount = normalized_amount
    transaction.money_in = normalized_money_in
    transaction.money_out = normalized_money_out
    transaction.needs_review = row.needs_review
    transaction.confidence = validate_optional_confidence(
        row.confidence,
        field_name="confidence",
    )
    transaction.notes = row.notes
    transaction.trade_summary = row.trade_summary
    if source_content is not None:
        transaction.source_content = source_content
    elif preserve_bound_content and existing is not None:
        transaction.source_content = existing.source_content
    else:
        transaction.source_content = _combined_source_content(effective_source_rows)
    transaction.source_revision_id = source_revision.id
    transaction.is_deleted = False
    transaction.updated_at = utcnow()

    session.add(transaction)
    session.flush()
    if not preserve_bound_content:
        _replace_source_associations(session, transaction, effective_source_rows)

    existing_items = session.exec(
        select(TransactionItem).where(TransactionItem.transaction_id == transaction.id)
    ).all()
    for item in existing_items:
        session.delete(item)
    session.flush()

    item_names = _safe_json_list(row.item_names_json)
    items_in = _safe_json_list(row.items_in_json)
    items_out = _safe_json_list(row.items_out_json)

    for item_name in item_names:
        session.add(
            TransactionItem(transaction_id=transaction.id, direction="named", item_name=item_name)
        )
    for item_name in items_in:
        session.add(
            TransactionItem(transaction_id=transaction.id, direction="in", item_name=item_name)
        )
    for item_name in items_out:
        session.add(
            TransactionItem(transaction_id=transaction.id, direction="out", item_name=item_name)
        )

    return transaction


def rebuild_transactions(session: Session) -> int:
    connection = session.connection()
    if connection.dialect.name == "sqlite":
        driver_connection = connection.connection.driver_connection
        if not driver_connection.in_transaction:
            connection.exec_driver_sql("BEGIN")
    rows = session.exec(
        select(DiscordMessage).order_by(DiscordMessage.created_at, DiscordMessage.id)
    ).all()
    synced = 0
    for original_row in rows:
        row_id = original_row.id
        if row_id is None:
            continue
        row = session.get(DiscordMessage, row_id)
        try:
            affected_transaction_ids = _bounded_transaction_ids_for_message(
                session,
                row,
            )
        except StaleSourceRevisionError as exc:
            row.parse_status = PARSE_FAILED
            row.needs_review = True
            row.source_refresh_required = True
            row.last_error = (
                f"{DISCORD_SOURCE_REFRESH_REQUIRED_ERROR}: "
                f"transaction rebuild blocked: {exc}"
            )
            session.add(row)
            continue
        try:
            with session.begin_nested():
                transaction = sync_transaction_from_message(session, row)
        except (SourceRefreshRequiredError, StaleSourceRevisionError) as exc:
            failed_row = session.get(DiscordMessage, row_id)
            failed_row.parse_status = PARSE_FAILED
            failed_row.needs_review = True
            failed_row.source_refresh_required = True
            failed_row.last_error = (
                f"{DISCORD_SOURCE_REFRESH_REQUIRED_ERROR}: "
                f"transaction rebuild blocked: {exc}"
            )
            session.add(failed_row)
            for transaction_id in affected_transaction_ids:
                affected_transaction = session.get(Transaction, transaction_id)
                if affected_transaction is None:
                    continue
                _tombstone_transaction_projection(
                    session,
                    affected_transaction,
                    reason=f"Transaction rebuild rejected stale source: {exc}",
                    parse_status=PARSE_PENDING,
                )
            continue
        if transaction:
            synced += 1
    session.commit()
    return synced


def _transaction_item_names(session: Session, transaction_id: int) -> list[str]:
    items = session.exec(
        select(TransactionItem).where(TransactionItem.transaction_id == transaction_id)
    ).all()
    names: list[str] = []
    seen: set[str] = set()
    for item in items:
        name = str(item.item_name or "").strip()
        if not name:
            continue
        key = name.casefold()
        if key in seen:
            continue
        seen.add(key)
        names.append(name)
    return names


def build_transaction_reconciliation_snapshot(session: Session, transaction: Transaction) -> dict:
    item_names = _transaction_item_names(session, transaction.id)
    return {
        "source_message_id": transaction.source_message_id,
        "discord_message_id": transaction.discord_message_id,
        "occurred_at": transaction.occurred_at,
        "entry_kind": transaction.entry_kind,
        "deal_type": transaction.deal_type,
        "amount": normalize_money_value(transaction.amount),
        "money_in": normalize_money_value(transaction.money_in),
        "money_out": normalize_money_value(transaction.money_out),
        "payment_method": transaction.payment_method,
        "cash_direction": transaction.cash_direction,
        "category": transaction.category,
        "expense_category": transaction.expense_category,
        "channel_id": transaction.channel_id,
        "channel_name": transaction.channel_name,
        "author_name": transaction.author_name,
        "item_count": len(item_names),
        "item_names": item_names,
    }


def _discord_source_quarantined_sql(message_columns):
    return or_(
        func.coalesce(message_columns.source_refresh_required, False).is_(True),
        func.coalesce(message_columns.last_error, "").contains(
            DISCORD_SOURCE_REFRESH_REQUIRED_ERROR
        ),
    )


def transaction_is_reportable_predicate():
    """Reusable fail-closed SQL predicate for monetary Transaction consumers."""

    direct_message = DiscordMessage.__table__
    direct_revision = DiscordMessageRevision.__table__.alias("report_direct_revision")
    association = TransactionSourceRevision.__table__.alias("report_source_association")
    bound_message = DiscordMessage.__table__.alias("report_bound_message")
    bound_revision = DiscordMessageRevision.__table__.alias("report_bound_revision")
    legacy_group_message = DiscordMessage.__table__.alias("report_legacy_group_message")
    reportable_primary_statuses = sorted(
        expand_parse_status_filter_values([PARSE_PARSED, PARSE_REVIEW_REQUIRED])
    )
    ignored_statuses = sorted(expand_parse_status_filter_values([PARSE_IGNORED]))

    direct_revision_matches = (
        select(direct_revision.c.id)
        .where(direct_revision.c.id == Transaction.source_revision_id)
        .where(direct_revision.c.message_id == Transaction.source_message_id)
        .where(direct_message.c.current_revision_id == direct_revision.c.id)
        .where(direct_message.c.content == direct_revision.c.content)
        .where(
            direct_message.c.attachment_urls_json
            == direct_revision.c.attachment_urls_json
        )
        .exists()
    )

    has_associations = (
        select(association.c.id)
        .where(association.c.transaction_id == Transaction.id)
        .exists()
    )
    association_count = (
        select(func.count(association.c.id))
        .where(association.c.transaction_id == Transaction.id)
        .scalar_subquery()
    )
    association_min_position = (
        select(func.min(association.c.source_position))
        .where(association.c.transaction_id == Transaction.id)
        .scalar_subquery()
    )
    association_max_position = (
        select(func.max(association.c.source_position))
        .where(association.c.transaction_id == Transaction.id)
        .scalar_subquery()
    )
    association_distinct_position_count = (
        select(func.count(func.distinct(association.c.source_position)))
        .where(association.c.transaction_id == Transaction.id)
        .scalar_subquery()
    )
    association_distinct_message_count = (
        select(func.count(func.distinct(association.c.message_id)))
        .where(association.c.transaction_id == Transaction.id)
        .scalar_subquery()
    )
    has_matching_primary_association = (
        select(association.c.id)
        .where(association.c.transaction_id == Transaction.id)
        .where(association.c.source_position == 0)
        .where(association.c.message_id == Transaction.source_message_id)
        .where(association.c.revision_id == Transaction.source_revision_id)
        .exists()
    )
    invalid_bound_source = (
        select(association.c.id)
        .select_from(
            association.outerjoin(
                bound_message,
                association.c.message_id == bound_message.c.id,
            ).outerjoin(
                bound_revision,
                and_(
                    association.c.revision_id == bound_revision.c.id,
                    association.c.message_id == bound_revision.c.message_id,
                ),
            )
        )
        .where(association.c.transaction_id == Transaction.id)
        .where(
            or_(
                bound_message.c.id.is_(None),
                bound_revision.c.id.is_(None),
                bound_message.c.is_deleted.is_(True),
                _discord_source_quarantined_sql(bound_message.c),
                bound_message.c.current_revision_id.is_distinct_from(
                    association.c.revision_id
                ),
                bound_message.c.content.is_distinct_from(bound_revision.c.content),
                bound_message.c.attachment_urls_json.is_distinct_from(
                    bound_revision.c.attachment_urls_json
                ),
                association.c.source_position < 0,
                and_(
                    association.c.source_position == 0,
                    or_(
                        association.c.message_id.is_distinct_from(
                            Transaction.source_message_id
                        ),
                        Transaction.source_revision_id.is_(None),
                        association.c.revision_id.is_distinct_from(
                            Transaction.source_revision_id
                        ),
                        func.coalesce(bound_message.c.parse_status, "").not_in(
                            reportable_primary_statuses
                        ),
                    ),
                ),
                and_(
                    association.c.source_position > 0,
                    func.coalesce(bound_message.c.parse_status, "").not_in(
                        ignored_statuses
                    ),
                ),
            )
        )
        .exists()
    )
    quarantined_legacy_group_source = (
        select(legacy_group_message.c.id)
        .where(direct_message.c.stitched_group_id.is_not(None))
        .where(
            legacy_group_message.c.stitched_group_id
            == direct_message.c.stitched_group_id
        )
        .where(
            or_(
                legacy_group_message.c.is_deleted.is_(True),
                _discord_source_quarantined_sql(legacy_group_message.c),
            )
        )
        .exists()
    )

    return and_(
        Transaction.is_deleted.is_(False),
        Transaction.parse_status.in_(reportable_primary_statuses),
        direct_message.c.id.is_not(None),
        direct_message.c.is_deleted.is_(False),
        func.coalesce(direct_message.c.parse_status, "").in_(
            reportable_primary_statuses
        ),
        ~_discord_source_quarantined_sql(direct_message.c),
        or_(
            direct_message.c.stitched_group_id.is_(None),
            direct_message.c.stitched_primary.is_(True),
        ),
        or_(Transaction.source_revision_id.is_(None), direct_revision_matches),
        or_(
            ~has_associations,
            and_(
                Transaction.source_revision_id.is_not(None),
                association_count.between(1, MAX_TRANSACTION_SOURCE_ROWS),
                association_min_position == 0,
                association_max_position == association_count - 1,
                association_distinct_position_count == association_count,
                association_distinct_message_count == association_count,
                has_matching_primary_association,
                ~invalid_bound_source,
            ),
        ),
        or_(has_associations, ~quarantined_legacy_group_source),
    )


def transaction_base_query(
    *,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    channel_id: Optional[str] = None,
    entry_kind: Optional[str] = None,
):
    stmt = select(Transaction).join(
        DiscordMessage,
        Transaction.source_message_id == DiscordMessage.id,
    )
    stmt = stmt.where(transaction_is_reportable_predicate())

    if start:
        stmt = stmt.where(Transaction.occurred_at >= start)
    if end:
        stmt = stmt.where(Transaction.occurred_at <= end)
    if channel_id:
        stmt = stmt.where(Transaction.channel_id == channel_id)
    if entry_kind:
        stmt = stmt.where(Transaction.entry_kind == entry_kind)

    return stmt.order_by(Transaction.occurred_at, Transaction.id)


def get_transactions(
    session: Session,
    *,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    channel_id: Optional[str] = None,
    entry_kind: Optional[str] = None,
    limit: Optional[int] = None,
) -> list[Transaction]:
    stmt = transaction_base_query(start=start, end=end, channel_id=channel_id, entry_kind=entry_kind)
    if limit is not None:
        stmt = stmt.limit(limit)
    return session.exec(stmt).all()


def build_transaction_summary(rows: list[Transaction]) -> dict:
    totals = defaultdict(float)
    counts = defaultdict(int)
    expense_categories = defaultdict(float)
    channels = defaultdict(float)
    channels_money_in = defaultdict(float)
    channels_money_out = defaultdict(float)
    payment_methods = defaultdict(float)
    categories = defaultdict(float)
    timeline = defaultdict(lambda: defaultdict(float))

    for row in rows:
        money_in = normalize_money_value(row.money_in)
        money_out = normalize_money_value(row.money_out)
        entry_kind = row.entry_kind or "unknown"
        expense_category = (row.expense_category or "").strip().lower()
        day_key = row.occurred_at.date().isoformat()
        net_value = signed_money_delta(money_in, money_out)
        is_non_operating = _is_non_operating_transaction(entry_kind, expense_category)
        reporting_amount = row.amount
        if reporting_amount is None:
            reporting_amount = money_in or money_out or 0.0

        totals["money_in"] += money_in
        totals["money_out"] += money_out
        if is_non_operating:
            totals["non_operating_money_in"] += money_in
            totals["non_operating_money_out"] += money_out
            totals["non_operating_net"] += net_value
        else:
            totals["net"] += net_value
        counts[entry_kind] += 1
        if row.needs_review:
            counts["needs_review"] += 1
        if entry_kind == "unknown":
            counts["unknown"] += 1

        if is_non_operating:
            pass
        elif entry_kind == "sale":
            totals["sales"] += money_in
            timeline[day_key]["sales"] += money_in
        elif entry_kind == "buy":
            totals["buys"] += money_out
            totals["inventory_cash_out"] += money_out
            timeline[day_key]["buys"] += money_out
        elif entry_kind == "expense":
            totals["expenses"] += money_out
            if expense_category == "inventory":
                totals["inventory_expenses"] += money_out
            else:
                totals["operating_expenses"] += money_out
            timeline[day_key]["expenses"] += money_out
        elif entry_kind == "trade":
            totals["trade_cash_in"] += money_in
            totals["trade_cash_out"] += money_out
            totals["inventory_cash_out"] += money_out
            timeline[day_key]["trade_in"] += money_in
            timeline[day_key]["trade_out"] += money_out
        elif money_out:
            totals["operating_expenses"] += money_out

        if row.expense_category and not is_non_operating:
            expense_categories[row.expense_category] += money_out
        if row.category:
            categories[row.category] += normalize_money_value(reporting_amount)
        if row.payment_method:
            payment_methods[row.payment_method] += normalize_money_value(reporting_amount)
        if row.channel_name or row.channel_id:
            channel_key = row.channel_name or row.channel_id or "unknown"
            channels[channel_key] += net_value
            channels_money_in[channel_key] += money_in
            channels_money_out[channel_key] += money_out

    totals["gross_margin"] = totals["sales"] - totals["buys"]
    totals["inventory_spend"] = totals["inventory_cash_out"] + totals["inventory_expenses"]

    return {
        "totals": {key: round(value, 2) for key, value in totals.items()},
        "counts": dict(counts),
        "expense_categories": {
            key: round(value, 2)
            for key, value in sorted(expense_categories.items(), key=lambda item: (-item[1], item[0]))
        },
        "channel_net": {
            key: round(value, 2)
            for key, value in sorted(channels.items(), key=lambda item: (-item[1], item[0]))
        },
        "channel_detail": [
            {
                "channel": key,
                "money_in": round(channels_money_in[key], 2),
                "money_out": round(channels_money_out[key], 2),
                "net": round(channels[key], 2),
            }
            for key in sorted(channels.keys(), key=lambda value: (-channels[value], value))
        ],
        "payment_methods": {
            key: round(value, 2)
            for key, value in sorted(payment_methods.items(), key=lambda item: (-item[1], item[0]))
        },
        "categories": {
            key: round(value, 2)
            for key, value in sorted(categories.items(), key=lambda item: (-item[1], item[0]))
        },
        "timeline": [
            {
                "date": date_key,
                "sales": round(values.get("sales", 0.0), 2),
                "buys": round(values.get("buys", 0.0), 2),
                "expenses": round(values.get("expenses", 0.0), 2),
                "trade_in": round(values.get("trade_in", 0.0), 2),
                "trade_out": round(values.get("trade_out", 0.0), 2),
                "net": round(
                    values.get("sales", 0.0)
                    + values.get("trade_in", 0.0)
                    - values.get("buys", 0.0)
                    - values.get("expenses", 0.0)
                    - values.get("trade_out", 0.0),
                    2,
                ),
            }
            for date_key, values in sorted(timeline.items())
        ],
        "rows": len(rows),
    }
