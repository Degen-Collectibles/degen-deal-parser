from sqlalchemy import update
from sqlmodel import Session
from ..models import SmsSuppression, SmsOutbox, utcnow

def suppress(session: Session, fingerprint: str, blocked: bool):
    from sqlalchemy.dialects.sqlite import insert as sqlite_insert
    from sqlalchemy.dialects.postgresql import insert as pg_insert
    insert = pg_insert if session.get_bind().dialect.name == "postgresql" else sqlite_insert
    now = utcnow()
    if blocked:
        statement = insert(SmsSuppression).values(phone_fingerprint=fingerprint, blocked=True, stopped_at=now)
        session.exec(statement.on_conflict_do_update(index_elements=["phone_fingerprint"],
            set_={"blocked": True, "stopped_at": now}))
        session.exec(update(SmsOutbox).where(SmsOutbox.phone_fingerprint == fingerprint,
            SmsOutbox.status.in_(("queued", "retry"))).values(status="cancelled", error_code="recipient_stopped", updated_at=now))
    else:
        # Never create consent. A new app opt-in must postdate stopped_at.
        session.exec(update(SmsSuppression).where(SmsSuppression.phone_fingerprint == fingerprint).values(blocked=False))


