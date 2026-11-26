"""Transactional outbox helpers (rationale: see OutboxEvent in app/db/models.py)."""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from app.db.models import OutboxEvent


def enqueue(session: Session, *, aggregate_id: str, event_type: str, payload: dict) -> str:
    """Write an outbox row in the CALLER's transaction, so it commits (or rolls
    back) atomically with the business change that caused it."""
    event_id = str(uuid.uuid4())
    body = {"event_id": event_id, "event_type": event_type, **payload}
    session.add(
        OutboxEvent(
            id=event_id,
            aggregate_id=aggregate_id,
            event_type=event_type,
            payload_json=json.dumps(body),
            published=0,
        )
    )
    return event_id


def fetch_unpublished(session: Session, limit: int = 100) -> list[tuple[str, str, str]]:
    """Oldest-first batch of (id, aggregate_id, payload_json)."""
    rows = session.execute(
        select(OutboxEvent.id, OutboxEvent.aggregate_id, OutboxEvent.payload_json)
        .where(OutboxEvent.published == 0)
        .order_by(OutboxEvent.created_at, OutboxEvent.id)
        .limit(limit)
    ).all()
    return [(r.id, r.aggregate_id, r.payload_json) for r in rows]


def mark_published(session: Session, event_ids: list[str]) -> None:
    if not event_ids:
        return
    session.execute(
        update(OutboxEvent)
        .where(OutboxEvent.id.in_(event_ids))
        .values(published=1, published_at=datetime.now(timezone.utc))
    )
