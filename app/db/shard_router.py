"""Shard routing: shard = md5(account_id) mod N. Pure function, no lookup table."""
from __future__ import annotations

import hashlib
from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session, sessionmaker

from app.config import get_settings
from app.db.models import Base


def shard_index_for(account_id: str, shard_count: int) -> int:
    digest = hashlib.md5(account_id.encode("utf-8")).hexdigest()  # speed/uniformity, not security
    return int(digest, 16) % shard_count


class ShardRegistry:
    def __init__(self, shard_urls: list[str]):
        if not shard_urls:
            raise ValueError("At least one shard URL is required")
        self.shard_urls = shard_urls
        self._engines = []
        self._factories = []
        for url in shard_urls:
            if url.startswith("sqlite"):
                engine = create_engine(
                    url, future=True,
                    connect_args={"check_same_thread": False, "timeout": 30},
                )

                @event.listens_for(engine, "connect")
                def _sqlite_pragmas(dbapi_conn, _record):
                    cur = dbapi_conn.cursor()
                    cur.execute("PRAGMA journal_mode=WAL")
                    cur.execute("PRAGMA synchronous=NORMAL")
                    cur.close()
            else:
                engine = create_engine(url, future=True, pool_pre_ping=True)
            self._engines.append(engine)
            self._factories.append(sessionmaker(bind=engine, expire_on_commit=False, future=True))

    @property
    def shard_count(self) -> int:
        return len(self._engines)

    def create_all(self) -> None:
        for engine in self._engines:
            Base.metadata.create_all(engine)

    def shard_for_account(self, account_id: str) -> int:
        return shard_index_for(account_id, self.shard_count)

    @contextmanager
    def session_for_shard(self, shard_index: int) -> Iterator[Session]:
        session = self._factories[shard_index]()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def session_for_account(self, account_id: str):
        return self.session_for_shard(self.shard_for_account(account_id))

    def dispose(self) -> None:
        for engine in self._engines:
            engine.dispose()


_registry: ShardRegistry | None = None


def get_shard_registry() -> ShardRegistry:
    global _registry
    if _registry is None:
        _registry = ShardRegistry(get_settings().shard_url_list)
    return _registry
