import asyncio
import sqlite3
import time
import uuid
from pathlib import Path


class RateLimitExceeded(Exception):
    def __init__(self, limit: int, used: int, requested: int):
        super().__init__("Tenant token limit exceeded")
        self.limit = limit
        self.used = used
        self.requested = requested


class TokenRateLimiter:
    def __init__(
        self,
        database_path: str = "gateway_usage.sqlite3",
        limit: int = 50_000,
        window_seconds: float = 60.0,
    ):
        if isinstance(limit, bool) or not isinstance(limit, int):
            raise ValueError("limit must be an integer")

        if limit <= 0 or window_seconds <= 0:
            raise ValueError("Limit and window must be positive")

        self.database_path = str(Path(database_path).resolve())
        self.limit = limit
        self.window_seconds = window_seconds

        self._initialize()

    def _connect(self):
        connection = sqlite3.connect(
            self.database_path,
            timeout=5.0,
            isolation_level=None,
        )

        connection.execute("PRAGMA busy_timeout = 5000")
        return connection

    def _initialize(self):
        connection = self._connect()

        try:
            connection.execute("PRAGMA journal_mode = WAL")

            connection.execute("""
                CREATE TABLE IF NOT EXISTS token_reservations (
                    reservation_id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    tokens INTEGER NOT NULL CHECK (tokens >= 0),
                    state TEXT NOT NULL
                        CHECK (state IN ('pending', 'settled')),
                    charged_at REAL NOT NULL
                )
            """)

            connection.execute("""
                CREATE INDEX IF NOT EXISTS token_reservations_tenant
                ON token_reservations (tenant_id, state, charged_at)
            """)

        finally:
            connection.close()

    async def reserve(self, tenant_id: str, tokens: int) -> str:
        if not isinstance(tenant_id, str) or not tenant_id.strip():
            raise ValueError("tenant_id must be a nonempty string")

        if (
            isinstance(tokens, bool)
            or not isinstance(tokens, int)
            or tokens <= 0
        ):
            raise ValueError("tokens must be a positive integer")

        return await asyncio.to_thread(
            self._reserve,
            tenant_id,
            tokens,
        )

    def _reserve(self, tenant_id: str, tokens: int) -> str:
        connection = self._connect()

        try:
            # Acquire SQLite's write lock before checking the budget.
            connection.execute("BEGIN IMMEDIATE")

            now = time.time()
            cutoff = now - self.window_seconds

            # Active requests keep their reservations.
            # Only completed usage ages out of the sliding window.
            connection.execute(
                """
                DELETE FROM token_reservations
                WHERE state = 'settled' AND charged_at <= ?
                """,
                (cutoff,),
            )

            used = connection.execute(
                """
                SELECT COALESCE(SUM(tokens), 0)
                FROM token_reservations
                WHERE tenant_id = ?
                """,
                (tenant_id,),
            ).fetchone()[0]

            if used + tokens > self.limit:
                raise RateLimitExceeded(self.limit, used, tokens)

            reservation_id = uuid.uuid4().hex

            connection.execute(
                """
                INSERT INTO token_reservations (
                    reservation_id,
                    tenant_id,
                    tokens,
                    state,
                    charged_at
                )
                VALUES (?, ?, ?, 'pending', ?)
                """,
                (reservation_id, tenant_id, tokens, now),
            )

            connection.commit()
            return reservation_id

        except BaseException:
            connection.rollback()
            raise

        finally:
            connection.close()

    async def settle(
        self,
        reservation_id: str,
        actual_tokens: int,
    ) -> None:
        if (
            isinstance(actual_tokens, bool)
            or not isinstance(actual_tokens, int)
            or actual_tokens < 0
        ):
            raise ValueError("actual_tokens must be a nonnegative integer")

        await asyncio.to_thread(
            self._settle,
            reservation_id,
            actual_tokens,
        )

    def _settle(self, reservation_id: str, actual_tokens: int):
        connection = self._connect()

        try:
            connection.execute("BEGIN IMMEDIATE")

            row = connection.execute(
                """
                SELECT tokens, state
                FROM token_reservations
                WHERE reservation_id = ?
                """,
                (reservation_id,),
            ).fetchone()

            if row is None:
                raise ValueError("Unknown reservation")

            reserved_tokens, state = row

            if state == "settled":
                if actual_tokens != reserved_tokens:
                    raise ValueError("Reservation already settled differently")

                connection.commit()
                return

            # Keep the original reservation if usage exceeds its budget.
            # The router must reserve an adequate upper bound.
            if actual_tokens > reserved_tokens:
                raise ValueError("Actual usage exceeds reserved budget")

            connection.execute(
                """
                UPDATE token_reservations
                SET tokens = ?, state = 'settled', charged_at = ?
                WHERE reservation_id = ?
                """,
                (actual_tokens, time.time(), reservation_id),
            )

            connection.commit()

        except BaseException:
            connection.rollback()
            raise

        finally:
            connection.close()