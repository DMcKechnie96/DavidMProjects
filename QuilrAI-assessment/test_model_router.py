import asyncio
import json
import sqlite3
import tempfile
import unittest
from contextlib import ExitStack, asynccontextmanager
from pathlib import Path
from unittest.mock import patch

import httpx

import model_router
from rate_limiter import TokenRateLimiter


class ModelRouterTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)

        self.database = str(
            Path(self.directory.name) / "router.sqlite3"
        )

        self.limiter = TokenRateLimiter(
            database_path=self.database,
            limit=1000,
        )

        self.calls = []

    @asynccontextmanager
    async def client_for(self, provider):
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(provider)
        ) as upstream:
            with ExitStack() as stack:
                values = {
                    "client": upstream,
                    "limiter": self.limiter,
                    "api_key": "test-key",
                    "tenant_id": "test-tenant",
                }

                for name, value in values.items():
                    stack.enter_context(
                        patch.object(
                            model_router.app.state,
                            name,
                            value,
                            create=True,
                        )
                    )

                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(
                        app=model_router.app
                    ),
                    base_url="http://test",
                ) as client:
                    yield client

    def success(self, request, provider):
        payload = json.loads(request.content)
        prompt_tokens = len(payload["prompt"].encode("utf-8"))
        completion_tokens = min(payload["max_tokens"], 8)

        return httpx.Response(
            200,
            json={
                "provider": provider,
                "text": f"Demo response from {provider}.",
                "usage": {
                    "prompt_tokens": prompt_tokens,
                    "completion_tokens": completion_tokens,
                    "total_tokens": (
                        prompt_tokens + completion_tokens
                    ),
                },
            },
        )

    async def complete(self, client, **changes):
        return await client.post(
            "/completions",
            headers={"Authorization": "Bearer test-key"},
            json={
                "prompt": "Hello",
                "max_tokens": 100,
                **changes,
            },
        )

    def charged_tokens(self):
        connection = sqlite3.connect(self.database)

        try:
            return connection.execute(
                """
                SELECT COALESCE(SUM(tokens), 0)
                FROM token_reservations
                WHERE tenant_id = ?
                """,
                ("test-tenant",),
            ).fetchone()[0]
        finally:
            connection.close()

    async def test_primary_success(self):
        def provider(request):
            self.calls.append(request.url.path)
            return self.success(request, "primary")

        async with self.client_for(provider) as client:
            response = await self.complete(client)

        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.json()["fallback_used"])
        self.assertEqual(response.json()["provider"], "primary")
        self.assertEqual(self.calls, ["/primary/success"])
        self.assertEqual(self.charged_tokens(), 13)

    async def test_429_falls_back(self):
        def provider(request):
            self.calls.append(request.url.path)

            if request.url.path.startswith("/primary/"):
                return httpx.Response(429, json={"error": "limited"})

            return self.success(request, "secondary")

        async with self.client_for(provider) as client:
            response = await self.complete(
                client,
                primary_mode="rate-limited",
            )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["fallback_used"])
        self.assertEqual(response.json()["provider"], "secondary")
        self.assertEqual(
            self.calls,
            ["/primary/rate-limited", "/secondary/success"],
        )
        self.assertEqual(self.charged_tokens(), 13)

    async def test_timeout_falls_back_and_retains_charge(self):
        async def provider(request):
            self.calls.append(request.url.path)

            if request.url.path.startswith("/primary/"):
                await asyncio.sleep(0.1)

            return self.success(request, "secondary")

        # Shorten only the automated test deadline.
        with patch.object(
            model_router,
            "PROVIDER_TIMEOUT_SECONDS",
            0.01,
        ):
            async with self.client_for(provider) as client:
                response = await self.complete(
                    client,
                    primary_mode="timeout",
                )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["fallback_used"])
        self.assertEqual(
            self.calls,
            ["/primary/timeout", "/secondary/success"],
        )

        # 105 reserved for the uncertain primary, 13 for backup.
        self.assertEqual(self.charged_tokens(), 118)

    async def test_primary_error_is_sanitized_without_fallback(self):
        def provider(request):
            self.calls.append(request.url.path)
            return httpx.Response(
                500,
                json={"error": "SECRET primary stack trace"},
            )

        async with self.client_for(provider) as client:
            response = await self.complete(
                client,
                primary_mode="error",
            )

        self.assertEqual(response.status_code, 502)
        self.assertEqual(
            response.json()["error"]["code"],
            "PROVIDER_ERROR",
        )
        self.assertTrue(response.json()["error"]["request_id"])
        self.assertNotIn("SECRET", response.text)
        self.assertEqual(self.calls, ["/primary/error"])

    async def test_backup_error_is_sanitized(self):
        def provider(request):
            self.calls.append(request.url.path)

            if request.url.path.startswith("/primary/"):
                return httpx.Response(429, json={"error": "limited"})

            return httpx.Response(
                500,
                json={"error": "SECRET backup stack trace"},
            )

        async with self.client_for(provider) as client:
            response = await self.complete(
                client,
                primary_mode="rate-limited",
                secondary_mode="error",
            )

        self.assertEqual(response.status_code, 502)
        self.assertEqual(
            response.json()["error"]["code"],
            "PROVIDER_ERROR",
        )
        self.assertNotIn("SECRET", response.text)
        self.assertEqual(
            self.calls,
            ["/primary/rate-limited", "/secondary/error"],
        )

    async def test_over_budget_request_never_reaches_provider(self):
        def provider(request):
            self.calls.append(request.url.path)
            return self.success(request, "primary")

        async with self.client_for(provider) as client:
            # Input budget 5 + output budget 1000 exceeds limit 1000.
            response = await self.complete(
                client,
                max_tokens=1000,
            )

        self.assertEqual(response.status_code, 429)
        self.assertEqual(
            response.json()["error"]["code"],
            "TOKEN_LIMIT_EXCEEDED",
        )
        self.assertEqual(self.calls, [])

    async def test_invalid_key_never_reaches_provider(self):
        def provider(request):
            self.calls.append(request.url.path)
            return self.success(request, "primary")

        async with self.client_for(provider) as client:
            response = await client.post(
                "/completions",
                headers={"Authorization": "Bearer wrong-key"},
                json={"prompt": "Hello", "max_tokens": 100},
            )

        self.assertEqual(response.status_code, 401)
        self.assertEqual(
            response.json()["error"]["code"],
            "UNAUTHORIZED",
        )
        self.assertEqual(self.calls, [])


if __name__ == "__main__":
    unittest.main()