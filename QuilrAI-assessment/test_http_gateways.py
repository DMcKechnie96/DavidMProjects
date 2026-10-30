import json
import os
import unittest
from contextlib import asynccontextmanager
from unittest.mock import patch

import httpx

# gateway.py reads these variables when imported.
with patch.dict(
    os.environ,
    {
        "GATEWAY_ADMIN_TOKEN": "test-admin-token",
        "GATEWAY_VIEWER_TOKEN": "test-viewer-token",
    },
):
    import gateway

import llm_gateway


@asynccontextmanager
async def test_client(module, upstream_handler):
    """Replace outbound HTTP with an in-memory mock."""
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(upstream_handler)
    ) as upstream:
        with patch.object(
            module.app.state,
            "client",
            upstream,
            create=True,
        ):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=module.app),
                base_url="http://test",
            ) as client:
                yield client


class AuthorizationTests(unittest.IsolatedAsyncioTestCase):
    async def test_viewer_admin_call_never_reaches_downstream(self):
        calls = []

        def downstream(request):
            calls.append(request)
            return httpx.Response(200, json={})

        async with test_client(gateway, downstream) as client:
            response = await client.post(
                "/mcp",
                headers={
                    "Authorization": f"Bearer {gateway.VIEWER_TOKEN}",
                },
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "tools/call",
                    "params": {
                        "name": "admin_reset_key",
                        "arguments": {},
                    },
                },
            )

        self.assertEqual(
            response.json()["error"],
            {
                "code": -32001,
                "message": "Unauthorized Tool Call",
            },
        )
        self.assertEqual(calls, [])

    async def test_allowed_requests_are_forwarded_unchanged(self):
        received = []
        reply = b'{"jsonrpc":"2.0","id":2,"result":{"ok":true}}'

        def downstream(request):
            received.append(json.loads(request.content))
            return httpx.Response(
                200,
                content=reply,
                headers={"Content-Type": "application/json"},
            )

        cases = [
            (
                gateway.VIEWER_TOKEN,
                {
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "tools/list",
                    "params": {},
                },
            ),
            (
                gateway.VIEWER_TOKEN,
                {
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "tools/call",
                    "params": {
                        "name": "get_customer_record",
                        "arguments": {"customer_id": "CUST-12345"},
                    },
                },
            ),
            (
                gateway.ADMIN_TOKEN,
                {
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "tools/call",
                    "params": {
                        "name": "admin_reset_key",
                        "arguments": {},
                    },
                },
            ),
        ]

        async with test_client(gateway, downstream) as client:
            for token, payload in cases:
                response = await client.post(
                    "/mcp",
                    headers={"Authorization": f"Bearer {token}"},
                    json=payload,
                )

                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.content, reply)
                self.assertEqual(received[-1], payload)

        self.assertEqual(len(received), 3)

    async def test_missing_and_unknown_tokens_are_blocked(self):
        calls = []

        def downstream(request):
            calls.append(request)
            return httpx.Response(200, json={})

        async with test_client(gateway, downstream) as client:
            for headers in [
                {},
                {"Authorization": "Bearer unknown-token"},
            ]:
                response = await client.post(
                    "/mcp",
                    headers=headers,
                    json={
                        "jsonrpc": "2.0",
                        "id": 3,
                        "method": "tools/list",
                        "params": {},
                    },
                )

                self.assertEqual(response.status_code, 401)
                self.assertEqual(
                    response.json()["error"]["code"],
                    -32001,
                )

        self.assertEqual(calls, [])


class StreamingGatewayTests(unittest.IsolatedAsyncioTestCase):
    async def test_redacts_provider_deltas(self):
        chunks = [
            "Email david@",
            "example.com; SSN 123-",
            "45-6789; card 4111 ",
            "1111 1111 ",
            "1111.",
        ]

        events = "".join(
            "data: "
            + json.dumps({
                "choices": [
                    {
                        "index": 0,
                        "delta": {"content": chunk},
                    }
                ]
            })
            + "\n\n"
            for chunk in chunks
        )
        events += "data: [DONE]\n\n"

        def provider(request):
            self.assertTrue(json.loads(request.content)["stream"])
            return httpx.Response(
                200,
                content=events.encode(),
                headers={"Content-Type": "text/event-stream"},
            )

        async with test_client(llm_gateway, provider) as client:
            response = await client.post(
                "/v1/chat/completions",
                json={"model": "mock-model", "stream": True},
            )

        self.assertEqual(response.status_code, 200)

        pieces = []
        done = False

        for line in response.text.splitlines():
            if not line.startswith("data: "):
                continue

            data = line[6:]

            if data == "[DONE]":
                done = True
                continue

            event = json.loads(data)
            self.assertNotIn("error", event)

            for choice in event.get("choices", []):
                pieces.append(
                    choice.get("delta", {}).get("content", "")
                )

        self.assertTrue(done)
        self.assertEqual(
            "".join(pieces),
            "Email [REDACTED]; SSN [REDACTED]; card [REDACTED].",
        )

    async def test_connection_error_is_sanitized(self):
        def provider(request):
            raise httpx.ConnectError(
                "SECRET internal connection detail",
                request=request,
            )

        async with test_client(llm_gateway, provider) as client:
            response = await client.post(
                "/v1/chat/completions",
                json={"stream": True},
            )

        self.assertEqual(response.status_code, 502)
        self.assertEqual(
            response.json(),
            {"detail": "Provider unavailable"},
        )
        self.assertNotIn("SECRET", response.text)

    async def test_invalid_stream_event_is_sanitized(self):
        def provider(request):
            return httpx.Response(
                200,
                content=b"data: not-valid-json-SECRET\n\n",
                headers={"Content-Type": "text/event-stream"},
            )

        async with test_client(llm_gateway, provider) as client:
            response = await client.post(
                "/v1/chat/completions",
                json={"stream": True},
            )

        self.assertEqual(response.status_code, 200)
        self.assertIn("UPSTREAM_STREAM_ERROR", response.text)
        self.assertNotIn("SECRET", response.text)
        self.assertNotIn("[DONE]", response.text)


if __name__ == "__main__":
    unittest.main()