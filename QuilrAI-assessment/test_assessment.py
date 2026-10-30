import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pydantic import ValidationError

from Server import execute_tool
from rate_limiter import RateLimitExceeded, TokenRateLimiter
from stream_guardrail import TokenRedactor


class ToolValidationTests(unittest.TestCase):
    def test_customer_lookup(self):
        result = execute_tool(
            "get_customer_record",
            {"customer_id": "CUST-12345"},
        )

        self.assertEqual(result["customer_id"], "CUST-12345")
        self.assertTrue(result["mock"])

    def test_valid_refund_is_simulated(self):
        result = execute_tool(
            "trigger_refund",
            {
                "customer_id": "CUST-12345",
                "amount": 25.5,
                "reason": "Customer returned the item",
            },
        )

        self.assertEqual(result["status"], "simulated")

    def test_invalid_refund_arguments(self):
        valid = {
            "customer_id": "CUST-12345",
            "amount": 25.5,
            "reason": "Customer returned the item",
        }

        invalid_cases = [
            {"customer_id": "12345"},
            {"customer_id": 12345},
            {"amount": 0},
            {"amount": -1},
            {"amount": float("inf")},
            {"amount": float("nan")},
            {"amount": "25.5"},
            {"amount": True},
            {"reason": "Short"},
            {"unexpected": "field"},
        ]

        for changes in invalid_cases:
            with self.subTest(changes=changes):
                with self.assertRaises(ValidationError):
                    execute_tool(
                        "trigger_refund",
                        {**valid, **changes},
                    )

    def test_missing_required_field(self):
        with self.assertRaises(ValidationError):
            execute_tool("get_customer_record", {})

    def test_unknown_tool(self):
        with self.assertRaises(ValueError):
            execute_tool("unknown_tool", {})


class GuardrailTests(unittest.TestCase):
    CASES = [
        (
            "Email david@example.com; SSN 123-45-6789.",
            "Email [REDACTED]; SSN [REDACTED].",
        ),
        (
            "Card 4111 1111 1111 1111 approved.",
            "Card [REDACTED] approved.",
        ),
        (
            "Card=4111-1111-1111-1111.",
            "Card=[REDACTED].",
        ),
        (
            "Ordinary text stays unchanged.",
            "Ordinary text stays unchanged.",
        ),
    ]

    def test_every_two_chunk_split(self):
        for text, expected in self.CASES:
            for split in range(len(text) + 1):
                with self.subTest(text=text, split=split):
                    redactor = TokenRedactor()

                    result = redactor.feed(text[:split])
                    result += redactor.feed(text[split:])
                    result += redactor.finish()

                    self.assertEqual(result, expected)

    def test_one_character_chunks(self):
        for text, expected in self.CASES:
            with self.subTest(text=text):
                redactor = TokenRedactor()
                output = []

                for character in text:
                    output.append(redactor.feed(character))
                    self.assertLessEqual(
                        redactor.buffered_chars,
                        512,
                    )

                output.append(redactor.finish())
                self.assertEqual("".join(output), expected)

    def test_overflow_and_recovery(self):
        redactor = TokenRedactor(max_buffer_chars=32)

        result = redactor.feed("x" * 1000 + "\nSafe text.")
        result += redactor.finish()

        self.assertEqual(result, "[REDACTED]\nSafe text.")
        self.assertEqual(redactor.buffered_chars, 0)

    def test_safe_text_released_before_finish(self):
        redactor = TokenRedactor()

        self.assertEqual(redactor.feed("Hello! "), "Hello! ")
        self.assertEqual(redactor.feed("david@"), "")
        self.assertEqual(
            redactor.feed("example.com "),
            "[REDACTED] ",
        )


class RateLimiterTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)

        self.database = str(
            Path(self.directory.name) / "usage.sqlite3"
        )

        self.limiter = TokenRateLimiter(
            database_path=self.database,
            limit=100,
            window_seconds=60,
        )

    async def test_concurrent_reservations(self):
        async def reserve():
            try:
                return await self.limiter.reserve("tenant-a", 60)
            except RateLimitExceeded:
                return None

        results = await asyncio.gather(reserve(), reserve())

        self.assertEqual(
            sum(result is not None for result in results),
            1,
        )

    async def test_settlement_and_tenant_isolation(self):
        reservation = await self.limiter.reserve("tenant-a", 100)
        await self.limiter.settle(reservation, 20)

        await self.limiter.reserve("tenant-a", 80)

        with self.assertRaises(RateLimitExceeded):
            await self.limiter.reserve("tenant-a", 1)

        await self.limiter.reserve("tenant-b", 100)

    async def test_persistence(self):
        await self.limiter.reserve("tenant-a", 100)

        restarted = TokenRateLimiter(
            database_path=self.database,
            limit=100,
        )

        with self.assertRaises(RateLimitExceeded):
            await restarted.reserve("tenant-a", 1)

    async def test_sliding_expiration(self):
        with patch("rate_limiter.time.time", return_value=1000) as clock:
            first = await self.limiter.reserve("tenant-a", 60)
            await self.limiter.settle(first, 60)

            clock.return_value = 1030
            second = await self.limiter.reserve("tenant-a", 40)
            await self.limiter.settle(second, 40)

            clock.return_value = 1059.999
            with self.assertRaises(RateLimitExceeded):
                await self.limiter.reserve("tenant-a", 1)

            clock.return_value = 1060
            await self.limiter.reserve("tenant-a", 60)

            with self.assertRaises(RateLimitExceeded):
                await self.limiter.reserve("tenant-a", 1)

            clock.return_value = 1090
            await self.limiter.reserve("tenant-a", 40)

            clock.return_value = 1200
            with self.assertRaises(RateLimitExceeded):
                await self.limiter.reserve("tenant-a", 1)

    async def test_settlement_is_idempotent(self):
        reservation = await self.limiter.reserve("tenant-a", 100)

        await self.limiter.settle(reservation, 20)
        await self.limiter.settle(reservation, 20)

        with self.assertRaises(ValueError):
            await self.limiter.settle(reservation, 10)


if __name__ == "__main__":
    unittest.main()