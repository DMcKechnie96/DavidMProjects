import asyncio
import hashlib
import os
import sqlite3
import uuid
from contextlib import asynccontextmanager
from typing import Literal

import httpx
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from rate_limiter import RateLimitExceeded, TokenRateLimiter


MODEL_BASE_URL = "http://127.0.0.1:8004"
PROVIDER_TIMEOUT_SECONDS = 3.0

PRIMARY_PATHS = {
    "success": "/primary/success",
    "rate-limited": "/primary/rate-limited",
    "timeout": "/primary/timeout",
    "error": "/primary/error",
}

SECONDARY_PATHS = {
    "success": "/secondary/success",
    "error": "/secondary/error",
}


class CompletionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    prompt: str = Field(max_length=10_000)
    max_tokens: int = Field(strict=True, ge=1, le=10_000)

    # Demo controls for testing our local mock endpoints.
    primary_mode: Literal[
        "success", "rate-limited", "timeout", "error"
    ] = "success"

    secondary_mode: Literal["success", "error"] = "success"


class Usage(BaseModel):
    prompt_tokens: int = Field(strict=True, ge=0)
    completion_tokens: int = Field(strict=True, ge=0)
    total_tokens: int = Field(strict=True, ge=0)


class ProviderResult(BaseModel):
    provider: str
    text: str
    usage: Usage


class GatewayFailure(Exception):
    def __init__(self, status: int, code: str, message: str):
        self.status = status
        self.code = code
        self.message = message


class TryBackup(Exception):
    pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    api_key = os.environ.get("ROUTER_API_KEY", "")

    if not api_key:
        raise RuntimeError("ROUTER_API_KEY must be configured")

    # Store a digest in the usage database, not the raw API key.
    app.state.api_key = api_key
    app.state.tenant_id = hashlib.sha256(api_key.encode()).hexdigest()

    app.state.limiter = TokenRateLimiter(
        database_path="gateway_usage.sqlite3",
        limit=50_000,
        window_seconds=60.0,
    )

    async with httpx.AsyncClient(
        timeout=PROVIDER_TIMEOUT_SECONDS
    ) as client:
        app.state.client = client
        yield


app = FastAPI(lifespan=lifespan)


@app.middleware("http")
async def add_request_id(request: Request, call_next):
    request.state.request_id = uuid.uuid4().hex
    return await call_next(request)


def error_response(request, status, code, message):
    return JSONResponse(
        status_code=status,
        content={
            "error": {
                "code": code,
                "message": message,
                "request_id": request.state.request_id,
            }
        },
    )


@app.exception_handler(GatewayFailure)
async def gateway_failure_handler(request, exception):
    return error_response(
        request,
        exception.status,
        exception.code,
        exception.message,
    )


@app.exception_handler(RequestValidationError)
async def validation_failure_handler(request, exception):
    return error_response(
        request,
        400,
        "INVALID_REQUEST",
        "Invalid completion request",
    )


@app.exception_handler(sqlite3.Error)
async def database_failure_handler(request, exception):
    return error_response(
        request,
        503,
        "GATEWAY_UNAVAILABLE",
        "Token accounting is temporarily unavailable",
    )


@app.exception_handler(Exception)
async def unexpected_failure_handler(request, exception):
    return error_response(
        request,
        500,
        "INTERNAL_ERROR",
        "The gateway could not complete the request",
    )


async def call_provider(
    request: Request,
    body: CompletionRequest,
    path: str,
    allow_fallback: bool,
):
    limiter = request.app.state.limiter

    # Matches our mock provider's explicitly defined accounting.
    # Real providers require their own tokenizer and message overhead.
    prompt_budget = len(body.prompt.encode("utf-8"))
    token_budget = prompt_budget + body.max_tokens

    try:
        reservation = await limiter.reserve(
            request.app.state.tenant_id,
            token_budget,
        )
    except RateLimitExceeded:
        raise GatewayFailure(
            429,
            "TOKEN_LIMIT_EXCEEDED",
            "Tenant token budget exceeded",
        ) from None

    try:
        # asyncio.timeout limits the whole HTTP attempt,
        # rather than only individual socket operations.
        async with asyncio.timeout(PROVIDER_TIMEOUT_SECONDS):
            response = await request.app.state.client.post(
                MODEL_BASE_URL + path,
                json={
                    "prompt": body.prompt,
                    "max_tokens": body.max_tokens,
                },
            )

    except (TimeoutError, httpx.TimeoutException):
        # The provider may still have processed the timed-out request.
        await limiter.settle(reservation, token_budget)

        if allow_fallback:
            raise TryBackup() from None

        raise GatewayFailure(
            504,
            "PROVIDER_TIMEOUT",
            "Backup provider timed out",
        ) from None

    except httpx.RequestError:
        # Conservatively retain the full budget for uncertain usage.
        await limiter.settle(reservation, token_budget)

        raise GatewayFailure(
            502,
            "PROVIDER_UNAVAILABLE",
            "Provider unavailable",
        ) from None

    except asyncio.CancelledError:
        # Preserve accounting if the caller disconnects or cancels.
        await asyncio.shield(
            limiter.settle(reservation, token_budget)
        )
        raise

    if response.status_code == 429:
        # Our mock contract guarantees no usage for rejected requests.
        await limiter.settle(reservation, 0)

        if allow_fallback:
            raise TryBackup()

        raise GatewayFailure(
            503,
            "PROVIDER_RATE_LIMITED",
            "Backup provider is rate limited",
        )

    if response.status_code != 200:
        await limiter.settle(reservation, token_budget)

        raise GatewayFailure(
            502,
            "PROVIDER_ERROR",
            "Provider request failed",
        )

    try:
        result = ProviderResult.model_validate(response.json())
        usage = result.usage

        if usage.total_tokens != (
            usage.prompt_tokens + usage.completion_tokens
        ):
            raise ValueError("Inconsistent usage")

        if usage.prompt_tokens != prompt_budget:
            raise ValueError("Unexpected mock input usage")

        if usage.completion_tokens > body.max_tokens:
            raise ValueError("Output exceeds reserved budget")

    except (ValidationError, ValueError):
        await limiter.settle(reservation, token_budget)

        raise GatewayFailure(
            502,
            "INVALID_PROVIDER_RESPONSE",
            "Provider returned an invalid response",
        ) from None

    await limiter.settle(reservation, usage.total_tokens)
    return result


@app.post("/completions")
async def completions(body: CompletionRequest, request: Request):
    authorization = request.headers.get("authorization", "")
    parts = authorization.split()

    if (
        len(parts) != 2
        or parts[0].lower() != "bearer"
        or parts[1] != request.app.state.api_key
    ):
        raise GatewayFailure(
            401,
            "UNAUTHORIZED",
            "Invalid API key",
        )

    used_fallback = False

    try:
        result = await call_provider(
            request,
            body,
            PRIMARY_PATHS[body.primary_mode],
            allow_fallback=True,
        )
    except TryBackup:
        used_fallback = True

        result = await call_provider(
            request,
            body,
            SECONDARY_PATHS[body.secondary_mode],
            allow_fallback=False,
        )

    return {
        "request_id": request.state.request_id,
        "fallback_used": used_fallback,
        **result.model_dump(),
    }