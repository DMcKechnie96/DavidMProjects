import asyncio

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field


app = FastAPI()


class CompletionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    prompt: str = Field(max_length=10_000)
    max_tokens: int = Field(strict=True, ge=1, le=10_000)


def mock_completion(provider: str, request: CompletionRequest):
    # Explicit mock accounting shared with our upcoming router.
    # UTF-8 bytes are a conservative demo input budget,
    # not an actual model tokenizer.
    prompt_tokens = len(request.prompt.encode("utf-8"))
    completion_tokens = min(request.max_tokens, 8)

    return {
        "provider": provider,
        "text": f"Demo response from {provider}.",
        "usage": {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": prompt_tokens + completion_tokens,
        },
    }


@app.post("/primary/success")
async def primary_success(request: CompletionRequest):
    return mock_completion("primary", request)


@app.post("/primary/rate-limited")
async def primary_rate_limited(request: CompletionRequest):
    return JSONResponse(
        status_code=429,
        content={"error": "Mock primary rate limit"},
    )


@app.post("/primary/timeout")
async def primary_timeout(request: CompletionRequest):
    await asyncio.sleep(4.0)
    return mock_completion("primary", request)


@app.post("/primary/error")
async def primary_error(request: CompletionRequest):
    return JSONResponse(
        status_code=500,
        content={"error": "Internal mock detail: do not expose"},
    )


@app.post("/secondary/success")
async def secondary_success(request: CompletionRequest):
    return mock_completion("secondary", request)


@app.post("/secondary/error")
async def secondary_error(request: CompletionRequest):
    return JSONResponse(
        status_code=500,
        content={"error": "Internal backup detail: do not expose"},
    )