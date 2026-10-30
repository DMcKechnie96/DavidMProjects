import json
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse

from stream_guardrail import TokenRedactor


PROVIDER_URL = "http://127.0.0.1:8002/v1/chat/completions"
MAX_EVENT_BYTES = 65536


@asynccontextmanager
async def lifespan(app: FastAPI):
    timeout = httpx.Timeout(15.0, connect=5.0)

    async with httpx.AsyncClient(timeout=timeout) as client:
        app.state.client = client
        yield


app = FastAPI(lifespan=lifespan)


def sse_event(payload: dict) -> str:
    return f"data: {json.dumps(payload)}\n\n"


async def read_sse_data(response):
    """Read SSE events with bounded line and event buffers."""
    pending = bytearray()
    data_lines = []
    event_bytes = 0

    async for block in response.aiter_bytes():
        fragments = block.split(b"\n")

        for index, fragment in enumerate(fragments):
            if len(pending) + len(fragment) > MAX_EVENT_BYTES:
                raise ValueError("Provider line too large")

            pending.extend(fragment)

            if index == len(fragments) - 1:
                continue

            line = bytes(pending).rstrip(b"\r")
            pending.clear()

            event_bytes += len(line) + 1

            if event_bytes > MAX_EVENT_BYTES:
                raise ValueError("Provider event too large")

            if not line:
                if data_lines:
                    yield "\n".join(data_lines)

                data_lines = []
                event_bytes = 0
                continue

            if line.startswith(b":"):
                continue

            if line.startswith(b"data:"):
                value = line[5:]

                if value.startswith(b" "):
                    value = value[1:]

                data_lines.append(value.decode("utf-8"))

    if pending or data_lines:
        raise ValueError("Incomplete provider event")


@app.post("/v1/chat/completions")
async def completions(payload: dict):
    # Support one text completion per request.
    if payload.get("n", 1) != 1:
        raise HTTPException(
            status_code=400,
            detail="Only one completion is supported",
        )

    provider_payload = {
        **payload,
        "stream": True,
        "n": 1,
    }

    request = app.state.client.build_request(
        "POST",
        PROVIDER_URL,
        json=provider_payload,
    )

    try:
        upstream = await app.state.client.send(
            request,
            stream=True,
        )
    except httpx.RequestError:
        raise HTTPException(
            status_code=502,
            detail="Provider unavailable",
        ) from None

    if upstream.status_code != 200:
        await upstream.aclose()
        raise HTTPException(
            status_code=502,
            detail="Provider request failed",
        )

    content_type = upstream.headers.get("content-type", "")

    if not content_type.startswith("text/event-stream"):
        await upstream.aclose()
        raise HTTPException(
            status_code=502,
            detail="Invalid provider response",
        )

    async def generate():
        redactor = TokenRedactor()

        def content_event(text):
            return sse_event({
                "choices": [
                    {
                        "index": 0,
                        "delta": {"content": text},
                        "finish_reason": None,
                    }
                ]
            })

        try:
            async for data in read_sse_data(upstream):
                if data == "[DONE]":
                    remaining = redactor.finish()

                    if remaining:
                        yield content_event(remaining)

                    yield sse_event({
                        "choices": [
                            {
                                "index": 0,
                                "delta": {},
                                "finish_reason": "stop",
                            }
                        ]
                    })

                    yield "data: [DONE]\n\n"
                    return

                event = json.loads(data)

                if not isinstance(event, dict):
                    raise ValueError("Invalid provider event")

                if "error" in event:
                    raise ValueError("Provider stream failed")

                choices = event.get("choices", [])

                if not isinstance(choices, list):
                    raise ValueError("Invalid provider choices")

                if not choices:
                    continue

                if len(choices) != 1:
                    raise ValueError("Multiple choices unsupported")

                choice = choices[0]

                if not isinstance(choice, dict):
                    raise ValueError("Invalid provider choice")

                if choice.get("index", 0) != 0:
                    raise ValueError("Unexpected choice index")

                delta = choice.get("delta", {})

                if not isinstance(delta, dict):
                    raise ValueError("Invalid provider delta")

                if delta.get("tool_calls") or delta.get("function_call"):
                    raise ValueError("Only text output is supported")

                text = delta.get("content")

                if text is None:
                    continue

                if not isinstance(text, str):
                    raise ValueError("Invalid provider content")

                safe_text = redactor.feed(text)

                if safe_text:
                    yield content_event(safe_text)

            raise ValueError("Provider ended without DONE")

        except (httpx.HTTPError, ValueError):
            # Never forward raw errors or unflushed pending text.
            yield sse_event({
                "error": {
                    "code": "UPSTREAM_STREAM_ERROR",
                    "message": "Generation stream failed",
                }
            })

        finally:
            await upstream.aclose()

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )