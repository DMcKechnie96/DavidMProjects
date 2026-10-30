import asyncio
import json

from fastapi import FastAPI
from fastapi.responses import StreamingResponse


app = FastAPI()


@app.post("/v1/chat/completions")
async def completions():
    async def generate():
        # Deliberately split sensitive data across stream chunks.
        chunks = [
            "Hello! ",
            "Contact david@",
            "example.com for help. ",
            "SSN: 123-",
            "45-6789. ",
            "Card: 4111 ",
            "1111 11",
            "11 1111. ",
            "Thanks!",
        ]

        for chunk in chunks:
            event = {
                "choices": [
                    {
                        "index": 0,
                        "delta": {"content": chunk},
                        "finish_reason": None,
                    }
                ]
            }

            yield f"data: {json.dumps(event)}\n\n"
            await asyncio.sleep(0.05)

        yield "data: [DONE]\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
    )