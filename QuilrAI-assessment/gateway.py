import json
import os
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response


DOWNSTREAM_URL = "http://127.0.0.1:8001/mcp"

# Local assessment tokens, configured when starting the gateway.
ADMIN_TOKEN = os.environ["GATEWAY_ADMIN_TOKEN"]
VIEWER_TOKEN = os.environ["GATEWAY_VIEWER_TOKEN"]

if not ADMIN_TOKEN or not VIEWER_TOKEN or ADMIN_TOKEN == VIEWER_TOKEN:
    raise RuntimeError("Configure two different, nonempty gateway tokens")

TOKEN_ROLES = {
    ADMIN_TOKEN: "admin",
    VIEWER_TOKEN: "viewer",
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with httpx.AsyncClient(timeout=5.0) as client:
        app.state.client = client
        yield


app = FastAPI(lifespan=lifespan)


def rpc_error(request_id, code, message, status_code=200):
    return JSONResponse(
        status_code=status_code,
        content={
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {
                "code": code,
                "message": message,
            },
        },
    )


@app.post("/mcp")
async def gateway(request: Request):
    try:
        payload = await request.json()
    except (json.JSONDecodeError, UnicodeDecodeError):
        return rpc_error(None, -32700, "Parse error")

    # This assessment endpoint supports individual requests with IDs.
    if not isinstance(payload, dict):
        return rpc_error(None, -32600, "Invalid Request")

    request_id = payload.get("id")

    valid_id = (
        isinstance(request_id, (str, int))
        and not isinstance(request_id, bool)
    )

    if (
        payload.get("jsonrpc") != "2.0"
        or not isinstance(payload.get("method"), str)
        or not valid_id
    ):
        return rpc_error(None, -32600, "Invalid Request")

    # Verify the opaque token against a server-controlled mapping.
    authorization = request.headers.get("authorization", "")
    parts = authorization.split()

    if len(parts) != 2 or parts[0].lower() != "bearer":
        return rpc_error(
            request_id, -32001, "Unauthorized", status_code=401
        )

    role = TOKEN_ROLES.get(parts[1])

    if role is None:
        return rpc_error(
            request_id, -32001, "Unauthorized", status_code=401
        )

    method = payload["method"]
    params = payload.get("params", {})

    if not isinstance(params, dict):
        return rpc_error(request_id, -32602, "Invalid params")

    if method == "tools/call":
        tool_name = params.get("name")

        if not isinstance(tool_name, str) or not tool_name:
            return rpc_error(request_id, -32602, "Invalid tool name")

        if tool_name.startswith("admin_") and role != "admin":
            return rpc_error(
                request_id,
                -32001,
                "Unauthorized Tool Call",
            )

    elif method != "tools/list":
        return rpc_error(request_id, -32601, "Method not found")

    # Only authorized requests reach this point.
    try:
        upstream = await request.app.state.client.post(
            DOWNSTREAM_URL,
            content=await request.body(),
            headers={"Content-Type": "application/json"},
        )
    except httpx.TimeoutException:
        return rpc_error(
            request_id,
            -32002,
            "Downstream timeout",
            status_code=504,
        )
    except httpx.RequestError:
        return rpc_error(
            request_id,
            -32002,
            "Downstream unavailable",
            status_code=502,
        )

    return Response(
        content=upstream.content,
        status_code=upstream.status_code,
        headers={
            "Content-Type": upstream.headers.get(
                "content-type", "application/json"
            ),
        },
    )