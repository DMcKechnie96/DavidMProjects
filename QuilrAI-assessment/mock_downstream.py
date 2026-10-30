from fastapi import FastAPI, Request


app = FastAPI()


@app.post("/mcp")
async def handle_mcp(request: Request):
    payload = await request.json()

    request_id = payload.get("id")
    method = payload.get("method")

    if method == "tools/list":
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "tools": [
                    {
                        "name": "get_customer_record",
                        "description": "Return a demo customer.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "customer_id": {"type": "string"},
                            },
                            "required": ["customer_id"],
                            "additionalProperties": False,
                        },
                    },
                    {
                        "name": "admin_reset_key",
                        "description": "Simulate an admin key reset.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {},
                            "additionalProperties": False,
                        },
                    },
                ]
            },
        }

    if method == "tools/call":
        name = payload.get("params", {}).get("name")

        if name in {"get_customer_record", "admin_reset_key"}:
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "content": [
                        {
                            "type": "text",
                            "text": f"Mock tool executed: {name}",
                        }
                    ],
                    "isError": False,
                },
            }

        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {
                "code": -32602,
                "message": "Unknown tool",
            },
        }

    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "error": {
            "code": -32601,
            "message": "Method not found",
        },
    }