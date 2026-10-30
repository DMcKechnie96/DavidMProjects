import asyncio
import json
import logging
import sys
from typing import Annotated

import anyio
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from mcp.server.lowlevel import Server
from mcp.server.stdio import stdio_server
from mcp.shared.exceptions import MCPError
from mcp.shared.message import SessionMessage
from mcp.types import (
    CallToolRequestParams,
    CallToolResult,
    ErrorData,
    JSONRPCError,
    ListToolsResult,
    PaginatedRequestParams,
    TextContent,
    Tool,
)


# stdout is reserved for MCP messages. Logs go to stderr.
logging.basicConfig(
    level=logging.INFO,
    stream=sys.stderr,
    format="%(levelname)s: %(message)s",
)

logger = logging.getLogger(__name__)


# Assumption: customer IDs contain CUST- followed by five digits.
CustomerId = Annotated[
    str,
    Field(strict=True, pattern=r"^CUST-[0-9]{5}$"),
]


class GetCustomerRecordInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    customer_id: CustomerId


class TriggerRefundInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    customer_id: CustomerId
    amount: float = Field(
        strict=True,
        gt=0,
        allow_inf_nan=False,
    )
    reason: str = Field(
        strict=True,
        min_length=10,
    )


server = Server("quilrai-customer-server")


# Describe the tools available to clients.
async def list_tools() -> list[Tool]:
    return [
        Tool(
            name="get_customer_record",
            description="Return a mock customer record using a customer ID.",
            inputSchema=GetCustomerRecordInput.model_json_schema(),
        ),
        Tool(
            name="trigger_refund",
            description="Simulate a refund. No real payment is issued.",
            inputSchema=TriggerRefundInput.model_json_schema(),
        ),
    ]


async def handle_list_tools(context, params):
    tools = await list_tools()
    return ListToolsResult(tools=tools)


# Validate arguments before executing mock tool logic.
def execute_tool(name: str, arguments: dict) -> dict:
    if name == "get_customer_record":
        validated = GetCustomerRecordInput.model_validate(arguments)

        return {
            "customer_id": validated.customer_id,
            "name": "Demo Customer",
            "status": "active",
            "mock": True,
        }

    if name == "trigger_refund":
        validated = TriggerRefundInput.model_validate(arguments)

        return {
            "customer_id": validated.customer_id,
            "amount": validated.amount,
            "reason": validated.reason,
            "status": "simulated",
            "mock": True,
        }

    raise ValueError("Unknown tool")


async def handle_call_tool(context, params: CallToolRequestParams):
    arguments = params.arguments if params.arguments is not None else {}

    try:
        result = execute_tool(params.name, arguments)
    except ValidationError:
        raise MCPError(
            code=-32602,
            message="Invalid tool arguments",
        ) from None
    except ValueError:
        raise MCPError(
            code=-32602,
            message="Unknown tool",
        ) from None

    return CallToolResult(
        content=[
            TextContent(
                type="text",
                text=json.dumps(result),
            )
        ]
    )


# Register handlers using the installed SDK's interface.
server.add_request_handler(
    "tools/list",
    PaginatedRequestParams,
    handle_list_tools,
)

server.add_request_handler(
    "tools/call",
    CallToolRequestParams,
    handle_call_tool,
)


# Convert transport validation failures into JSON-RPC errors.
async def forward_validated_messages(source, destination, responses):
    async with destination:
        async for item in source:
            if isinstance(item, ValidationError):
                malformed_json = any(
                    error["type"] == "json_invalid"
                    for error in item.errors()
                )

                code = -32700 if malformed_json else -32600
                message = (
                    "Parse error" if malformed_json else "Invalid Request"
                )

                await responses.send(
                    SessionMessage(
                        JSONRPCError(
                            jsonrpc="2.0",
                            id=None,
                            error=ErrorData(
                                code=code,
                                message=message,
                            ),
                        )
                    )
                )
                continue

            # Preserve other SDK messages or exceptions.
            await destination.send(item)


async def main():
    logger.info("Starting QuilrAI MCP server over stdio")

    async with stdio_server() as (read_stream, write_stream):
        sender, receiver = anyio.create_memory_object_stream(0)

        async with anyio.create_task_group() as tasks:
            tasks.start_soon(
                forward_validated_messages,
                read_stream,
                sender,
                write_stream,
            )

            try:
                await server.run(
                    receiver,
                    write_stream,
                    server.create_initialization_options(),
                )
            finally:
                tasks.cancel_scope.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass