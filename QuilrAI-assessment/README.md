# QuilrAI AI Gateway Assessment

Python implementations of four assessment tasks covering MCP tools,
authorization, streaming PII redaction, and resilient model routing.

Customer records, refunds, and model providers are mocked.
No real payments or external model credentials are required.

## Requirements and setup

Developed and tested locally using Python 3.13.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

Run commands from the project root. Each running server requires its
own terminal with the virtual environment activated.

## Project files

| File | Purpose |
|---|---|
| Server.py | Task 1: MCP server using stdio |
| gateway.py | Task 2: HTTP authorization gateway |
| mock_downstream.py | Task 2: downstream MCP tool mock |
| stream_guardrail.py | Task 3: stateful text redaction |
| mock_llm.py | Task 3: streaming provider mock |
| llm_gateway.py | Task 3: streaming redaction gateway |
| rate_limiter.py | Task 4: SQLite token reservations |
| mock_models.py | Task 4: provider behavior mocks |
| model_router.py | Task 4: rate-limited fallback router |

## Task 1: MCP server

Start:

```bash
python Server.py
```

Tools:

- `get_customer_record`: requires a customer ID.
- `trigger_refund`: requires a customer ID, positive finite amount,
  and reason containing at least 10 characters.

Customer IDs are interpreted as `CUST-` followed by exactly five digits.
Unexpected tool argument fields are rejected.

The server uses the official MCP SDK and Pydantic validation.
Application logs go to stderr; stdout carries JSON-RPC messages.

Error mappings include:

- `-32700`: malformed JSON.
- `-32600`: invalid JSON-RPC structure.
- `-32601`: unknown method.
- `-32602`: invalid tool arguments or unknown tool.

The parsing adapter converts transport validation exceptions into
JSON-RPC responses before forwarding valid messages to the SDK runner.

### Manual protocol example

Enter each JSON message as a separate line in the running server's terminal:

```json
{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"manual-test","version":"1.0.0"}}}
{"jsonrpc":"2.0","method":"notifications/initialized"}
{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}
{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"get_customer_record","arguments":{"customer_id":"CUST-12345"}}}
```

Refunds return a simulated result and do not contact a payment system.

## Task 2: MCP authorization gateway

Terminal 1:

```bash
python -m uvicorn mock_downstream:app --host 127.0.0.1 --port 8001
```

Terminal 2:

```bash
GATEWAY_ADMIN_TOKEN=admin-demo-token GATEWAY_VIEWER_TOKEN=viewer-demo-token python -m uvicorn gateway:app --host 127.0.0.1 --port 8000
```

The gateway verifies opaque Bearer tokens against a server-controlled
role mapping.

- Authenticated `tools/list` requests are forwarded without filtering.
- Viewer calls to tools beginning with `admin_` are rejected locally.
- Admin calls are forwarded.
- Unknown tokens receive HTTP 401.
- Blocked admin calls return JSON-RPC error `-32001`:
  `Unauthorized Tool Call`.

### Test viewer authorization

```bash
curl -s http://127.0.0.1:8000/mcp \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer viewer-demo-token" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"admin_reset_key","arguments":{}}}'
```

Expected: `-32001` and no additional downstream request log.

This is a focused HTTP/JSON-RPC assessment proxy. It supports individual
requests with string or integer IDs, not batches, notifications, or a
complete MCP Streamable HTTP session lifecycle.

The downstream mock is intentionally minimal and is not a replacement
for Task 1's validated MCP server.

## Task 3: streaming redaction gateway

Terminal 1:

```bash
python -m uvicorn mock_llm:app --host 127.0.0.1 --port 8002
```

Terminal 2:

```bash
python -m uvicorn llm_gateway:app --host 127.0.0.1 --port 8003
```

Test:

```bash
curl -N -s http://127.0.0.1:8003/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"mock-model","messages":[{"role":"user","content":"Run the demo"}],"stream":true}'
```

Expected combined text:

```text
Hello! Contact [REDACTED] for help. SSN: [REDACTED]. Card: [REDACTED]. Thanks!
```

The gateway parses provider SSE events and redacts their text deltas.
It does not collect the entire generated response.

Supported patterns:

- Common ASCII email addresses.
- SSNs formatted as `123-45-6789`.
- Card-like sequences of 13–19 digits with optional single spaces
  or hyphens between digits.

Card matching does not perform a Luhn check and may redact unrelated
long numbers. These patterns do not constitute universal PII detection.

The redactor retains unfinished tokens and possible numeric sequences
across chunks. Pending text is limited to 512 characters. Overflow
produces `[REDACTED]` and suppresses the rest of that line.

This conservative policy can hide ordinary text. Word buffering also
adds latency until a safe boundary arrives; there is no fixed
time-based flushing guarantee.

SSE line and event buffers are limited to 64 KiB. The parser supports
LF and CRLF event framing. The endpoint supports one text completion;
tool-call output and multiple choices are unsupported.

Connection failures return a sanitized HTTP 502. Detected failures
after streaming starts are represented by a sanitized SSE error.

A local mock test observed first text at 28 ms and total duration at
487 ms. These are individual local measurements, not production
performance guarantees.

## Task 4: SQLite rate limiting and fallback

Terminal 1:

```bash
python -m uvicorn mock_models:app --host 127.0.0.1 --port 8004
```

Terminal 2:

```bash
ROUTER_API_KEY=tenant-demo-key python -m uvicorn model_router:app --host 127.0.0.1 --port 8005
```

### Primary success

```bash
curl -s http://127.0.0.1:8005/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer tenant-demo-key" \
  -d '{"prompt":"Hello","max_tokens":100,"primary_mode":"success"}'
```

### Fallback after primary 429

```bash
curl -s http://127.0.0.1:8005/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer tenant-demo-key" \
  -d '{"prompt":"Hello","max_tokens":100,"primary_mode":"rate-limited"}'
```

### Fallback after primary timeout

```bash
curl -s -w '\nTotal time: %{time_total}s\n' \
  http://127.0.0.1:8005/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer tenant-demo-key" \
  -d '{"prompt":"Hello","max_tokens":100,"primary_mode":"timeout"}'
```

The primary attempt has a 3-second total deadline. A primary 429 or
timeout triggers one backup attempt with its own 3-second deadline.
Other primary failures return sanitized errors without fallback.

The demo mode fields select fixed local endpoints. Clients cannot
supply arbitrary provider URLs.

### Token accounting

The default limit is 50,000 tokens per tenant over 60 seconds.

SQLite `BEGIN IMMEDIATE` makes budget checks and reservations atomic.
Each provider attempt reserves its own budget before dispatch.

The accounting policy counts:

- Actual settled usage charged within the previous 60 seconds.
- All pending reservations, regardless of age.

Settled usage is timestamped at completion. Expired settled rows are
removed when a new reservation is attempted.

For this mock contract, input usage equals the prompt's UTF-8 byte
length. The reservation adds the requested maximum output tokens.
This is demo accounting, not an actual model tokenizer. Production
providers require appropriate tokenization and message overhead.

Successful calls reconcile against reported usage. Timed-out or
otherwise uncertain attempts retain their full reserved charge.
Mock 429 responses release the reservation because the mock contract
defines them as consuming no tokens.

Fallback may be rejected if insufficient tenant budget remains.
Response usage describes only the successful provider, not all
conservatively charged attempts.

The database is `gateway_usage.sqlite3`. Tenant identifiers are API-key
digests. The limiter supports multiple tenant IDs; the demo HTTP
endpoint configures one valid API key.

Pending reservations survive restarts and do not automatically expire.
Abandoned reservations require operational reconciliation. Automatic
crash recovery is not implemented.

## Validation performed

Checks run during development included:

- MCP initialization, discovery, successful tool calls, and validation errors.
- Malformed JSON handling, recovery, and unknown-method errors.
- Separate capture of stdout and stderr.
- Viewer/admin authorization and rejection of unknown tokens.
- Confirmation that blocked admin calls did not reach downstream.
- Redaction at every two-chunk split for selected samples.
- One-character chunks and bounded-buffer overflow recovery.
- End-to-end SSE redaction and client-observed first-text timing.
- Sanitized provider-unavailable responses.
- Concurrent SQLite reservations, tenant isolation, and persistence.
- Exact sliding-window expiration boundaries.
- Primary success, 429 fallback, and approximately 3-second timeout fallback.
- Sanitized primary and backup error responses.

These checks were run from terminal commands. A committed automated
test suite has not yet been added.

Further validation should cover midstream provider failures, client
disconnects, cancellation races, and multiple worker processes.

## Local demo scope

All HTTP services bind to loopback. Example tokens are demonstration
values. Production deployment would require authentication integration,
secret management, HTTPS, provider-specific adapters, and operational
monitoring.


## Automated tests

Run all tests from the project root:

```bash
python -m unittest discover -v
```

The suite contains 28 tests covering tool validation, real MCP stdio
communication, gateway authorization, streaming redaction, SQLite
accounting, and model fallback.

HTTP tests use in-memory mocks. The MCP protocol test starts and stops
its own subprocess. No manually running servers or external credentials
are required.

Separate live mock-server checks verified end-to-end streaming and the
configured 3-second primary timeout.