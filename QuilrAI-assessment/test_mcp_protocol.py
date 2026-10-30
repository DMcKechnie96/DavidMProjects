import json
import selectors
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class MCPProtocolTests(unittest.TestCase):
    def test_protocol_and_output_isolation(self):
        project_root = Path(__file__).resolve().parent

        with tempfile.TemporaryFile(mode="w+") as logs:
            process = subprocess.Popen(
                [sys.executable, str(project_root / "Server.py")],
                cwd=project_root,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=logs,
                text=True,
                bufsize=1,
            )

            selector = selectors.DefaultSelector()
            selector.register(process.stdout, selectors.EVENT_READ)

            def send(payload):
                process.stdin.write(json.dumps(payload) + "\n")
                process.stdin.flush()

            def receive():
                self.assertTrue(
                    selector.select(timeout=5),
                    "Timed out waiting for MCP response",
                )

                line = process.stdout.readline()
                self.assertTrue(line, "Server closed stdout")

                response = json.loads(line)
                self.assertEqual(response.get("jsonrpc"), "2.0")
                return response

            def call_tool(request_id, name, arguments):
                send({
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": "tools/call",
                    "params": {
                        "name": name,
                        "arguments": arguments,
                    },
                })

                response = receive()
                self.assertEqual(response["id"], request_id)
                return response

            try:
                # Malformed JSON before initialization.
                process.stdin.write('{"jsonrpc":\n')
                process.stdin.flush()

                response = receive()
                self.assertIsNone(response["id"])
                self.assertEqual(response["error"]["code"], -32700)

                send({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": {
                        "protocolVersion": "2025-03-26",
                        "capabilities": {},
                        "clientInfo": {
                            "name": "automated-test",
                            "version": "1.0.0",
                        },
                    },
                })

                response = receive()
                self.assertEqual(response["id"], 1)
                self.assertIn("result", response)

                send({
                    "jsonrpc": "2.0",
                    "method": "notifications/initialized",
                })

                send({
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "tools/list",
                    "params": {},
                })

                response = receive()
                self.assertEqual(response["id"], 2)
                self.assertEqual(
                    {
                        tool["name"]
                        for tool in response["result"]["tools"]
                    },
                    {"get_customer_record", "trigger_refund"},
                )

                response = call_tool(
                    3,
                    "get_customer_record",
                    {"customer_id": "CUST-12345"},
                )

                customer = json.loads(
                    response["result"]["content"][0]["text"]
                )
                self.assertEqual(customer["customer_id"], "CUST-12345")

                response = call_tool(
                    4,
                    "trigger_refund",
                    {
                        "customer_id": "CUST-12345",
                        "amount": 25.5,
                        "reason": "Customer returned the item",
                    },
                )

                refund = json.loads(
                    response["result"]["content"][0]["text"]
                )
                self.assertEqual(refund["status"], "simulated")

                response = call_tool(
                    5,
                    "trigger_refund",
                    {
                        "customer_id": "CUST-12345",
                        "amount": 0,
                        "reason": "Customer returned the item",
                    },
                )
                self.assertEqual(response["error"]["code"], -32602)

                send({
                    "jsonrpc": "2.0",
                    "id": 6,
                    "method": "unknown_method",
                    "params": {},
                })

                response = receive()
                self.assertEqual(response["id"], 6)
                self.assertEqual(response["error"]["code"], -32601)

                # Valid JSON, invalid JSON-RPC request.
                send({})
                response = receive()
                self.assertEqual(response["error"]["code"], -32600)

                process.stdin.close()
                process.wait(timeout=5)
                self.assertEqual(process.returncode, 0)

                # Any remaining stdout must also be JSON-RPC.
                for line in process.stdout:
                    self.assertEqual(
                        json.loads(line).get("jsonrpc"),
                        "2.0",
                    )

                logs.seek(0)
                self.assertIn(
                    "Starting QuilrAI MCP server",
                    logs.read(),
                )

            finally:
                selector.close()

                if process.poll() is None:
                    process.kill()
                    process.wait()

                if process.stdin and not process.stdin.closed:
                    process.stdin.close()

                process.stdout.close()


if __name__ == "__main__":
    unittest.main()