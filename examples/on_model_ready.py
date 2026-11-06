#!/usr/bin/env python3
"""Receiver-side example: replace this script's body with your local operation."""
import json
import os

event_id = os.environ["RELAY_EVENT_ID"]
payload = json.loads(os.environ["RELAY_PAYLOAD_JSON"])
print(f"收到模型就绪事件：{event_id}")
print(json.dumps(payload, ensure_ascii=False, indent=2))
# Call a local evaluation script or service here. Use event_id as an idempotency key.
