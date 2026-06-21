# Neurogossip reference implementation

`examples/talking_agents.py` is a self-contained demo showing how an agent uses the
`NeurogossipClient` library to talk to another agent through the Neurogossip
WebSocket server.

## Run it

Install the two packages (from PyPI, or editable from this repo):

```bash
pip install neurogossip-server neurogossip-client
```

Start the server in one terminal:

```bash
neurogossip-server --port 8765        # or: python -m neurogossip_server --port 8765
```

Run the demo in another (from the repo root):

```bash
python examples/talking_agents.py --server-url ws://localhost:8765
```

(If `8765` is busy, pick any free port and pass the same URL to the example.)

## What it demonstrates

Two agents — `axioma` and `thea` — run in a single process and complete a full exchange:

1. Both agents `connect()` and register.
2. `axioma` calls `list_agents()` and discovers `thea`.
3. `axioma` sends the question *"What is the integral of e^(-x²) from -∞ to ∞?"*.
4. The server forwards it to `thea` (per-pair `seq=1`).
5. `thea`'s `on_message` handler calls `send_receipt()` — completing the end-to-end
   ACK so `axioma` is told the message was **delivered** (not just buffered).
6. `thea` replies `sqrt(pi)` with `reply_to` + `conversation_id` (authorized reply,
   `seq=1` on the return pair).
7. `axioma` receipts the reply.
8. `thea` ends the conversation; `axioma` receives the `conversation_ended` event.

Expected output (abridged):

```
[thea]   registered (session …)
[axioma] registered (session …)
[axioma] directory: [('thea','online'), ('axioma','online')]
[axioma] sent question (msg_id=…)
[axioma] ack msg=… status=pending
[thea]   recv from axioma: 'What is the integral of e^(-x^2) …' (seq=1)
[thea]   replying with 'sqrt(pi)'
[thea]   ended the conversation
[axioma] ack msg=… status=delivered
[axioma] recv from thea: 'sqrt(pi)' (seq=1)
[axioma] conversation … ended (reason=resolved, by=thea)
[axioma] question delivered=True
```

## Reusing the patterns in your own agent

The handler shapes in `talking_agents.py` map directly onto the design's integration
examples (DESIGN §4.2/§4.3):

```python
from neurogossip_client import NeurogossipClient

client = NeurogossipClient(server_url, agent_id="myagent",
                           metadata={"display_name": "My Agent", "version": "1.0"})

async def on_message(msg):
    await client.send_receipt(msg["msg_id"])          # complete the e2e ACK
    await client.send(msg["from"], "got it",
                      reply_to=msg["msg_id"],
                      conversation_id=msg.get("conversation_id"))

client.on_message(on_message)
await client.connect()
```