"""Demo demonstrating a multi-agent conversational session with human-in-the-loop."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import redis.asyncio as aioredis

from neurogossip_agent.session_manager import AgentConversationManager
from neurogossip_agent.transport import MockAgentTransport

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("conversational_demo")


async def run_demo():
    # Load env variables from workspace .env file if available
    env = {}
    env_path = "/home/ubuntu/neurogossip/.env"
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    env[k.strip()] = v.strip()

    redis_url = (
        os.environ.get("NEUROGOSSIP_V3_REDIS_URL")
        or os.environ.get("REDIS_URL")
        or env.get("NEUROGOSSIP_V3_REDIS_URL")
        or env.get("REDIS_URL")
        or "redis://localhost:6379/0"
    )

    # 1. Initialize live Redis and Session Manager
    redis_client = aioredis.from_url(redis_url, decode_responses=True)
    await redis_client.flushdb()  # Clear database before demo
    
    # We use MockAgentTransport for the demo to run locally without a running Redis server
    transport = MockAgentTransport()
    
    manager = AgentConversationManager(redis_client, transport)
    
    log.info("--- Starting Conversational Session Demo ---")
    
    # 2. Human initiates a conversation with Coordinator Agent "skye"
    session = await manager.create_conversation(initiator_id="skye", metadata={"topic": "eta_stability"})
    cid = session.conversation_id
    log.info(f"Created conversation session: {cid}")
    
    # Create the root request from human to skye
    root_req = await manager.create_request(
        conversation_id=cid,
        sender_id="human",
        recipient_ids=["skye"],
        payload="Check if the eta stability is stable across runs."
    )
    log.info(f"[Human -> skye] Request (Root): '{root_req.payload}' (ID: {root_req.request_id})")
    
    # 3. Coordinator Agent "skye" plans and delegates tasks to "axioma"
    log.info("[skye] Analyzing request...")
    await manager.log_status_report(cid, "skye", "Analyzing stability request...")
    
    sub_req = await manager.create_request(
        conversation_id=cid,
        sender_id="skye",
        recipient_ids=["axioma"],
        payload="Run eta stability simulation with 1000 iterations.",
        parent_request_id=root_req.request_id
    )
    log.info(f"[skye -> axioma] Sub-request: '{sub_req.payload}' (ID: {sub_req.request_id})")
    
    # 4. Worker Agent "axioma" processes the request. It needs clarification on the parameters, so it asks the human.
    log.info("[axioma] Received request. Need clarification on the learning rate parameter.")
    await manager.log_status_report(cid, "axioma", "Axioma checking parameters...")
    
    human_req = await manager.create_request(
        conversation_id=cid,
        sender_id="axioma",
        recipient_ids=["human"],
        payload="Should I run the simulation with standard learning rate (0.01) or high (0.1)?",
        parent_request_id=sub_req.request_id
    )
    log.info(f"[axioma -> Human] Clarification request: '{human_req.payload}' (ID: {human_req.request_id})")
    
    # Verify the session transitioned to WAITING_FOR_HUMAN status
    current_session = await manager.get_conversation(cid)
    log.info(f"Current conversation status: {current_session.status.value}")
    
    # 5. Human observes the request and provides input
    log.info("[Human] Reviewing clarification request...")
    await asyncio.sleep(0.5)
    
    await manager.send_response(
        request_id=human_req.request_id,
        sender_id="human",
        payload="Use standard learning rate (0.01)."
    )
    log.info(f"[Human -> axioma] Input response: 'Use standard learning rate (0.01).'")
    
    # Verify the session transitioned back to ACTIVE
    current_session = await manager.get_conversation(cid)
    log.info(f"Current conversation status: {current_session.status.value}")
    
    # 6. axioma receives human input, finishes the simulation, and replies to skye
    log.info("[axioma] Running simulation with learning rate = 0.01...")
    await asyncio.sleep(0.5)
    
    await manager.send_response(
        request_id=sub_req.request_id,
        sender_id="axioma",
        payload={"result": "Stable", "variance": 0.002, "iterations_run": 1000}
    )
    log.info(f"[axioma -> skye] Sub-request response: Simulation results submitted.")
    
    # 7. skye receives axioma's results, compiles final findings, and responds to human (completing the root request)
    log.info("[skye] Compiling final findings...")
    await manager.log_status_report(cid, "skye", "Compiling final findings for human...")
    
    final_findings = (
        "Based on axioma's simulation (1000 iterations, lr=0.01), the eta stability is stable. "
        "The variance observed was very low (0.002)."
    )
    
    await manager.send_response(
        request_id=root_req.request_id,
        sender_id="skye",
        payload=final_findings
    )
    log.info(f"[skye -> Human] Final findings response: '{final_findings}'")
    
    # 8. Verify the session status is now COMPLETED
    current_session = await manager.get_conversation(cid)
    log.info(f"Final conversation status: {current_session.status.value}")
    
    # 9. Demonstrate Archiving & Un-archiving
    log.info("\n--- Memory Management: Archiving & Un-archiving Demo ---")
    
    # Manually load the session context to local memory
    ctx = await manager.process_incoming_message(
        conversation_id=cid,
        message_id="msg_dummy",
        reply_to=None,
        tags=["status_report"],
        sender_id="skye",
        payload={"message": "Dummy load"}
    )
    log.info(f"Session {cid} loaded in active_sessions (memory): {cid in manager.active_sessions}")
    
    # Run sweep to archive it (since it is completed, it archives immediately)
    archived = await manager.sweep_sessions(idle_timeout_s=999.0)
    log.info(f"Sweeper archived session IDs: {archived}")
    log.info(f"Session {cid} remains in active_sessions (memory): {cid in manager.active_sessions}")
    
    # Simulate un-archiving on a new incoming follow-up message
    log.info("Simulating incoming follow-up message...")
    unarchived_ctx = await manager.process_incoming_message(
        conversation_id=cid,
        message_id="msg_followup",
        reply_to=root_req.request_id,
        tags=["status_report"],
        sender_id="human",
        payload={"message": "Thanks for the feedback!"}
    )
    log.info(f"Session {cid} un-archived and loaded back into memory: {cid in manager.active_sessions}")
    log.info(f"Reloaded conversation history depth: {len(unarchived_ctx.local_state.get('history', []))}")
    
    # 10. Print out the full persistent history log
    log.info("\n--- Persistent Conversation History Log ---")
    history = await manager.get_history(cid)
    for i, event in enumerate(history, 1):
        sender = event.sender_id or "System"
        recipient = f" -> {event.recipient_id}" if event.recipient_id else ""
        content = json.dumps(event.content) if event.content else ""
        print(f"{i}. [{event.timestamp.strftime('%H:%M:%S')}] {sender}{recipient} | {event.type.upper()} | {content}")
        
    await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(run_demo())
