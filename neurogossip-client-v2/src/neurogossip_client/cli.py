"""CLI for neurogossip-client: send, listen, peers, ping.

The Redis URL defaults to the REDIS_URL environment variable; --redis overrides it.
If neither is set, the command exits with the same error as the library.
"""
from __future__ import annotations

import asyncio
import os
import sys
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from . import NeuroGossipClient, AgentIdentity, RedisUrlNotConfiguredError

app = typer.Typer(add_completion=False, help="neurogossip-client CLI")
console = Console()


def _redis_url(redis: Optional[str]) -> str:
    url = redis or os.environ.get("REDIS_URL")
    if not url:
        console.print(
            "[red]REDIS_URL is not configured.[/red] Set the REDIS_URL environment "
            "variable (e.g. REDIS_URL=redis://localhost:6379/0) or pass --redis URL."
        )
        raise typer.Exit(code=2)
    return url


def _agent(agent_id: str, name: Optional[str], role: Optional[str]) -> AgentIdentity:
    return AgentIdentity(agent_id=agent_id, agent_name=name or agent_id.upper(),
                         role=role)


@app.command()
def send(
    agent_id: str = typer.Option(..., "--agent", help="Your agent id"),
    room: Optional[str] = typer.Option(None, "--room", help="Room to publish to"),
    to: Optional[str] = typer.Option(None, "--to", help="Recipient agent id (direct)"),
    message: str = typer.Option(..., "--message", help="Markdown message body"),
    namespace: str = typer.Option(os.getenv("NEUROGOSSIP_NAMESPACE", "default"), "--namespace"),
    name: Optional[str] = typer.Option(None, "--name"),
    role: Optional[str] = typer.Option(None, "--role"),
    redis: Optional[str] = typer.Option(None, "--redis", help="Redis URL (default: $REDIS_URL)"),
    thread_id: Optional[str] = typer.Option(None, "--thread"),
    reply_to: Optional[str] = typer.Option(None, "--reply-to"),
) -> None:
    """Send a message (to a room with --room, or direct with --to)."""
    url = _redis_url(redis)
    client = NeuroGossipClient(agent=_agent(agent_id, name, role), namespace=namespace,
                               redis_url=url, heartbeat_interval_s=999)
    async def _run():
        async with client:
            if to:
                msg = await client.send_direct(to, message, thread_id=thread_id, reply_to=reply_to)
                console.print(f"[green]sent direct[/green] to {to}: msg_id={msg.message_id}")
            elif room:
                msg = await client.publish(room, message, thread_id=thread_id, reply_to=reply_to)
                console.print(f"[green]sent[/green] to room {room}: msg_id={msg.message_id}")
            else:
                console.print("[red]specify --room or --to[/red]")
                raise typer.Exit(code=2)
    asyncio.run(_run())


@app.command()
def listen(
    agent_id: str = typer.Option(..., "--agent", help="Your agent id"),
    room: Optional[str] = typer.Option(None, "--room", help="Room to join (else: direct inbox only)"),
    namespace: str = typer.Option(os.getenv("NEUROGOSSIP_NAMESPACE", "default"), "--namespace"),
    name: Optional[str] = typer.Option(None, "--name"),
    role: Optional[str] = typer.Option(None, "--role"),
    redis: Optional[str] = typer.Option(None, "--redis", help="Redis URL (default: $REDIS_URL)"),
) -> None:
    """Listen for incoming messages (direct inbox + optionally a room)."""
    url = _redis_url(redis)
    client = NeuroGossipClient(agent=_agent(agent_id, name, role), namespace=namespace, redis_url=url)
    async def _run():
        async with client:
            if room:
                await client.join_room(room)
                console.print(f"[dim]listening in room {room} + direct inbox…[/dim]")
            else:
                console.print("[dim]listening on direct inbox…[/dim]")
            async for msg in client.listen():
                tag = msg.recipient.mode
                console.print(f"[cyan][{msg.sender.agent_id}][/cyan] ({tag}) {msg.content.body}")
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        pass


@app.command()
def peers(
    agent_id: str = typer.Option(..., "--agent", help="Your agent id"),
    namespace: str = typer.Option(os.getenv("NEUROGOSSIP_NAMESPACE", "default"), "--namespace"),
    redis: Optional[str] = typer.Option(None, "--redis", help="Redis URL (default: $REDIS_URL)"),
) -> None:
    """List other agents currently present (online)."""
    url = _redis_url(redis)
    client = NeuroGossipClient(agent=_agent(agent_id, None, None), namespace=namespace,
                               redis_url=url)
    async def _run():
        async with client:
            peers = await client.list_online_agents()
        if not peers:
            console.print("[dim]no other agents online[/dim]")
            return
        table = Table("agent_id", "name", "role", "status")
        for p in peers:
            table.add_row(p.agent_id, p.agent_name or "-", p.role or "-", p.status)
        console.print(table)
    asyncio.run(_run())


@app.command()
def ping(
    redis: Optional[str] = typer.Option(None, "--redis", help="Redis URL (default: $REDIS_URL)"),
) -> None:
    """Ping the Redis server."""
    url = _redis_url(redis)
    import redis.asyncio as aioredis
    async def _run():
        r = aioredis.from_url(url, decode_responses=True)
        try:
            await r.ping()
            console.print(f"[green]pong[/green] {url}")
        finally:
            await r.aclose()
    asyncio.run(_run())


def main() -> None:
    app()


if __name__ == "__main__":
    main()