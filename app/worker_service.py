import asyncio
import signal
import socket
import threading

from .backfill_requests import backfill_request_loop, requeue_interrupted_backfill_requests
from .config import get_settings
from .db import init_db, managed_session
from .discord_ingest import (
    discord_runtime_state,
    get_discord_client,
    periodic_attachment_repair_loop,
    recent_message_audit_loop,
    run_discord_bot,
    seed_channels_from_env,
)
from .ops_log import write_operations_log
from .runtime_logging import setup_runtime_file_logging
from .runtime_monitor import runtime_heartbeat_loop
from .worker import (
    parser_loop,
    periodic_stitch_audit_loop,
)

settings = get_settings()
setup_runtime_file_logging("worker.log")


def worker_runtime_details() -> dict:
    return {
        "discord_status": discord_runtime_state.get("status"),
        "discord_error": discord_runtime_state.get("error"),
        "parser_worker_enabled": settings.parser_worker_enabled,
        "discord_ingest_enabled": settings.discord_ingest_enabled,
        "periodic_stitch_audit_enabled": settings.periodic_stitch_audit_enabled,
        "periodic_stitch_audit_interval_minutes": settings.periodic_stitch_audit_interval_minutes,
        "periodic_attachment_repair_enabled": settings.periodic_attachment_repair_enabled,
        "periodic_attachment_repair_interval_minutes": settings.periodic_attachment_repair_interval_minutes,
        "periodic_attachment_repair_lookback_hours": settings.periodic_attachment_repair_lookback_hours,
        "periodic_attachment_repair_limit": settings.periodic_attachment_repair_limit,
        "periodic_attachment_repair_min_age_minutes": settings.periodic_attachment_repair_min_age_minutes,
        "service_mode": "worker-host",
        "last_recent_audit_at": discord_runtime_state.get("last_recent_audit_at"),
        "last_recent_audit_summary": discord_runtime_state.get("last_recent_audit_summary"),
        "last_attachment_repair_at": discord_runtime_state.get("last_attachment_repair_at"),
        "last_attachment_repair_summary": discord_runtime_state.get("last_attachment_repair_summary"),
    }


async def run_worker_service() -> None:
    if not settings.discord_ingest_enabled and not settings.parser_worker_enabled:
        raise SystemExit("Worker service has nothing to run. Enable DISCORD_INGEST_ENABLED and/or PARSER_WORKER_ENABLED.")

    init_db()
    seed_channels_from_env()

    with managed_session() as session:
        requeue_interrupted_backfill_requests(session)
        if not settings.discord_ingest_enabled:
            write_operations_log(
                session,
                event_type="backfill_executor_disabled",
                level="warning",
                source="worker",
                message=(
                    "Backfill queue execution is disabled on this runtime because "
                    "DISCORD_INGEST_ENABLED is false."
                ),
                details={
                    "runtime_name": settings.runtime_name,
                    "discord_ingest_enabled": settings.discord_ingest_enabled,
                    "parser_worker_enabled": settings.parser_worker_enabled,
                },
            )

    stop_event = asyncio.Event()
    heartbeat_stop_event = threading.Event()
    heartbeat_thread = threading.Thread(
        target=runtime_heartbeat_loop,
        kwargs={
            "stop_event": heartbeat_stop_event,
            "runtime_name": settings.runtime_name,
            "host_name": socket.gethostname(),
            "details_provider": worker_runtime_details,
        },
        name="worker-heartbeat",
        daemon=True,
    )
    heartbeat_thread.start()

    background_tasks: list[asyncio.Task] = []
    if settings.discord_ingest_enabled:
        background_tasks.append(asyncio.create_task(run_discord_bot(stop_event), name="discord-ingest"))
        background_tasks.append(
            asyncio.create_task(
                backfill_request_loop(stop_event, get_discord_client),
                name="backfill-queue",
            )
        )
        background_tasks.append(
            asyncio.create_task(
                recent_message_audit_loop(stop_event, get_discord_client),
                name="recent-message-audit",
            )
        )
    if settings.discord_ingest_enabled and settings.parser_worker_enabled:
        background_tasks.append(
            asyncio.create_task(
                periodic_stitch_audit_loop(stop_event),
                name="stitch-audit",
            )
        )
    if settings.discord_ingest_enabled and settings.periodic_attachment_repair_enabled:
        background_tasks.append(
            asyncio.create_task(
                periodic_attachment_repair_loop(stop_event, get_discord_client),
                name="attachment-repair-audit",
            )
        )
    if settings.parser_worker_enabled:
        background_tasks.append(asyncio.create_task(parser_loop(stop_event), name="parser-worker"))

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    stop_waiter = asyncio.create_task(stop_event.wait(), name="worker-stop-waiter")
    try:
        done, _pending = await asyncio.wait([stop_waiter, *background_tasks], return_when=asyncio.FIRST_COMPLETED)
        if stop_waiter not in done:
            stop_event.set()
            for task in done:
                if task is stop_waiter:
                    continue
                exc = task.exception()
                if exc:
                    raise exc
                raise RuntimeError(f"{task.get_name()} exited unexpectedly")
    finally:
        stop_event.set()
        heartbeat_stop_event.set()
        for task in background_tasks:
            if not task.done():
                task.cancel()
        if background_tasks:
            await asyncio.gather(*background_tasks, return_exceptions=True)
        stop_waiter.cancel()
        await asyncio.gather(stop_waiter, return_exceptions=True)
        heartbeat_thread.join(timeout=5)


def main() -> None:
    asyncio.run(run_worker_service())


if __name__ == "__main__":
    main()
