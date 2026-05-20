"""Compatibility entrypoint for existing worker service units."""

from .discord.worker_service import main, run_worker_service


if __name__ == "__main__":
    main()
