"""Entry point for ``python -m neurogossip_server`` and the console script."""

from .server import main

if __name__ == "__main__":  # pragma: no cover - exercised via `python -m`
    main()