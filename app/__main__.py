"""Entry point: ``python -m app``."""

from __future__ import annotations

from .config import load_config
from .server import serve


def main() -> None:
    serve(load_config())


if __name__ == "__main__":
    main()
