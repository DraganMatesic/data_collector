"""Dramatiq CLI wrapper that patches StreamablePipe before worker spawning.

Dramatiq's ``cli.py`` wraps stderr in a ``StreamablePipe`` backed by
``multiprocessing.connection.Connection.send_bytes()``.  Python 3.12+
raises ``ValueError`` on concurrent ``send_bytes()`` calls, crashing
the worker subprocess before it can import ``actors.py``.

This wrapper replaces ``StreamablePipe`` with a pass-through that
writes directly to file descriptor 2 (OS-level stderr).  Using the
raw fd avoids infinite recursion (Dramatiq replaces ``sys.stderr``
with ``StreamablePipe``, so writing to ``sys.stderr`` would recurse).

Usage::

    python -m data_collector.dramatiq.cli_wrapper data_collector.dramatiq.actors -p 2 -t 4
"""

from __future__ import annotations

import os

import dramatiq.compat  # pyright: ignore[reportUnusedImport]

if hasattr(dramatiq.compat, "StreamablePipe"):

    class _DirectFdPipe:
        """Drop-in replacement for ``StreamablePipe`` using raw fd 2.

        Writes directly to OS file descriptor 2 (stderr) via
        ``os.write()``.  This bypasses ``sys.stderr`` entirely,
        avoiding recursion when Dramatiq replaces ``sys.stderr``
        with this object.  The object is fully picklable (no locks,
        no file handles) for Windows ``spawn`` multiprocessing.
        """

        def __init__(self, pipe: object, *, encoding: str = "utf-8") -> None:  # noqa: ARG002
            self.encoding = encoding

        def write(self, s: str) -> int:
            """Write to OS stderr fd, bypassing sys.stderr."""
            try:
                data = s.encode(self.encoding, errors="replace")
                os.write(2, data)
                return len(s)
            except OSError:
                return 0

        def flush(self) -> None:
            """No-op -- os.write() is unbuffered."""

    dramatiq.compat.StreamablePipe = _DirectFdPipe  # type: ignore[assignment, attr-defined]

from dramatiq.cli import main as dramatiq_main  # pyright: ignore[reportUnknownVariableType]  # noqa: E402

if __name__ == "__main__":
    dramatiq_main()  # pyright: ignore[reportUnknownMemberType]
