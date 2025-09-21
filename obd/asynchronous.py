# -*- coding: utf-8 -*-

########################################################################
#                                                                      #
# python-OBD: A python OBD-II serial module derived from pyobd         #
#                                                                      #
# Copyright 2004 Donour Sizemore (donour@uchicago.edu)                 #
# Copyright 2009 Secons Ltd. (www.obdtester.com)                       #
# Copyright 2009 Peter J. Creath                                       #
# Copyright 2016 Brendan Whitfield (brendan-w.com)                     #
# Copyright 2025 John E. Scott (john.s@elqo-algos.com)                 #
#                                                                      #
########################################################################
#                                                                      #
# async.py                                                             #
#                                                                      #
# This file is part of python-OBD (a derivative of pyOBD)              #
#                                                                      #
# python-OBD is free software: you can redistribute it and/or modify   #
# it under the terms of the GNU General Public License as published by #
# the Free Software Foundation, either version 2 of the License, or    #
# (at your option) any later version.                                  #
#                                                                      #
# python-OBD is distributed in the hope that it will be useful,        #
# but WITHOUT ANY WARRANTY; without even the implied warranty of       #
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        #
# GNU General Public License for more details.                         #
#                                                                      #
# You should have received a copy of the GNU General Public License    #
# along with python-OBD.  If not, see <http://www.gnu.org/licenses/>.  #
#                                                                      #
########################################################################

from __future__ import annotations

import asyncio
import contextlib
import functools
import logging
import threading
from collections.abc import Awaitable, Callable
from types import TracebackType
from typing import Any, Dict, Final, Optional, Type

import obd

__all__: Final = ["Async"]

# --------------------------------------------------------------------------- #
# Typing aliases                                                              #
# --------------------------------------------------------------------------- #
_OBDResponse = obd.OBDResponse
_Callback = Callable[[_OBDResponse], Awaitable[None] | None]
CommandLike = obd.OBDCommand | str

log = logging.getLogger(__name__)


def _ensure_coroutine(fn: _Callback) -> Callable[[_OBDResponse], Awaitable[None]]:
    """
    Wrap *fn* so that it can always be `await`-ed.
    Synchronous callbacks are executed in the loop's default executor.
    """
    if asyncio.iscoroutinefunction(fn):

        @functools.wraps(fn)
        async def _wrapper(resp: _OBDResponse) -> None:  # type: ignore[override]
            await fn(resp)

    else:

        @functools.wraps(fn)
        async def _wrapper(resp: _OBDResponse) -> None:  # type: ignore[override]
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, fn, resp)

    return _wrapper


class _PollItem:
    """
    Internal container that tracks one watched command, its callback and
    the asyncio.Task that performs the polling.
    """

    __slots__ = ("command", "callback", "task")

    def __init__(self, command: obd.OBDCommand, callback: _Callback):
        self.command: obd.OBDCommand = command
        self.callback: Callable[[_OBDResponse], Awaitable[None]] = _ensure_coroutine(
            callback
        )
        self.task: Optional[asyncio.Task[None]] = None


# =========================================================================== #
# Async class                                                                 #
# =========================================================================== #
class Async:
    """
    A modern, fully asyncio-based replacement for ``obd.Async``.

    Parameters
    ----------
    *args, **kw:
        Forwarded to ``obd.OBD`` constructor (port, baudrate, fast, …)
    loop:
        Optional event-loop to use.  Defaults to ``asyncio.get_event_loop()``.
    poll_interval:
        Seconds between successive polls of each watched command.
    use_executor:
        If True (default) every `conn.query` call runs in the default
        ThreadPoolExecutor.  Set to False when the transport backend offers
        an async `query_async` coroutine – we’ll call that directly.
    """

    # --------------------------------------------------------------------- #
    def __init__(
        self,
        *args: Any,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        poll_interval: float = 0.25,
        use_executor: bool = True,
        **kw: Any,
    ):
        self._loop: asyncio.AbstractEventLoop = loop or asyncio.get_event_loop()
        self._conn: obd.OBD = obd.OBD(*args, **kw)

        self._poll_interval: float = poll_interval
        self._use_executor: bool = use_executor

        self._running: asyncio.Event = asyncio.Event()
        self._stopped: asyncio.Event = asyncio.Event()
        self._tasks: set[asyncio.Task[None]] = set()
        self._watchlist: Dict[str, _PollItem] = {}

        # For start(blocking=True) support
        self._private_loop: Optional[asyncio.AbstractEventLoop] = None
        self._private_thread: Optional[threading.Thread] = None

    # =========================== context-manager ========================= #
    async def __aenter__(self) -> "Async":
        self.start()
        await self.wait_running()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        await self.stop()

    # =============================== API ================================= #
    def watch(
        self,
        cmd: CommandLike,
        callback: _Callback,
        *,
        force: bool = False,
    ) -> None:
        """
        Begin polling *cmd* and invoke *callback* with each new value.
        """
        command = _coerce_command(cmd)
        key = command.id

        if key in self._watchlist and not force:
            raise ValueError(f"Command {command} is already being watched")

        self._watchlist[key] = _PollItem(command, callback)

        # If connection already running schedule immediately
        if self.is_running:
            self._schedule_poll(self._watchlist[key])

    def unwatch(self, cmd: CommandLike) -> None:
        """
        Stop polling *cmd*.  Silent if command not currently watched.
        """
        command = _coerce_command(cmd)
        item = self._watchlist.pop(command.id, None)
        if item and item.task:
            item.task.cancel()

    # ----------------------- lifecycle ----------------------------------- #
    def start(self, *, blocking: bool = False) -> None:
        """
        Kick off background polling tasks.

        blocking=False (default): reuse whichever loop is already running.
        blocking=True            : spawn a dedicated event-loop in its own
                                   daemon thread so *synchronous* scripts can
                                   still call `.start()` without asyncio.
        """
        if self.is_running:
            return

        self._running.set()
        self._stopped.clear()

        # Ensure we have an event-loop to create tasks on
        if blocking:
            # Create a private loop in a daemon thread exactly once
            if self._private_loop is None:
                self._private_loop = asyncio.new_event_loop()

                def _run_loop(
                    loop: asyncio.AbstractEventLoop,
                ) -> None:  # pragma: no cover
                    asyncio.set_event_loop(loop)
                    loop.run_forever()

                self._private_thread = threading.Thread(
                    target=_run_loop, args=(self._private_loop,), daemon=True
                )
                self._private_thread.start()

            self._loop = self._private_loop  # future tasks use private loop

        # Schedule all currently watched commands
        for item in self._watchlist.values():
            self._schedule_poll(item)

    async def stop(self) -> None:
        """
        Cancel pollers, close the underlying OBD connection and shut down any
        private event-loop created by `start(blocking=True)`.
        """
        if not self.is_running:
            return

        self._running.clear()

        # Cancel polling tasks
        for t in list(self._tasks):
            t.cancel()

        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

        # Close the physical connection in executor to avoid blocking I/O
        await self._loop.run_in_executor(None, self._conn.close)

        # Teardown private loop if we created one
        if self._private_loop:
            self._private_loop.call_soon_threadsafe(self._private_loop.stop)
            if self._private_thread:
                self._private_thread.join()
            self._private_loop = None
            self._private_thread = None

        self._stopped.set()

    # ------------------------- status helpers ---------------------------- #
    @property
    def is_running(self) -> bool:
        return self._running.is_set()

    async def wait_running(self) -> None:
        await self._running.wait()

    async def wait_stopped(self) -> None:
        await self._stopped.wait()

    # ===================================================================== #
    # Internal helpers                                                      #
    # ===================================================================== #
    def _schedule_poll(self, item: _PollItem) -> None:
        """
        Create an asyncio.Task that loops forever (until cancelled), querying
        the command and forwarding the response to its callback.
        """

        async def _poll() -> None:
            cmd = item.command
            cb = item.callback
            interval = self._poll_interval
            conn = self._conn

            while self.is_running:
                try:
                    # Choose fastest query path
                    if not self._use_executor and hasattr(conn, "query_async"):
                        response: _OBDResponse = await conn.query_async(cmd)  # type: ignore[attr-defined]
                    else:
                        response = await self._loop.run_in_executor(
                            None, conn.query, cmd, False
                        )
                    await cb(response)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001
                    log.exception("Error while polling %s: %s", cmd, exc)

                await asyncio.sleep(interval)

        task = self._loop.create_task(_poll(), name=f"poll-{item.command}")
        task.add_done_callback(self._task_done)
        item.task = task
        self._tasks.add(task)

    # ------------------------------------------------------------------ #
    def _task_done(self, task: "asyncio.Task[None]") -> None:
        self._tasks.discard(task)
        with contextlib.suppress(asyncio.CancelledError):
            exc = task.exception()
            if exc:
                log.error("Polling task crashed: %s", exc, exc_info=exc)


# =========================================================================== #
# Utility functions                                                           #
# =========================================================================== #
def _coerce_command(cmd: CommandLike) -> obd.OBDCommand:
    """
    Accept either an ``obd.OBDCommand`` instance or a string such as "RPM".
    """
    if isinstance(cmd, obd.OBDCommand):
        return cmd
    if isinstance(cmd, str):
        return getattr(obd.commands, cmd.upper())
    raise TypeError("cmd must be str or OBDCommand")
