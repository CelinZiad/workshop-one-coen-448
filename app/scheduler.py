from __future__ import annotations

import time
import heapq
import threading
import itertools
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


class TaskHandle:
    """Handle used to cancel a scheduled task."""
    __slots__ = ("_id", "_cancelled", "_lock")

    def __init__(self, task_id: int):
        self._id = task_id
        self._cancelled = False
        self._lock = threading.Lock()

    @property
    def id(self) -> int:
        return self._id

    def cancel(self) -> None:
        with self._lock:
            self._cancelled = True

    def cancelled(self) -> bool:
        with self._lock:
            return self._cancelled


@dataclass(order=True)
class _ScheduledItem:
    run_at: float
    seq: int
    handle: TaskHandle = field(compare=False)
    func: Callable[..., Any] = field(compare=False)
    args: tuple[Any, ...] = field(compare=False, default_factory=tuple)
    kwargs: dict[str, Any] = field(compare=False, default_factory=dict)
    interval: Optional[float] = field(compare=False, default=None)


class TaskScheduler:
    """
    Basic delayed task scheduler with concurrent worker threads.
    """

    def __init__(self, workers: int = 4, max_queue: int = 0):
        if workers <= 0:
            raise ValueError("workers must be >= 1")

        self._workers = workers
        self._max_queue = max_queue

        self._cv = threading.Condition()
        self._heap: list[_ScheduledItem] = []
        self._seq = itertools.count()
        self._task_ids = itertools.count(1)

        self._work_cv = threading.Condition()
        self._work_q: list[_ScheduledItem] = []

        self._running = False
        self._shutdown = False
        self._dispatcher: Optional[threading.Thread] = None
        self._workers_threads: list[threading.Thread] = []

    def start(self) -> None:
        with self._cv:
            if self._running:
                return
            self._running = True

        self._dispatcher = threading.Thread(
            target=self._dispatcher_loop,
            name="Scheduler-Dispatcher",
            daemon=True,
        )
        self._dispatcher.start()

        for i in range(self._workers):
            t = threading.Thread(
                target=self._worker_loop,
                name=f"Scheduler-Worker-{i+1}",
                daemon=True,
            )
            self._workers_threads.append(t)
            t.start()

    def shutdown(self, wait: bool = True) -> None:
        with self._cv:
            self._shutdown = True
            self._cv.notify_all()

        with self._work_cv:
            self._work_cv.notify_all()

        if wait:
            if self._dispatcher:
                self._dispatcher.join()
            for t in self._workers_threads:
                t.join()

    def schedule(
        self,
        func: Callable[..., Any],
        *,
        delay: Optional[float] = None,
        run_at: Optional[float] = None,
        interval: Optional[float] = None,
        args: tuple[Any, ...] = (),
        kwargs: Optional[dict[str, Any]] = None,
    ) -> TaskHandle:
        if kwargs is None:
            kwargs = {}

        if delay is not None and run_at is not None:
            raise ValueError("Use either delay or run_at, not both")

        if delay is None and run_at is None:
            run_at = time.time()
        elif delay is not None:
            run_at = time.time() + delay

        if interval is not None and interval <= 0:
            raise ValueError("interval must be > 0")

        handle = TaskHandle(next(self._task_ids))

        item = _ScheduledItem(
            run_at=run_at,
            seq=next(self._seq),
            handle=handle,
            func=func,
            args=args,
            kwargs=kwargs,
            interval=interval,
        )

        with self._cv:
            if self._shutdown:
                raise RuntimeError("Scheduler is shut down")
            heapq.heappush(self._heap, item)
            self._cv.notify_all()

        return handle

    def _dispatcher_loop(self) -> None:
        while True:
            with self._cv:
                if self._shutdown:
                    return

                if not self._heap:
                    self._cv.wait(timeout=0.5)
                    continue

                now = time.time()
                next_item = self._heap[0]

                if next_item.run_at > now:
                    self._cv.wait(timeout=next_item.run_at - now)
                    continue

                item = heapq.heappop(self._heap)

            with self._work_cv:
                while (
                    not self._shutdown
                    and self._max_queue > 0
                    and len(self._work_q) >= self._max_queue
                ):
                    self._work_cv.wait()

                if self._shutdown:
                    return

                self._work_q.append(item)
                self._work_cv.notify()

    def _worker_loop(self) -> None:
        while True:
            with self._work_cv:
                while not self._shutdown and not self._work_q:
                    self._work_cv.wait()

                if self._shutdown:
                    return

                item = self._work_q.pop(0)
                self._work_cv.notify_all()

            if item.handle.cancelled():
                continue

            try:
                item.func(*item.args, **item.kwargs)
            except Exception as e:
                print(f"[Scheduler] Task {item.handle.id} failed: {e}")

            if item.interval and not item.handle.cancelled():
                next_run = item.run_at + item.interval
                new_item = _ScheduledItem(
                    run_at=next_run,
                    seq=next(self._seq),
                    handle=item.handle,
                    func=item.func,
                    args=item.args,
                    kwargs=item.kwargs,
                    interval=item.interval,
                )
                with self._cv:
                    if not self._shutdown:
                        heapq.heappush(self._heap, new_item)
                        self._cv.notify_all()
