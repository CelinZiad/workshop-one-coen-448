import threading
import time

import pytest
from mockito import spy, unstub, verify, when

from app import scheduler as scheduler_module
from app.scheduler import TaskScheduler


@pytest.fixture(autouse=True)
def _cleanup_mocks():
    yield
    unstub()


def test_schedule_uses_mocked_time_for_delay_calculation():
    scheduler = TaskScheduler(workers=1)
    when(scheduler_module.time).time().thenReturn(100.0)

    scheduler.schedule(lambda: None, delay=2.5)

    assert scheduler._heap[0].run_at == pytest.approx(102.5)


def test_invalid_schedule_inputs_rejected():
    scheduler = TaskScheduler(workers=1)

    with pytest.raises(ValueError):
        scheduler.schedule(lambda: None, delay=1, run_at=10)

    with pytest.raises(ValueError):
        scheduler.schedule(lambda: None, interval=0)

    with pytest.raises(ValueError):
        scheduler.schedule(lambda: None, interval=-0.1)


def test_delayed_execution_waits_until_due():
    scheduler = TaskScheduler(workers=1)
    fired = threading.Event()
    timestamps = {}

    def task():
        timestamps["ran_at"] = time.monotonic()
        fired.set()

    scheduler.start()
    try:
        delay = 0.1
        start = time.monotonic()
        scheduler.schedule(task, delay=delay)

        assert fired.wait(timeout=1.0)
        elapsed = timestamps["ran_at"] - start
        assert elapsed >= delay * 0.8
    finally:
        scheduler.shutdown(wait=True)


def test_concurrent_dispatch_with_multiple_workers():
    scheduler = TaskScheduler(workers=2)
    started = [threading.Event(), threading.Event()]
    release = threading.Event()

    def make_task(index: int):
        def task():
            started[index].set()
            release.wait(timeout=1.0)
        return task

    scheduler.start()
    try:
        scheduler.schedule(make_task(0), delay=0)
        scheduler.schedule(make_task(1), delay=0)

        assert started[0].wait(timeout=1.0)
        assert started[1].wait(timeout=1.0)
        release.set()
    finally:
        scheduler.shutdown(wait=True)


def test_cancellation_prevents_execution():
    scheduler = TaskScheduler(workers=1)
    task_spy = spy(lambda: None)

    scheduler.start()
    try:
        handle = scheduler.schedule(task_spy, delay=0.2)
        handle.cancel()
        time.sleep(0.3)

        verify(task_spy, times=0).__call__()
    finally:
        scheduler.shutdown(wait=True)


def test_cancel_repeating_task_stops_reschedule():
    scheduler = TaskScheduler(workers=1)
    first_run = threading.Event()
    finish = threading.Event()
    run_count = {"count": 0}
    lock = threading.Lock()

    def task():
        with lock:
            run_count["count"] += 1
        first_run.set()
        finish.wait(timeout=1.0)

    scheduler.start()
    try:
        handle = scheduler.schedule(task, delay=0, interval=0.05)
        assert first_run.wait(timeout=1.0)
        handle.cancel()
        finish.set()
        time.sleep(0.2)

        with lock:
            assert run_count["count"] == 1
    finally:
        scheduler.shutdown(wait=True)
