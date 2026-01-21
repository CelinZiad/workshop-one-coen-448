import threading
import time

import pytest
from mockito import spy, verify

from app.scheduler import TaskScheduler


@pytest.fixture()
def scheduler():
    sched = TaskScheduler(workers=2)
    sched.start()
    yield sched
    sched.shutdown(wait=True)


def wait_for(predicate, timeout=0.5, interval=0.005):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if predicate():
            return True
        time.sleep(interval)
    return False


def test_invalid_workers():
    with pytest.raises(ValueError):
        TaskScheduler(workers=0)


def test_schedule_with_both_delay_and_run_at_raises(scheduler):
    with pytest.raises(ValueError):
        scheduler.schedule(lambda: None, delay=0.01, run_at=time.time())


def test_interval_must_be_positive(scheduler):
    with pytest.raises(ValueError):
        scheduler.schedule(lambda: None, delay=0.01, interval=0)


def test_schedule_after_shutdown_raises():
    sched = TaskScheduler()
    sched.start()
    sched.shutdown(wait=True)
    with pytest.raises(RuntimeError):
        sched.schedule(lambda: None)


def test_scheduled_tasks_execute_in_order(scheduler):
    executed = []
    lock = threading.Lock()
    done = threading.Event()

    def record(label):
        with lock:
            executed.append(label)
            if len(executed) == 2:
                done.set()

    scheduler.schedule(record, delay=0.02, args=("A",))
    scheduler.schedule(record, delay=0.01, args=("B",))

    assert done.wait(timeout=0.5)
    assert executed == ["B", "A"]


def test_cancelled_task_does_not_run(scheduler):
    def task():
        return None

    task_spy = spy(task)
    handle = scheduler.schedule(task_spy, delay=0.05)
    handle.cancel()

    time.sleep(0.1)
    verify(task_spy, times=0)()


def test_interval_task_runs_multiple_times(scheduler):
    count_lock = threading.Lock()
    count = {"value": 0}
    done = threading.Event()
    handle_holder = {}

    def tick():
        with count_lock:
            count["value"] += 1
            if count["value"] >= 3:
                handle_holder["handle"].cancel()
                done.set()

    handle = scheduler.schedule(tick, delay=0.01, interval=0.02)
    handle_holder["handle"] = handle
    assert done.wait(timeout=0.5)

    with count_lock:
        completed = count["value"]

    time.sleep(0.05)
    with count_lock:
        assert count["value"] == completed


def test_failure_does_not_stop_other_tasks(scheduler, capsys):
    completed = threading.Event()

    def fail():
        raise RuntimeError("boom")

    def succeed():
        completed.set()

    scheduler.schedule(fail, delay=0.01)
    scheduler.schedule(succeed, delay=0.02)

    assert completed.wait(timeout=0.5)
    captured = capsys.readouterr()
    assert "failed" in captured.out


def test_concurrent_tasks_complete(scheduler):
    count_lock = threading.Lock()
    count = {"value": 0}
    done = threading.Event()

    def task():
        with count_lock:
            count["value"] += 1
            if count["value"] == 6:
                done.set()

    for _ in range(6):
        scheduler.schedule(task, delay=0.01)

    assert done.wait(timeout=0.5)
