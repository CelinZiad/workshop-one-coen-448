import time
from app.scheduler import TaskScheduler


def hello(name: str) -> None:
    print(f"{time.strftime('%H:%M:%S')} hello {name}")


def main():
    scheduler = TaskScheduler(workers=3)
    scheduler.start()

    scheduler.schedule(hello, delay=2, args=("A",))
    scheduler.schedule(hello, delay=1, args=("B",))

    repeating = scheduler.schedule(
        hello,
        delay=0.5,
        interval=1.5,
        args=("repeat",),
    )

    time.sleep(5)
    repeating.cancel()

    scheduler.shutdown(wait=True)


if __name__ == "__main__":
    main()
