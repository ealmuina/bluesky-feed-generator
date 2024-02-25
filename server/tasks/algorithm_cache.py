import threading
import time

from server.algos.top_followed.spanish import TopSpanishAlgorithm

CACHED_ALGORITHMS = [
    TopSpanishAlgorithm
]


def run(stop_event=None):
    for algo in CACHED_ALGORITHMS:
        threading.Thread(
            target=run_thread, args=(algo(), stop_event)
        ).start()


def run_thread(algo, stop_event=None):
    while stop_event is None or not stop_event.is_set():
        algo.populate_cache()
        time.sleep(5)
        continue
