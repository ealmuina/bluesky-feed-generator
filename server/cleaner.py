import datetime
import time

from server.database import Post


def run(stop_event=None):
    while stop_event is None or not stop_event.is_set():
        now = datetime.datetime.now()
        Post.delete().where(Post.indexed_at <= now - datetime.timedelta(days=7))

        time.sleep(86400)  # 1 day
        continue
