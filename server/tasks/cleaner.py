import datetime
import time

from server.database import Post, Interaction


def run(stop_event=None):
    while stop_event is None or not stop_event.is_set():
        now = datetime.datetime.now()
        Post.delete().where(Post.indexed_at <= now - datetime.timedelta(days=7)).execute()
        Interaction.delete().where(Interaction.indexed_at <= now - datetime.timedelta(days=7)).execute()

        time.sleep(86400)  # 1 day
        continue
