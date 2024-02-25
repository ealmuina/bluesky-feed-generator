import datetime
import time

from server.database import Post, Interaction, PostLanguage, FeedCache


def run(stop_event=None):
    while stop_event is None or not stop_event.is_set():
        now = datetime.datetime.now()

        # Post language tags
        PostLanguage.delete().where(
            PostLanguage.post.in_(
                Post.select(Post.id).where(Post.indexed_at <= now - datetime.timedelta(days=7))
            )
        ).execute()

        # Interactions
        Interaction.delete().where(Interaction.indexed_at <= now - datetime.timedelta(days=7)).execute()

        # Posts
        Post.delete().where(Post.indexed_at <= now - datetime.timedelta(days=7)).execute()

        # Cached feed entries
        FeedCache.delete().where(FeedCache.created_at <= now - datetime.timedelta(days=7)).execute()

        time.sleep(86400)  # 1 day
        continue
