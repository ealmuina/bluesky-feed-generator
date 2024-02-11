import datetime
import logging
import os
import time

from atproto_client.client.client import Client
from atproto_client.exceptions import BadRequestError

from server.database import User

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def run(stop_event=None):
    while stop_event is None or not stop_event.is_set():
        client = Client()
        client.login(
            os.environ.get("STATISTICS_USER"),
            os.environ.get("STATISTICS_PASSWORD")
        )

        now = datetime.datetime.now()
        users = User.select().where(
            User.last_update.is_null(True)
            | (User.last_update <= now - datetime.timedelta(days=1))
        )
        users_count = users.count()

        for i, user in enumerate(users):
            try:
                profile = client.get_profile(user.did)
            except BadRequestError:
                # User account deleted
                user.delete()
                logger.info(f"[{i + 1}/{users_count}] Deleted user '{user.did}'")
                continue

            user.handle = profile.handle
            user.followers_count = profile.followers_count
            user.follows_count = profile.follows_count
            user.posts_count = profile.posts_count
            user.last_update = now

            user.save()
            logger.info(f"[{i + 1}/{users_count}] Updated data for user '{user.did}'")

        time.sleep(86400)  # 1 hour
        continue
