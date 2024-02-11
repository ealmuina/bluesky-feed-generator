import datetime
import logging
import os
import time
from threading import Thread

from redis import Redis
from atproto_client.client.client import Client
from atproto_client.exceptions import BadRequestError

from server.database import User

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

QUEUE_NAME = "bsky-statistics"


class StatisticsUpdater(Thread):
    def __init__(self):
        super().__init__()

        self.client = Client()
        self.client.login(
            os.environ.get("STATISTICS_USER"),
            os.environ.get("STATISTICS_PASSWORD")
        )
        self.redis = Redis(host="redis")

    def run(self, stop_event=None):
        while stop_event is None or not stop_event.is_set():
            _, user_did = self.redis.brpop(QUEUE_NAME)
            user_did = user_did.decode()
            try:
                profile = self.client.get_profile(user_did)

                user = User.get(did=user_did)
                user.handle = profile.handle
                user.followers_count = profile.followers_count
                user.follows_count = profile.follows_count
                user.posts_count = profile.posts_count
                user.last_update = datetime.datetime.now()

                user.save()
            except Exception:
                logger.exception(f"Error updating statistics for DID: {user_did}", exc_info=True)
