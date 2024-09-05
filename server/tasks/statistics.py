import datetime
import logging
import os
from multiprocessing import Process

from atproto_client.client.client import Client
from redis import Redis

from server.database import User, db

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

QUEUE_NAME = "bsky-statistics"
QUEUE_INDEX = "bsky-statistics-index"


class StatisticsUpdater(Process):
    def __init__(self):
        super().__init__()

        self.client = Client()
        self.client.login(
            os.environ.get("STATISTICS_USER"),
            os.environ.get("STATISTICS_PASSWORD")
        )
        self.redis = Redis(host="redis")

    def run(self):
        # Create separate DB connection for the process
        db.close()
        db.connect()

        while True:
            _, user_did = self.redis.brpop(QUEUE_NAME)
            user_did = user_did.decode()
            self.redis.srem(QUEUE_INDEX, user_did)

            try:
                now = datetime.datetime.now()
                user = User.get(did=user_did)

                if user.last_update is None or user.last_update < now - datetime.timedelta(days=1):
                    profile = self.client.get_profile(user_did)

                    user.handle = profile.handle
                    user.followers_count = profile.followers_count
                    user.follows_count = profile.follows_count
                    user.posts_count = profile.posts_count
                    user.last_update = now

                    user.save()

            except Exception as e:
                logger.warning(f"Error updating statistics for DID '{user_did}': {e}")

            logger.info(f"{self.redis.llen(QUEUE_NAME)} users pending for update")
