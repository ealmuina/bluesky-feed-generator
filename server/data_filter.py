import logging
import pickle
from collections import defaultdict
from datetime import datetime
from multiprocessing import Process

from atproto import models
from dateutil import parser
from ftlangdetect.detect import get_or_load_model
from redis import Redis

from server.database import Language, Post, db, User, PostLanguage
from server.tasks import statistics
from server.utils import remove_emoji, remove_links

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

QUEUE_NAME = "bsky-posts"
POSTS_BATCH_SIZE = 100


class PostProcessor(Process):
    def __init__(self):
        super().__init__()

        self.redis = Redis(host="redis")
        self.posts_queue = []
        self.interactions_queue = []

    @staticmethod
    def _detect_language(text, user_languages):
        user_languages = map(str.lower, user_languages)
        user_languages = [
            language.split("-")[0] for language in user_languages
        ]

        text = text.replace('\n', '. ').strip()
        text = remove_emoji(text)
        text = remove_links(text)

        if not text:
            return user_languages

        model = get_or_load_model(low_memory=False)
        labels, scores = model.predict(text, k=5)
        language_prob = {
            lang.replace("__label__", ''): score
            for lang, score in zip(labels, scores)
        }

        # Confirm user tag
        # if its confidence is higher than 0.15
        text_languages = []
        for language in user_languages:
            prob = language_prob.get(language, 0)
            if prob > 0.15:
                text_languages.append(language)
        if text_languages:
            return text_languages

        # Set model-detected language
        # if its confidence is higher than 0.7
        best_match = labels[0].replace("__label__", '')
        best_score = scores[0]
        if best_score > 0.7:
            return best_match

        # Language uncertain
        return []

    @staticmethod
    def _get_or_create_post(post_uri, post_cid):
        post, created = Post.get_or_create(
            uri=post_uri,
            defaults={
                "cid": post_cid,
                "created_at": datetime.utcnow(),
            }
        )
        if not created:
            post.indexed_at = datetime.utcnow()
            post.save()
        return post

    def _get_or_create_author(self, op, update_statistics=False):
        author_did = op["author"]
        author, _ = User.get_or_create(
            did=author_did
        )
        if update_statistics and not self.redis.sismember(statistics.QUEUE_INDEX, author_did):
            self.redis.sadd(statistics.QUEUE_INDEX, author_did)
            self.redis.lpush(statistics.QUEUE_NAME, author_did)
        return author

    def _process_posts(self, ops):
        created_posts = ops.get(models.ids.AppBskyFeedPost, {}).get("created", [])
        self.posts_queue.extend(created_posts)

        if len(self.posts_queue) > POSTS_BATCH_SIZE:
            posts_to_create = []
            post_languages = []

            for created_post in self.posts_queue:
                record = created_post['record']

                reply_parent = None
                if record.reply and record.reply.parent.uri:
                    reply_parent = record.reply.parent.uri

                reply_root = None
                if record.reply and record.reply.root.uri:
                    reply_root = record.reply.root.uri

                # Get or create author
                author = self._get_or_create_author(created_post, update_statistics=True)

                post_dict = {
                    "uri": created_post["uri"],
                    "author": author,
                    "cid": created_post["cid"],
                    "reply_parent": reply_parent,
                    "reply_root": reply_root,
                    "created_at": parser.parse(record.created_at),
                }
                posts_to_create.append(post_dict)

                # Detect languages
                languages = record.langs or []
                languages = self._detect_language(record.text, languages)
                languages = {
                    Language.get_or_create(code=lang)[0]
                    for lang in languages
                }
                for language in languages:
                    post_languages.append(
                        {
                            "post_id": created_post["uri"],
                            "language_id": language.id,
                        }
                    )

            with db.atomic():
                Post.insert_many(posts_to_create).on_conflict_ignore().execute()
                PostLanguage.insert_many(post_languages).on_conflict_ignore().execute()

            self.posts_queue.clear()

        posts_to_delete = ops.get(models.ids.AppBskyFeedPost, {}).get("deleted", [])
        posts_to_delete = [p['uri'] for p in posts_to_delete]

        if posts_to_delete:
            Post.delete().where(Post.uri.in_(posts_to_delete))

    def run(self):
        # Create separate DB connection for the process
        db.close()
        db.connect()

        while True:
            _, ops = self.redis.brpop(QUEUE_NAME)
            ops = pickle.loads(ops)
            try:
                self._process_posts(ops)
            except Exception as e:
                logger.warning(e)


def operations_callback(ops: defaultdict) -> None:
    ops = dict(ops)
    redis = Redis(host="redis")
    redis.lpush(QUEUE_NAME, pickle.dumps(ops))
