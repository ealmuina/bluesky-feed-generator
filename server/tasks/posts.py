import logging
from datetime import datetime
from itertools import chain, cycle

from atproto import models
from dateutil import parser
from ftlangdetect.detect import get_or_load_model
from redis import Redis

from server.celery_config import app
from server.database import Language, Post, db, Interaction, User
from server.tasks import statistics
from server.tasks.base import BaseCeleryTask
from server.tasks.statistics import update_user_statistics
from server.utils import remove_emoji, remove_links

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

redis = Redis(host="redis")


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


def _get_or_create_author(op, update_statistics=False):
    author_did = op["author"]
    author, _ = User.get_or_create(
        did=author_did
    )
    if update_statistics and not redis.sismember(statistics.QUEUE_INDEX, author_did):
        redis.sadd(statistics.QUEUE_INDEX, author_did)
        update_user_statistics.delay(author_did)
    return author


def _process_posts(ops):
    created_posts = ops[models.ids.AppBskyFeedPost]["created"]

    posts_to_create = []
    for created_post in created_posts:
        reply_parent = created_post["record"]["reply_parent"]
        reply_root = created_post["record"]["reply_root"]

        # Get or create author
        author = _get_or_create_author(created_post, update_statistics=True)

        # Detect languages
        languages = created_post["record"]["langs"] or []
        languages = _detect_language(created_post["record"]["text"], languages)
        languages = {
            Language.get_or_create(code=lang)[0]
            for lang in languages
        }

        post_dict = {
            'author': author,
            'uri': created_post['uri'],
            'cid': created_post['cid'],
            'reply_parent': reply_parent,
            'reply_root': reply_root,
            'languages': languages,
            'created_at': parser.parse(created_post["record"]["created_at"]),
        }
        posts_to_create.append(post_dict)

    posts_to_delete = ops[models.ids.AppBskyFeedPost]["deleted"]
    posts_to_delete = [p['uri'] for p in posts_to_delete]

    if posts_to_delete:
        Post.delete().where(Post.uri.in_(posts_to_delete))

    if posts_to_create:
        with db.atomic():
            for post_dict in posts_to_create:
                languages = post_dict.pop("languages")
                post = Post.create(**post_dict)
                post.languages = list(languages)


def _process_interactions(ops):
    created_likes = ops[models.ids.AppBskyFeedLike]["created"]
    created_reposts = ops[models.ids.AppBskyFeedRepost]["created"]

    interactions_to_create = []
    for interaction_type, created_interaction in chain(
            zip(cycle([Interaction.LIKE]), created_likes),
            zip(cycle([Interaction.REPOST]), created_reposts)
    ):
        # Get author and Post
        author = _get_or_create_author(created_interaction)
        post = _get_or_create_post(
            created_interaction["record"]["subject"]["uri"],
            created_interaction["record"]["subject"]["cid"]
        )

        interaction_dict = {
            'author': author,
            'post': post,
            'uri': created_interaction['uri'],
            'cid': created_interaction['cid'],
            'interaction_type': interaction_type,
            'created_at': parser.parse(created_interaction["record"]["created_at"]),
        }
        interactions_to_create.append(interaction_dict)

    likes_to_delete = ops[models.ids.AppBskyFeedLike]["deleted"]
    reposts_to_delete = ops[models.ids.AppBskyFeedRepost]["deleted"]
    interactions_to_delete = [
        p['uri']
        for p in likes_to_delete + reposts_to_delete
    ]
    if interactions_to_delete:
        Interaction.delete().where(Interaction.uri.in_(interactions_to_delete))

    if interactions_to_create:
        with db.atomic():
            for interaction_dict in interactions_to_create:
                Interaction.create(**interaction_dict)


@app.task(base=BaseCeleryTask, bind=True)
def process_events(self, ops):
    try:
        _process_posts(ops)
        _process_interactions(ops)
    except Exception as e:
        logger.warning(e)
