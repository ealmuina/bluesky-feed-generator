from datetime import datetime
from itertools import cycle, chain

from dateutil import parser
from ftlangdetect.detect import get_or_load_model
from redis import Redis

from server.database import db, Post, Language, User, Interaction
from server.tasks import statistics
from server.utils import remove_emoji, remove_links

redis = Redis(host="redis")


def detect_language(text, user_languages):
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


def operations_callback(ops: dict) -> None:
    _process_posts(ops)
    _process_interactions(ops)


def _get_or_create_author(op, update_statistics=False):
    author_did = op["author"]
    author, _ = User.get_or_create(
        did=author_did
    )
    if update_statistics and not redis.sismember(statistics.QUEUE_INDEX, author_did):
        redis.sadd(statistics.QUEUE_INDEX, author_did)
        redis.lpush(statistics.QUEUE_NAME, author_did)
    return author


def _get_or_create_post(post_uri, post_cid):
    post, created = Post.get_or_create(
        uri=post_uri,
        defaults={
            "cid": post_cid,
        }
    )
    if not created:
        post.indexed_at = datetime.utcnow()
        post.save()
    return post


def _process_posts(ops):
    posts_to_create = []
    for created_post in ops['posts']['created']:
        record = created_post['record']

        reply_parent = None
        if record.reply and record.reply.parent.uri:
            reply_parent = record.reply.parent.uri

        reply_root = None
        if record.reply and record.reply.root.uri:
            reply_root = record.reply.root.uri

        # Get or create author
        author = _get_or_create_author(created_post, update_statistics=True)

        # Detect languages
        languages = record.langs or []
        languages = detect_language(record.text, languages)
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
            'created_at': parser.parse(record.created_at),
        }
        posts_to_create.append(post_dict)

    posts_to_delete = [p['uri'] for p in ops['posts']['deleted']]
    if posts_to_delete:
        Post.delete().where(Post.uri.in_(posts_to_delete))

    if posts_to_create:
        with db.atomic():
            for post_dict in posts_to_create:
                languages = post_dict.pop("languages")
                post = Post.create(**post_dict)
                post.languages = list(languages)


def _process_interactions(ops):
    interactions_to_create = []
    for interaction_type, created_interaction in chain(
            zip(cycle([Interaction.LIKE]), ops['likes']['created']),
            zip(cycle([Interaction.REPOST]), ops['reposts']['created'])
    ):
        record = created_interaction['record']

        # Get author and Post
        author = _get_or_create_author(created_interaction)
        post = _get_or_create_post(record.subject.uri, record.subject.cid)

        interaction_dict = {
            'author': author,
            'post': post,
            'uri': created_interaction['uri'],
            'cid': created_interaction['cid'],
            'interaction_type': interaction_type,
            'created_at': parser.parse(record.created_at),
        }
        interactions_to_create.append(interaction_dict)

    interactions_to_delete = [
        p['uri']
        for p in ops['likes']['deleted'] + ops['reposts']['deleted']
    ]
    if interactions_to_delete:
        Interaction.delete().where(Interaction.uri.in_(interactions_to_delete))

    if interactions_to_create:
        with db.atomic():
            for interaction_dict in interactions_to_create:
                Interaction.create(**interaction_dict)
