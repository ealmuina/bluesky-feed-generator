from ftlangdetect.detect import get_or_load_model
from redis import Redis

from server.database import db, Post, Language, User
from server.tasks import statistics
from server.utils import remove_emoji, remove_links

redis = Redis(host="redis")


def detect_language(text, user_languages):
    if not text:
        return user_languages

    model = get_or_load_model(low_memory=False)
    labels, scores = model.predict(text, k=5)
    language_prob = {
        lang.replace("__label__", ''): score
        for lang, score in zip(labels, scores)
    }

    text_languages = []
    for language in user_languages:
        prob = language_prob.get(language, 0)
        if prob > 0.05:
            text_languages.append(language)

    return text_languages or [labels[0].replace("__label__", '')]


def operations_callback(ops: dict) -> None:
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
        author_did = created_post["author"]
        author, _ = User.get_or_create(
            did=author_did
        )
        redis.lpush(statistics.QUEUE_NAME, author_did)

        # Bluesky user-tagged languages
        languages = created_post['record'].langs or []

        # Detect language
        inlined_text = record.text.replace('\n', '. ').strip()
        inlined_text = remove_emoji(inlined_text)
        inlined_text = remove_links(inlined_text)
        languages = detect_language(inlined_text, languages)

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
