import gcld3
from redis import Redis

from server.database import db, Post, Language, User
from server.tasks import statistics

detector = gcld3.NNetLanguageIdentifier(min_num_bytes=0, max_num_bytes=1000)
redis = Redis(host="redis")


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

        # Automatically detected languages
        # inlined_text = record.text.replace('\n', ' ')
        # detection_result = detector.FindLanguage(text=inlined_text)
        #
        # if not inlined_text.strip():
        #     languages = []
        #
        # if detection_result.is_reliable:
        #     languages = [detection_result.language]

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
