import lingua

from server.logger import logger
from server.database import db, Post, Language

detector = lingua.LanguageDetectorBuilder.from_all_languages().with_preloaded_language_models().build()


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

        # Bluesky user-tagged languages
        languages = {
            Language.get_or_create(code=lang)[0]
            for lang in created_post['record']['langs']
        }

        # Add automatically detected language if accuracy is high enough
        inlined_text = record.text.replace('\n', ' ')
        confidence_values = detector.compute_language_confidence_values(inlined_text)
        language, confidence = confidence_values[0]
        if confidence > 0.8:
            languages.add(
                Language.get_or_create(code=language.iso_code_639_1.name.lower())[0]
            )

        post_dict = {
            'uri': created_post['uri'],
            'cid': created_post['cid'],
            'reply_parent': reply_parent,
            'reply_root': reply_root,
            'languages': languages
        }
        posts_to_create.append(post_dict)

    posts_to_delete = [p['uri'] for p in ops['posts']['deleted']]
    if posts_to_delete:
        Post.delete().where(Post.uri.in_(posts_to_delete))
        logger.info(f'Deleted from feed: {len(posts_to_delete)}')

    if posts_to_create:
        with db.atomic():
            for post_dict in posts_to_create:
                languages = post_dict.pop("languages")
                post = Post.create(**post_dict)
                post.languages.add(list(languages))
        logger.info(f'Added to feed: {len(posts_to_create)}')
