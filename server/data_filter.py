import lingua

from server.database import db, Post, Language

detector = lingua.LanguageDetectorBuilder.from_all_languages().with_low_accuracy_mode().build()


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
        languages = created_post['record'].langs or []

        # Automatically detected languages
        inlined_text = record.text.replace('\n', ' ')
        confidence_values = detector.compute_language_confidence_values(inlined_text)
        language, confidence = confidence_values[0]

        if confidence > 0.8:
            languages = [language.iso_code_639_1.name.lower()]

        languages = {
            Language.get_or_create(code=lang)[0]
            for lang in languages
        }

        post_dict = {
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
                post.languages.add(list(languages))
