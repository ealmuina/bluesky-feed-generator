from datetime import datetime
from typing import Optional

from server import config
from server.database import Post, Language

CURSOR_EOF = "eof"
uri = config.SPANISH_URI


def handler(cursor: Optional[str], limit: int) -> dict:
    language = Language.get(Language.code == "es")

    posts = language.posts.where(
        Post.reply_root.is_null(True)
    ).order_by(
        Post.indexed_at.desc(),
        Post.cid.desc(),
    ).limit(limit)

    if cursor:
        if cursor == CURSOR_EOF:
            return {
                'cursor': CURSOR_EOF,
                'feed': []
            }
        cursor_parts = cursor.split('::')
        if len(cursor_parts) != 2:
            raise ValueError('Malformed cursor')

        indexed_at, cid = cursor_parts
        indexed_at = datetime.fromtimestamp(int(indexed_at) / 1000)
        posts = posts.where(
            (Post.indexed_at < indexed_at)
            | ((Post.indexed_at == indexed_at) & (Post.cid < cid))
        )

    feed = [{'post': post.uri} for post in posts]

    cursor = CURSOR_EOF
    last_post = posts[-1] if posts else None
    if last_post:
        cursor = f'{int(last_post.indexed_at.timestamp() * 1000)}::{last_post.cid}'

    return {
        'cursor': cursor,
        'feed': feed
    }

    return algo_languages.handler(
        language_code="es",
        cursor=cursor,
        limit=limit,
    )
