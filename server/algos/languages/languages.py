from typing import Optional

from server.algos import base
from server.database import Post, Language

CURSOR_EOF = "eof"


def handler(language_code: str, cursor: Optional[str], limit: int) -> dict:
    language = Language.get(Language.code == language_code)

    posts = language.posts.where(
        Post.reply_root.is_null(True)
    ).order_by(
        Post.indexed_at.desc(),
        Post.cid.desc(),
    ).limit(limit)

    return base.handler(cursor, posts)
