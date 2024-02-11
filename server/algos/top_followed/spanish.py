from typing import Optional

from server import config
from server.algos import base
from server.database import Post, Language, User

uri = config.TOP_SPANISH_URI


def handler(cursor: Optional[str], limit: int) -> dict:
    language = Language.get(Language.code == "es")

    posts = language.posts.join(
        User
    ).where(
        Post.reply_root.is_null(True),
        User.followers_count > 1000,
    ).order_by(
        Post.indexed_at.desc(),
        Post.cid.desc(),
    ).limit(limit)

    return base.handler(cursor, posts)
