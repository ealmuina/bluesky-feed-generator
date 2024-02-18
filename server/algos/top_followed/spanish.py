from typing import Optional

from server import config
from server.algos import base
from server.database import Post, Language, User

uri = config.TOP_SPANISH_URI


def handler(cursor: Optional[str], limit: int, requester_did: str) -> dict:
    language = Language.get(Language.code == "es")

    posts = language.posts.join(
        User, on=(Post.author == User.id)
    ).where(
        Post.reply_root.is_null(True),
        Post.created_at.is_null(False),
        User.followers_count > 500,
    ).order_by(
        Post.created_at.desc(),
        Post.cid.desc(),
    ).limit(limit)

    return base.handler(cursor, posts)
