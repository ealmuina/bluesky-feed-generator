from datetime import datetime
from typing import Optional

from server import config
from server.algos import base
from server.database import Post, Language, User

uri = config.TOP_SPANISH_URI


class TopSpanishAlgorithm:
    def __init__(self, min_followers=1000):
        self.language = Language.get_or_none(Language.code == "es")
        self.min_followers = min_followers

    def _get_posts_from_top_accounts(self, limit, created_at, cid):
        posts = self.language.posts.select(
            Post.id,
            Post.uri,
            Post.cid,
            Post.created_at,
        ).join(
            User, on=(Post.author == User.id)
        ).where(
            Post.reply_root.is_null(True),
            Post.created_at <= datetime.utcnow(),
            User.followers_count >= self.min_followers,
        ).order_by(
            Post.created_at.desc(),
            Post.cid.desc(),
        ).limit(limit)

        if created_at:
            posts = posts.where(
                (Post.created_at < created_at)
                | ((Post.created_at == created_at) & (Post.cid < cid))
            )
        return posts

    def handle(self, cursor: Optional[str], limit: int, requester_did: str) -> dict:
        created_at, cid = None, None
        if cursor:
            if cursor == base.CURSOR_EOF:
                return {
                    'cursor': base.CURSOR_EOF,
                    'feed': []
                }
            cursor_parts = cursor.split('::')
            if len(cursor_parts) != 2:
                raise ValueError('Malformed cursor')

            created_at, cid = cursor_parts
            created_at = datetime.fromtimestamp(int(created_at) / 1000)

        posts = self._get_posts_from_top_accounts(limit, created_at, cid)

        feed = []
        for post in posts:
            feed_entry = {'post': post['uri']}

            if repost_uri := post.get('repost_uri'):
                feed_entry['reason'] = {
                    '$type': 'app.bsky.feed.defs#skeletonReasonRepost',
                    'repost': repost_uri,
                }

            feed.append(feed_entry)

        cursor = base.CURSOR_EOF
        last_post = posts[-1] if posts else None
        if last_post:
            cursor = f'{int(last_post["created_at"].timestamp() * 1000)}::{last_post["cid"]}'

        return {
            'cursor': cursor,
            'feed': feed
        }
