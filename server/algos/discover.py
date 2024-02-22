import os
from datetime import datetime
from typing import Optional

import peewee
from atproto_client.client.client import Client
from peewee import fn

from server import config
from server.algos import base
from server.database import Post, User, Interaction
from server.utils import nth_item

uri = config.DISCOVER_URI


class DiscoverAlgorithm:
    def __init__(self, min_likes=3):
        self.client = Client()
        self.client.login(
            os.environ.get("STATISTICS_USER"),
            os.environ.get("STATISTICS_PASSWORD"),
        )
        self.min_likes = min_likes

    def _get_posts_from_likes(self, limit, created_at, cid, user_follows_dids, requester_did):
        InteractionUser = User.alias()
        PostUser = User.alias()

        posts = Post.select(
            Post.id,
            Post.uri,
            Post.cid,
            nth_item(Interaction.created_at, self.min_likes).alias("created_at"),
            nth_item(InteractionUser.did, self.min_likes).alias("like_by_did"),
        ).join(
            Interaction, on=(Interaction.post == Post.id)
        ).join(
            InteractionUser, on=(InteractionUser.id == Interaction.author)
        ).join(
            PostUser, on=(PostUser.id == Post.author)
        ).where(
            Post.created_at <= datetime.utcnow(),
            Interaction.interaction_type == Interaction.LIKE,
            Interaction.created_at <= datetime.utcnow(),
            InteractionUser.did.in_(user_follows_dids),
            PostUser.did != requester_did,
            (
                    PostUser.did.not_in(user_follows_dids)
                    | Post.reply_parent.is_null(False)
            )
        ).group_by(
            Post.id,
            Post.uri,
            Post.cid,
        ).having(
            fn.COUNT(Interaction.author.distinct()) >= self.min_likes,
        ).order_by(
            peewee.SQL("created_at DESC"),
            Post.cid.desc(),
        ).limit(limit)

        if created_at:
            posts = posts.where(
                (Interaction.created_at < created_at)
                | ((Interaction.created_at == created_at) & (Interaction.cid < cid))
            )

        return posts

    def _get_user_follows_dids(self, requester_did):
        user_follows_dids = []
        cursor = None
        while True:
            user_follows_response = self.client.get_follows(requester_did, cursor=cursor, limit=100)
            user_follows_dids.extend([
                profile_data.did
                for profile_data in user_follows_response.follows
            ])
            cursor = user_follows_response.cursor
            if not cursor:
                break
        return user_follows_dids

    def handle(self, cursor: Optional[str], limit: int, requester_did: str) -> dict:
        user_follows_dids = self._get_user_follows_dids(requester_did)
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

        posts = self._get_posts_from_likes(limit, created_at, cid, user_follows_dids, requester_did)
        posts = list(posts.dicts())

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
