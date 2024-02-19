import os
from datetime import datetime
from typing import Optional

import peewee
from atproto_client.client.client import Client
from peewee import fn

from server import config
from server.algos import base
from server.database import Post, User, Interaction
from server.utils import nth_item, last_item

uri = config.FOLLOWING_PLUS_URI


class FollowingPlusAlgorithm:
    def __init__(self, min_likes=3):
        self.client = Client()
        self.client.login(
            os.environ.get("STATISTICS_USER"),
            os.environ.get("STATISTICS_PASSWORD"),
        )
        self.min_likes = min_likes

    def _get_posts_from_follows(self, limit, created_at, cid, user_follows_dids):
        posts = Post.select(
            Post.id,
            Post.uri,
            Post.cid,
            Post.created_at,
        ).join(
            User, on=(User.id == Post.author)
        ).where(
            User.did.in_(user_follows_dids),
            Post.reply_root.is_null(True),
            Post.created_at <= datetime.utcnow(),
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

    def _get_posts_from_likes(self, limit, created_at, cid, user_follows_dids):
        posts = Post.select(
            Post.id,
            Post.uri,
            Post.cid,
            nth_item(Interaction.created_at, self.min_likes).alias("created_at"),
            nth_item(User.did, self.min_likes).alias("like_by_did"),
        ).join(
            Interaction, on=(Interaction.post == Post.id)
        ).join(
            User, on=(User.id == Interaction.author)
        ).where(
            Post.created_at <= datetime.utcnow(),
            Interaction.interaction_type == Interaction.LIKE,
            Interaction.created_at <= datetime.utcnow(),
            User.did.in_(user_follows_dids),
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

    def _get_posts_from_reposts(self, limit, created_at, cid, user_follows_dids):
        posts = Post.select(
            Post.id,
            Post.uri,
            Post.cid,
            last_item(Interaction.created_at).alias("created_at"),
            last_item(Interaction.uri).alias("repost_uri"),
        ).join(
            Interaction, on=(Interaction.post == Post.id)
        ).join(
            User, on=(User.id == Interaction.author)
        ).where(
            Post.created_at <= datetime.utcnow(),
            Interaction.interaction_type == Interaction.REPOST,
            Interaction.created_at <= datetime.utcnow(),
            User.did.in_(user_follows_dids),
        ).group_by(
            Post.id,
            Post.uri,
            Post.cid,
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

        posts_from_follows = self._get_posts_from_follows(limit, created_at, cid, user_follows_dids + [requester_did])
        posts_from_reposts = self._get_posts_from_reposts(limit, created_at, cid, user_follows_dids + [requester_did])
        posts_from_likes = self._get_posts_from_likes(limit, created_at, cid, user_follows_dids)
        posts_combined = sorted(
            [
                *map(
                    lambda p: {"expand_thread": True, **p},
                    posts_from_follows.dicts()
                ),
                *posts_from_reposts.dicts(),
                *posts_from_likes.dicts(),
            ],
            key=lambda post: post['created_at'],
            reverse=True,
        )

        # Remove duplicate posts
        posts_combined_dict = {}
        for post in posts_combined:
            posts_combined_dict.setdefault(post['id'], post)

        posts_combined = list(posts_combined_dict.values())[:limit]

        # Expand thread
        RootPost = Post.alias()
        for i, post in enumerate(posts_combined):
            if not post.get("expand_thread"):
                continue

            thread = Post.select(
                Post.uri,
                Post.created_at,
            ).join(
                RootPost, on=(RootPost.uri == Post.reply_root)
            ).where(
                RootPost.uri == post["uri"],
                Post.author == RootPost.author,
            )
            if thread.count() > 1:
                posts_combined.insert(i + 1, {
                    "uri": thread.order_by(Post.created_at.asc()).first().uri
                })
                posts_combined.insert(i + 2, {
                    "uri": thread.order_by(Post.created_at.desc()).first().uri
                })

        feed = []
        for post in posts_combined:
            feed_entry = {'post': post['uri']}

            if repost_uri := post.get('repost_uri'):
                feed_entry['reason'] = {
                    '$type': 'app.bsky.feed.defs#skeletonReasonRepost',
                    'repost': repost_uri,
                }

            feed.append(feed_entry)

        cursor = base.CURSOR_EOF
        last_post = posts_combined[-1] if posts_combined else None
        if last_post:
            cursor = f'{int(last_post["created_at"].timestamp() * 1000)}::{last_post["cid"]}'

        return {
            'cursor': cursor,
            'feed': feed
        }
