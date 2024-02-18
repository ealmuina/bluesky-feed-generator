import os
from datetime import datetime
from typing import Optional

from atproto_client.client.client import Client

from server import config
from server.algos import base
from server.database import Post, User, Interaction

uri = config.FOLLOWING_PLUS_URI


class FollowingPlusAlgorithm:
    def __init__(self):
        self.client = Client()
        self.client.login(
            os.environ.get("STATISTICS_USER"),
            os.environ.get("STATISTICS_PASSWORD"),
        )

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
            Interaction.created_at.alias("created_at"),
            User.did.alias("like_by_did"),
        ).join(
            Interaction, on=(Interaction.post == Post.id)
        ).join(
            User, on=(User.id == Interaction.author)
        ).where(
            Interaction.interaction_type == Interaction.LIKE,
            Interaction.created_at <= datetime.utcnow(),
            User.did.in_(user_follows_dids),
            Interaction.id.in_(
                Interaction.select(Interaction.id).where(Interaction.post == Post.id).order_by(
                    Interaction.created_at.asc()).offset(3).limit(1)
            )
        ).order_by(
            Interaction.created_at.desc(),
            Interaction.cid.desc(),
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
            Interaction.created_at.alias("created_at"),
            Interaction.uri.alias("repost_uri"),
        ).join(
            Interaction, on=(Interaction.post == Post.id)
        ).join(
            User, on=(User.id == Interaction.author)
        ).where(
            Interaction.interaction_type == Interaction.REPOST,
            Interaction.created_at <= datetime.utcnow(),
            User.did.in_(user_follows_dids),
            Interaction.id.in_(
                Interaction.select(Interaction.id).where(Interaction.post == Post.id).order_by(
                    Interaction.created_at.desc()).limit(1)
            )
        ).order_by(
            Interaction.created_at.desc(),
            Interaction.cid.desc(),
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
                *posts_from_follows.dicts(),
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
