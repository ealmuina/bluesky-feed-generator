from datetime import datetime
from typing import Optional

import peewee
from peewee import fn

from server import config
from server.algos import base
from server.database import Post, Language, User, Interaction
from server.utils import last_item, nth_item

uri = config.TOP_SPANISH_URI


class TopSpanishAlgorithm:
    def __init__(self, min_followers=500, min_likes=20):
        self.language = Language.get_or_none(Language.code == "es")
        self.min_followers = min_followers
        self.min_likes = min_likes

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

    def _get_reposts_from_top_accounts(self, limit, created_at, cid):
        posts = self.language.posts.select(
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
            User.followers_count >= self.min_followers,
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

    def _get_posts_with_likes_milestone(self, limit, created_at, cid):
        posts = self.language.posts.select(
            Post.id,
            Post.uri,
            Post.cid,
            nth_item(Interaction.created_at, self.min_likes).alias("created_at"),
        ).join(
            Interaction, on=(Interaction.post == Post.id)
        ).join(
            User, on=(User.id == Post.author)
        ).where(
            Post.created_at <= datetime.utcnow(),
            Interaction.interaction_type == Interaction.LIKE,
            Interaction.created_at <= datetime.utcnow(),
            User.followers_count < self.min_followers,
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

        posts_from_top_accounts = self._get_posts_from_top_accounts(limit, created_at, cid)
        reposts_from_top_accounts = self._get_reposts_from_top_accounts(limit, created_at, cid)
        posts_with_likes_milestone = self._get_posts_with_likes_milestone(limit, created_at, cid)
        posts_combined = sorted(
            [
                *posts_from_top_accounts.dicts(),
                *reposts_from_top_accounts.dicts(),
                *posts_with_likes_milestone.dicts(),
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
