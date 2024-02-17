from datetime import datetime

import peewee

db = peewee.PostgresqlDatabase(
    "bsky_feeds",
    user="postgres",
    password="postgres",
    host="db",
    port=5432
)
db_version = 2


class BaseModel(peewee.Model):
    class Meta:
        database = db


class User(BaseModel):
    did = peewee.CharField(index=True)
    handle = peewee.CharField(null=True)
    followers_count = peewee.IntegerField(null=True)
    follows_count = peewee.IntegerField(null=True)
    posts_count = peewee.IntegerField(null=True)

    indexed_at = peewee.DateTimeField(default=datetime.utcnow)
    last_update = peewee.DateTimeField(null=True)


class Language(BaseModel):
    code = peewee.CharField(unique=True)


class Post(BaseModel):
    author = peewee.ForeignKeyField(User, related_name='posts', null=True)

    uri = peewee.CharField(index=True)
    cid = peewee.CharField()
    reply_parent = peewee.CharField(null=True, default=None)
    reply_root = peewee.CharField(null=True, default=None)

    indexed_at = peewee.DateTimeField(default=datetime.utcnow)
    created_at = peewee.DateTimeField(null=True)
    languages = peewee.ManyToManyField(Language, backref='posts')


PostLanguage = Post.languages.get_through_model()


class Interaction(BaseModel):
    LIKE, REPOST = range(2)

    uri = peewee.CharField(index=True)
    cid = peewee.CharField()

    author = peewee.ForeignKeyField(User, related_name='likes')
    post = peewee.ForeignKeyField(Post, related_name='likes')
    interaction_type = peewee.IntegerField(
        index=True,
        choices=[
            (LIKE, 'like'),
            (REPOST, 'repost'),
        ],
    )

    indexed_at = peewee.DateTimeField(default=datetime.utcnow)
    created_at = peewee.DateTimeField(null=True)


class SubscriptionState(BaseModel):
    service = peewee.CharField(unique=True)
    cursor = peewee.IntegerField()


class DbMetadata(BaseModel):
    version = peewee.IntegerField()


if db.is_closed():
    db.connect()
    db.create_tables([
        User,
        Language,
        Post,
        PostLanguage,
        Interaction,
        SubscriptionState,
        DbMetadata
    ])

    # DB migration
    current_version = 1
    if DbMetadata.select().count() != 0:
        current_version = DbMetadata.select().first().version

    if current_version != db_version:
        with db.atomic():
            # V2
            # Drop cursors stored from the old bsky.social PDS
            if current_version == 1:
                SubscriptionState.delete().execute()

            # Update version in DB
            if DbMetadata.select().count() == 0:
                DbMetadata.insert({DbMetadata.version: db_version}).execute()
            else:
                DbMetadata.update({DbMetadata.version: db_version}).execute()
