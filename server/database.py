from datetime import datetime

import peewee

db = peewee.PostgresqlDatabase(
    "bsky_feeds",
    user="postgres",
    password="postgres",
    host="db",
    port=5432,
)


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
    code = peewee.CharField(index=True)


class Post(BaseModel):
    uri = peewee.CharField(primary_key=True)

    author = peewee.ForeignKeyField(User, related_name='posts', null=True)

    cid = peewee.CharField(index=True)
    reply_parent = peewee.CharField(null=True, default=None, index=True)
    reply_root = peewee.CharField(null=True, default=None, index=True)

    indexed_at = peewee.DateTimeField(default=datetime.utcnow)
    created_at = peewee.DateTimeField(null=True, index=True)
    languages = peewee.ManyToManyField(Language, backref='posts')


PostLanguage = Post.languages.get_through_model()


class SubscriptionState(BaseModel):
    service = peewee.CharField(unique=True)
    cursor = peewee.IntegerField()


if db.is_closed():
    db.connect()
    db.create_tables([
        User,
        Language,
        Post,
        PostLanguage,
        SubscriptionState,
    ])
