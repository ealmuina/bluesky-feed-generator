from datetime import datetime

import peewee

db = peewee.SqliteDatabase('feed_database.db')


class BaseModel(peewee.Model):
    class Meta:
        database = db


class Language(BaseModel):
    code = peewee.CharField(unique=True)


class Post(BaseModel):
    uri = peewee.CharField(index=True)
    cid = peewee.CharField()
    reply_parent = peewee.CharField(null=True, default=None)
    reply_root = peewee.CharField(null=True, default=None)
    indexed_at = peewee.DateTimeField(default=datetime.now)
    languages = peewee.ManyToManyField(Language, backref='posts')


PostLanguage = Post.languages.get_through_model()


class SubscriptionState(BaseModel):
    service = peewee.CharField(unique=True)
    cursor = peewee.IntegerField()


if db.is_closed():
    db.connect()
    db.create_tables([Language, Post, PostLanguage, SubscriptionState])
