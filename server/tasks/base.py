from celery import Task

from server.database import db


class BaseCeleryTask(Task):
    def __init__(self):
        # Reconnect to DB
        db.close()
        db.connect()
