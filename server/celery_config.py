from celery import Celery

from server.database import db

# Reconnect to DB
db.close()
db.connect()

app = Celery(
    'tasks',
    broker='redis://redis:6379/0',
    backend='redis://redis:6379/0'
)

app.autodiscover_tasks(
    [
        "server.tasks.posts",
        "server.tasks.statistics",
    ],
    force=True
)

# app.conf.beat_schedule = {
#     'refresh-node-settings': {
#         'task': 'web.tasks.refresh_node_settings',
#         'schedule': crontab(hour='1')
#     }
# }
app.conf.task_time_limit = 3600  # timeout after 1 hour
