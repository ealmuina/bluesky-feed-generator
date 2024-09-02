from collections import defaultdict

from atproto import models

from server.tasks.posts import process_events


def _parse_ops(ops):
    for op in ops:
        if "record" not in op:
            continue

        record = op["record"]
        reply = getattr(record, "reply", None)
        langs = getattr(record, "langs", None)
        subject = getattr(record, "subject", None)

        yield {
            "uri": op["uri"],
            "cid": op["cid"],
            "author": op["author"],
            "record": {
                "text": getattr(record, "text", ""),
                "subject": {
                    "uri": getattr(subject, "uri", None),
                    "cid": getattr(subject, "cid", None),
                },
                "reply_parent": reply.parent.uri if reply and reply.parent.uri else None,
                "reply_root": reply.root.uri if reply and reply.root.uri else None,
                "langs": langs,
                "created_at": record.created_at,
            }
        }


def operations_callback(ops: defaultdict) -> None:
    parsed_ops = {}

    for entity in (
            models.ids.AppBskyFeedPost,
            models.ids.AppBskyFeedLike,
            models.ids.AppBskyFeedRepost
    ):
        parsed_ops[entity] = {
            "created": list(_parse_ops(ops[entity]["created"])),
            "deleted": list(_parse_ops(ops[entity]["deleted"])),
        }

    process_events(parsed_ops)
