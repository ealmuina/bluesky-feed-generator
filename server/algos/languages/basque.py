from typing import Optional

import server.algos.languages.languages as algo_languages
from server import config

uri = config.BASQUE_URI


def handler(cursor: Optional[str], limit: int, requester_did: str) -> dict:
    return algo_languages.handler(
        language_code="eu",
        cursor=cursor,
        limit=limit,
    )
