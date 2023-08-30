from typing import Optional

import server.algos.languages.languages as algo_languages
from server import config

uri = config.PORTUGUESE_URI


def handler(cursor: Optional[str], limit: int) -> dict:
    return algo_languages.handler(
        language_code="pt",
        cursor=cursor,
        limit=limit,
    )
