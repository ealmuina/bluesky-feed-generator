from typing import Optional

import server.algos.languages.languages as algo_languages
from server import config

uri = config.SPANISH_URI


def handler(cursor: Optional[str], limit: int) -> dict:
    return algo_languages.handler(
        language_code="es",
        cursor=cursor,
        limit=limit,
    )
