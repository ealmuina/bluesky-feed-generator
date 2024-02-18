import server.algos.top_followed.spanish as top_spanish
from .following_plus import FollowingPlusAlgorithm
from .languages import spanish, catalan, portuguese, galician

algos = {
    top_spanish.uri: top_spanish.TopSpanishAlgorithm().handle,

    spanish.uri: spanish.handler,
    catalan.uri: catalan.handler,
    portuguese.uri: portuguese.handler,
    galician.uri: galician.handler,

    following_plus.uri: FollowingPlusAlgorithm().handle,
}
