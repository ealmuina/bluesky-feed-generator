import server.algos.top_followed.spanish as top_spanish
from .following_plus import FollowingPlusAlgorithm
from .languages import spanish, catalan, portuguese, galician, basque

algos = {
    top_spanish.uri: top_spanish.TopSpanishAlgorithm().handle,

    basque.uri: basque.handler,
    catalan.uri: catalan.handler,
    galician.uri: galician.handler,
    portuguese.uri: portuguese.handler,
    spanish.uri: spanish.handler,

    following_plus.uri: FollowingPlusAlgorithm().handle,
}
