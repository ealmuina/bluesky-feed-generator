from .languages import spanish, catalan, portuguese, galician
from .top_followed import spanish as top_spanish

algos = {
    spanish.uri: spanish.handler,
    top_spanish.uri: top_spanish.handler,
    catalan.uri: catalan.handler,
    portuguese.uri: portuguese.handler,
    galician.uri: galician.handler,
}
