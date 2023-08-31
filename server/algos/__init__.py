from .languages import spanish, catalan, portuguese, galician

algos = {
    spanish.uri: spanish.handler,
    catalan.uri: catalan.handler,
    portuguese.uri: portuguese.handler,
    galician.uri: galician.handler,
}
