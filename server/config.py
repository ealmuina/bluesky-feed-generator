import os

SERVICE_DID = os.environ.get('SERVICE_DID', None)
HOSTNAME = os.environ.get('BSKY_HOSTNAME', None)

if HOSTNAME is None:
    raise RuntimeError('You should set "BSKY_HOSTNAME" environment variable first.')

if SERVICE_DID is None:
    SERVICE_DID = f'did:web:{HOSTNAME}'

TOP_SPANISH_URI = os.environ.get('TOP_SPANISH_URI')

BASQUE_URI = os.environ.get('BASQUE_URI')
CATALAN_URI = os.environ.get('CATALAN_URI')
GALICIAN_URI = os.environ.get('GALICIAN_URI')
PORTUGUESE_URI = os.environ.get('PORTUGUESE_URI')
SPANISH_URI = os.environ.get('SPANISH_URI')

FOLLOWING_PLUS_URI = os.environ.get('FOLLOWING_PLUS_URI')
