import os

SERVICE_DID = os.environ.get('SERVICE_DID', None)
HOSTNAME = os.environ.get('HOSTNAME', None)

if HOSTNAME is None:
    raise RuntimeError('You should set "HOSTNAME" environment variable first.')

if SERVICE_DID is None:
    SERVICE_DID = f'did:web:{HOSTNAME}'

SPANISH_URI = os.environ.get('SPANISH_URI')
CATALAN_URI = os.environ.get('CATALAN_URI')
PORTUGUESE_URI = os.environ.get('PORTUGUESE_URI')
GALICIAN_URI = os.environ.get('GALICIAN_URI')
