import os

SERVICE_DID = os.environ.get('SERVICE_DID', None)
HOSTNAME = os.environ.get('HOSTNAME', None)

if HOSTNAME is None:
    raise RuntimeError('You should set "HOSTNAME" environment variable first.')

if SERVICE_DID is None:
    SERVICE_DID = f'did:web:{HOSTNAME}'


SPANISH_URI = os.environ.get('SPANISH_URI')
if SPANISH_URI is None:
    raise RuntimeError('Publish your feed first (run publish_feed.py) to obtain Feed URI. '
                       'Set this URI to "SPANISH_URI" environment variable.')
