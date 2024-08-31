import os
import signal
import sys
import threading

from flask import Flask, jsonify, request

from server import config, data_stream
from server.algos import algos
from server.auth import AuthorizationError, validate_auth
from server.data_filter import operations_callback, PostProcessor
from server.tasks import cleaner, statistics

app = Flask(__name__)

stop_event = threading.Event()

# Posts process
for _ in range(os.cpu_count()):
    PostProcessor().start()

# Stream thread
threading.Thread(
    target=data_stream.run, args=(config.SERVICE_DID, operations_callback, stop_event,)
).start()

# Cleaner thread
threading.Thread(
    target=cleaner.run, args=(stop_event,)
).start()

# Statistics update thread
statistics_updater = statistics.StatisticsUpdater()
threading.Thread(
    target=statistics_updater.run, args=(stop_event,)
).start()


def sigint_handler(*_):
    print('Stopping sub-threads...')
    stop_event.set()
    sys.exit(0)


signal.signal(signal.SIGINT, sigint_handler)


@app.route('/')
def index():
    return 'ATProto Feed Generator powered by The AT Protocol SDK for Python (https://github.com/MarshalX/atproto).'


@app.route('/.well-known/did.json', methods=['GET'])
def did_json():
    if not config.SERVICE_DID.endswith(config.HOSTNAME):
        return '', 404

    return jsonify({
        '@context': ['https://www.w3.org/ns/did/v1'],
        'id': config.SERVICE_DID,
        'service': [
            {
                'id': '#bsky_fg',
                'type': 'BskyFeedGenerator',
                'serviceEndpoint': f'https://{config.HOSTNAME}'
            }
        ]
    })


@app.route('/xrpc/app.bsky.feed.describeFeedGenerator', methods=['GET'])
def describe_feed_generator():
    feeds = [{'uri': uri} for uri in algos.keys()]
    response = {
        'encoding': 'application/json',
        'body': {
            'did': config.SERVICE_DID,
            'feeds': feeds
        }
    }
    return jsonify(response)


@app.route('/xrpc/app.bsky.feed.getFeedSkeleton', methods=['GET'])
def get_feed_skeleton():
    feed = request.args.get('feed', default=None, type=str)
    algo = algos.get(feed)
    if not algo:
        return 'Unsupported algorithm', 400

    try:
        requester_did = validate_auth(request)
    except AuthorizationError:
        return 'Unauthorized', 401

    try:
        cursor = request.args.get('cursor', default=None, type=str)
        limit = request.args.get('limit', default=20, type=int)
        body = algo(cursor, limit, requester_did)
    except ValueError:
        return 'Malformed cursor', 400

    return jsonify(body)
