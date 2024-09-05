from app import app
from server import data_stream, config, data_filter
from server.data_filter import operations_callback


def run_app():
    app.run(host='127.0.0.1', port=8000, debug=True)


def run_data_stream():
    data_stream.DataStreamHandler(config.SERVICE_DID, operations_callback).start()
    data_filter.PostProcessor().run()


if __name__ == '__main__':
    # FOR DEBUG PURPOSE ONLY
    # run_app()

    run_data_stream()
