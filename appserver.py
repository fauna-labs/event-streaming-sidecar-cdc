from flask import Flask
from dotenv import load_dotenv, find_dotenv
from flask import Response

import threading

from fauna import fql
from fauna.client import Client, StreamOptions
from fauna.errors import FaunaException
import os

load_dotenv(find_dotenv())

thread = None

def do_stream():
    stream_client = Client(secret=os.getenv("FAUNA_SECRET"), endpoint=os.getenv("FAUNA_ENDPOINT"))

    opts = StreamOptions(max_attempts=5, max_backoff=30)
    q = fql('Episode.all().toStream()')
    with stream_client.stream(q, opts) as stream:
        for event in stream:
            try:
                print(event)
                # access fields on the document
                # print(event["data"].ts) # fauna provided top level fields
                # print(event["data"].id)            
                # print(event["data"]["name"]) # user provided fields 
            except FaunaException as e:
                print(e)
            except Exception as e:
                print("EXCEPTION {}".format(e))

def start_stream():
    try:
        global thread
        if thread is None:
            thread = threading.Thread(target=do_stream)
            thread.start()
            return "Stream thread started"
        else:
            return "Already started"
    except Exception as error:
        return str(error)


def do_health_check():
    try:
        print("checking health")
        client = Client(secret=os.getenv("FAUNA_SECRET"), endpoint=os.getenv("FAUNA_ENDPOINT"))
        client.query(fql("healthcheck_ts.create({ ping: Time.now() })"))
        return Response("OK")
    except FaunaException as err:
        print(err)
        return Response("{}".format(err), status=400)        


def create_app():
    app = Flask(__name__)

    @app.route("/health")
    def health_checker():
        thread = threading.Thread(target=do_health_check)
        thread.start()
        return "ok"

    return app


if __name__ == '__main__':
    app = create_app()
    start_stream()
    app.run(threaded=True)
else:
    gunicorn_app = create_app()
    start_stream()


