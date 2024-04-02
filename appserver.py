from flask import Flask
from dotenv import load_dotenv, find_dotenv
from flask import Response

import threading

from fauna import fql
from fauna.client import Client, StreamOptions
from fauna.errors import FaunaException
from fauna.query.models import StreamToken
import os

load_dotenv(find_dotenv())

stream_token = None
thread = None

def do_stream():
    global stream_token

    stream_client = Client(secret=os.getenv("FAUNA_SECRET"), endpoint=os.getenv("FAUNA_ENDPOINT"))

    # If the app server restarted, we'll get the last timestamp of a healthcheck ping, and the original stream_token
    res = stream_client.query(
        fql("""
            let latest = healthcheck_ts.last_ping().first()
            if (latest != null) {
              {
                start_ts: latest!.ping.toMicros(),
                stream_token: latest!.stream_token
              }              
            } else {
              null
            }
            """)
    )
    start_ts = int(res.data["start_ts"]) if res.data is not None else None
    stream_token = res.data["stream_token"] if res.data is not None else None

    if stream_token is None:
        try:
            #----------------------------------#
            # your stream query here
            #----------------------------------#
            q = fql('Foo.all().toStream()')
            res = stream_client.query(q)        
            stream_token = res.data.token
        except FaunaException as err:
            return "Unable to obtain a stream token. ERR: {}".format(err)
    
    opts = StreamOptions(max_attempts=5, max_backoff=30, start_ts=start_ts)

    with stream_client.stream(StreamToken(stream_token), opts) as stream:
        for event in stream:
            try:
                #--------------------------------------------------------#
                # send these events to SNS, SQS, where you want them, etc.
                #--------------------------------------------------------#
              

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
    global stream_token
    try:
        print("checking health")
        client = Client(secret=os.getenv("FAUNA_SECRET"), endpoint=os.getenv("FAUNA_ENDPOINT"))
        client.query(
            fql("""
                healthcheck_ts.create({
                  ping: Time.now(),
                  stream_token: ${stream_token}
                })
                """,
                stream_token=stream_token
            )
        )
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


