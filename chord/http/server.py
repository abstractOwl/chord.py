import logging
import threading
import time

import click
from flask import Flask, request
from flask.logging import create_logger

from chord.exceptions import NodeFailureException
from chord.constants import (
        NODE, CREATE, FIND_SUCCESSOR, JOIN, NOTIFY, GET_PREDECESSOR, GET_SUCCESSOR_LIST, SHUTDOWN,
        GET_KEY, PUT_KEY
)
from chord.http.transport import HttpChordTransport
from chord.marshal import JsonChordMarshaller, JsonChordUnmarshaller
from chord.model import (
        CreateRequest, FindSuccessorRequest, GetKeyRequest, GetPredecessorRequest,
        GetSuccessorListRequest, JoinRequest, NodeRequest, NotifyRequest, PutKeyRequest,
        ShutdownRequest
)
from chord.node import ChordNode
from chord.storage import DictChordStorage


app = Flask(__name__)

logging.basicConfig()
log = create_logger(app)
log.setLevel(logging.INFO)


@app.before_first_request
def schedule_maintenance_tasks():
    def loop():
        while True:
            if app.config["node"].get_successor_list(GetSuccessorListRequest()).successor_list:
                log.info("Running maintenance tasks...")

                try:
                    app.config["node"].fix_fingers()
                except NodeFailureException:
                    log.info("Node failed while trying to fix fingers")

                try:
                    app.config["node"].stabilize()
                except NodeFailureException:
                    log.info("Node failed while trying to stabilize")

                app.config["node"].check_predecessor()

                log.info("Done.")

            time.sleep(1)

    thread = threading.Thread(target=loop, daemon=True)
    thread.start()


@app.route("/" + NODE, methods=["POST"])
def node_info():
    payload = request.get_json(force=True)
    req = app.config["unmarshaller"].unmarshal(payload, NodeRequest)
    log.info("%s: %s", NODE, req)
    return app.config["marshaller"].marshal(app.config["node"].node(req))


@app.route("/" + CREATE, methods=["POST"])
def create():
    payload = request.get_json(force=True)
    req = app.config["unmarshaller"].unmarshal(payload, CreateRequest)
    log.info("%s: %s", CREATE, req)
    return app.config["marshaller"].marshal(app.config["node"].create(req))


@app.route("/" + FIND_SUCCESSOR, methods=["POST"])
def find_successor():
    payload = request.get_json(force=True)
    req = app.config["unmarshaller"].unmarshal(payload, FindSuccessorRequest)
    log.info("%s: %s", FIND_SUCCESSOR, req)
    return app.config["marshaller"].marshal(app.config["node"].find_successor(req))


@app.route("/" + JOIN, methods=["POST"])
def join():
    payload = request.get_json(force=True)
    req = app.config["unmarshaller"].unmarshal(payload, JoinRequest)
    log.info("%s: %s", JOIN, req)
    return app.config["marshaller"].marshal(app.config["node"].join(req))


@app.route("/" + NOTIFY, methods=["POST"])
def notify():
    payload = request.get_json(force=True)
    req = app.config["unmarshaller"].unmarshal(payload, NotifyRequest)
    log.info("%s: %s", NOTIFY, req)
    return app.config["marshaller"].marshal(app.config["node"].notify(req))


@app.route("/" + GET_PREDECESSOR, methods=["POST"])
def get_predecessor():
    payload = request.get_json(force=True)
    req = app.config["unmarshaller"].unmarshal(payload, GetPredecessorRequest)
    log.info("%s: %s", GET_PREDECESSOR, req)
    return app.config["marshaller"].marshal(app.config["node"].get_predecessor(req))


@app.route("/" + GET_SUCCESSOR_LIST, methods=["POST"])
def get_successor_list():
    payload = request.get_json(force=True)
    req = app.config["unmarshaller"].unmarshal(payload, GetSuccessorListRequest)
    log.info("%s: %s", GET_SUCCESSOR_LIST, req)
    return app.config["marshaller"].marshal(app.config["node"].get_successor_list(req))


@app.route("/" + SHUTDOWN, methods=["POST"])
def shutdown():
    try:
        payload = request.get_json(force=True)
        req = app.config["unmarshaller"].unmarshal(payload, ShutdownRequest)
        log.info("%s: %s", SHUTDOWN, req)
        return app.config["marshaller"].marshal(app.config["node"].shutdown(req))
    finally:
        request.environ.get('werkzeug.server.shutdown')()


@app.route("/" + GET_KEY, methods=["POST"])
def get():
    payload = request.get_json(force=True)
    req = app.config["unmarshaller"].unmarshal(payload, GetKeyRequest)
    log.info("%s: %s", GET_KEY, req)
    return app.config["marshaller"].marshal(app.config["node"].get(req))


@app.route("/" + PUT_KEY, methods=["POST"])
def put():
    payload = request.get_json(force=True)
    req = app.config["unmarshaller"].unmarshal(payload, PutKeyRequest)
    log.info("%s: %s", PUT_KEY, req)
    return app.config["marshaller"].marshal(app.config["node"].put(req))


@click.command()
@click.argument("hostname", required=True)
@click.argument("port", required=True)
@click.argument("successor_list_size", required=True)
@click.argument("ring_size", required=True)
def run(hostname: str, port: str, successor_list_size: str, ring_size: str):
    node_id = f"{hostname}:{port}"
    successor_list_size_num = int(successor_list_size)
    ring_size_num = int(ring_size)

    log.info("Running on %s with successor list size %s and ring size %s...",
            node_id, successor_list_size, ring_size)
    transport = HttpChordTransport(successor_list_size_num)

    app.config["marshaller"] = JsonChordMarshaller()
    app.config["unmarshaller"] = JsonChordUnmarshaller(transport)
    app.config["node"] = ChordNode(
            node_id, DictChordStorage(), successor_list_size_num, ring_size_num
    )
    app.run(hostname, int(port))

if __name__ == '__main__':
    run()
