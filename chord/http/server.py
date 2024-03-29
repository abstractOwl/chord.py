import logging
import os

import click
from flask import Flask, request
from flask.logging import create_logger

from chord.constants import (
        NODE, CREATE, FIND_SUCCESSOR, JOIN, NOTIFY, GET_PREDECESSOR, GET_SUCCESSOR_LIST, SHUTDOWN,
        GET_KEY, PUT_KEY
)
from chord.http.transport import HttpChordTransport
from chord.marshal import JsonChordMarshaller
from chord.model import (
        CreateRequest, FindSuccessorRequest, GetKeyRequest, GetPredecessorRequest,
        GetSuccessorListRequest, JoinRequest, NodeRequest, NotifyRequest, PutKeyRequest,
        ShutdownRequest
)
from chord.node import ChordConfig, ChordNode
from chord.storage import DictChordStorage


app = Flask(__name__)

logging.basicConfig()
log = create_logger(app)
log.setLevel(logging.INFO)


@app.route("/" + NODE, methods=["POST"])
def node_info():
    payload = request.get_json(force=True)
    req = app.config["marshaller"].unmarshal(payload, NodeRequest)
    log.info("%s: %s", NODE, req)
    return app.config["marshaller"].marshal(app.config["node"].node(req))


@app.route("/" + CREATE, methods=["POST"])
def create():
    payload = request.get_json(force=True)
    req = app.config["marshaller"].unmarshal(payload, CreateRequest)
    log.info("%s: %s", CREATE, req)
    return app.config["marshaller"].marshal(app.config["node"].create(req))


@app.route("/" + FIND_SUCCESSOR, methods=["POST"])
def find_successor():
    payload = request.get_json(force=True)
    req = app.config["marshaller"].unmarshal(payload, FindSuccessorRequest)
    log.info("%s: %s", FIND_SUCCESSOR, req)
    return app.config["marshaller"].marshal(app.config["node"].find_successor(req))


@app.route("/" + JOIN, methods=["POST"])
def join():
    payload = request.get_json(force=True)
    req = app.config["marshaller"].unmarshal(payload, JoinRequest)
    log.info("%s: %s", JOIN, req)
    return app.config["marshaller"].marshal(app.config["node"].join(req))


@app.route("/" + NOTIFY, methods=["POST"])
def notify():
    payload = request.get_json(force=True)
    req = app.config["marshaller"].unmarshal(payload, NotifyRequest)
    log.info("%s: %s", NOTIFY, req)
    return app.config["marshaller"].marshal(app.config["node"].notify(req))


@app.route("/" + GET_PREDECESSOR, methods=["POST"])
def get_predecessor():
    payload = request.get_json(force=True)
    req = app.config["marshaller"].unmarshal(payload, GetPredecessorRequest)
    log.info("%s: %s", GET_PREDECESSOR, req)
    return app.config["marshaller"].marshal(app.config["node"].get_predecessor(req))


@app.route("/" + GET_SUCCESSOR_LIST, methods=["POST"])
def get_successor_list():
    payload = request.get_json(force=True)
    req = app.config["marshaller"].unmarshal(payload, GetSuccessorListRequest)
    log.info("%s: %s", GET_SUCCESSOR_LIST, req)
    return app.config["marshaller"].marshal(app.config["node"].get_successor_list(req))


@app.route("/" + SHUTDOWN, methods=["POST"])
def shutdown():
    try:
        payload = request.get_json(force=True)
        req = app.config["marshaller"].unmarshal(payload, ShutdownRequest)
        log.info("%s: %s", SHUTDOWN, req)
        return app.config["marshaller"].marshal(app.config["node"].shutdown(req))
    finally:
        # Note that this usually runs before the ShutdownResponse can be returned
        os._exit(0)


@app.route("/" + GET_KEY, methods=["POST"])
def get():
    payload = request.get_json(force=True)
    req = app.config["marshaller"].unmarshal(payload, GetKeyRequest)
    log.info("%s: %s", GET_KEY, req)
    return app.config["marshaller"].marshal(app.config["node"].get(req))


@app.route("/" + PUT_KEY, methods=["POST"])
def put():
    payload = request.get_json(force=True)
    req = app.config["marshaller"].unmarshal(payload, PutKeyRequest)
    log.info("%s: %s", PUT_KEY, req)
    return app.config["marshaller"].marshal(app.config["node"].put(req))


@click.command()
@click.argument("hostname", required=True)
@click.argument("port", required=True)
@click.argument("ring_size", required=True)
def run(hostname: str, port: str, ring_size: str):
    node_id = f"{hostname}:{port}"
    ring_size_num = int(ring_size)

    log.info("Running on %s with ring size %s...", node_id, ring_size)
    transport = HttpChordTransport(ring_size_num)

    app.config["marshaller"] = JsonChordMarshaller(transport)
    app.config["node"] = ChordNode(node_id, DictChordStorage(), ChordConfig(ring_size_num))
    app.config["node"].schedule_maintenance_tasks()
    app.run(hostname, int(port))


if __name__ == '__main__':
    run()
