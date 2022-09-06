import argparse
import logging
import threading
import time

from flask import Flask, jsonify, request
from flask.logging import create_logger

from chord.exceptions import NodeFailureException
from chord.constants import (
        NODE, CREATE, FIND_SUCCESSOR, JOIN, NOTIFY, GET_PREDECESSOR, GET_SUCCESSOR_LIST, SHUTDOWN,
        GET_KEY, PUT_KEY
)
from chord.http.marshaller import marshal
from chord.http.transport import HttpChordTransportFactory
from chord.marshal import JsonChordMarshaller, JsonChordUnmarshaller
from chord.model import *
from chord.node import ChordNode, RemoteChordNode
from chord.storage import DictChordStorage


APP = Flask(__name__)
CHORD_NODE = None
TRANSPORT_FACTORY = HttpChordTransportFactory()

MARSHALLER = JsonChordMarshaller()
UNMARSHALLER = JsonChordUnmarshaller(TRANSPORT_FACTORY)

logging.basicConfig()
LOG = create_logger(APP)
LOG.setLevel(logging.INFO)


@APP.before_first_request
def schedule_maintenance_tasks():
    def loop():
        while True:
            if CHORD_NODE.get_successor_list(GetSuccessorListRequest()).successor_list:
                LOG.info("Running maintenance tasks...")

                try:
                    CHORD_NODE.fix_fingers()
                except NodeFailureException:
                    LOG.info("Node failed while trying to fix fingers")

                try:
                    CHORD_NODE.stabilize()
                except NodeFailureException:
                    LOG.info("Node failed while trying to stabilize")

                CHORD_NODE.check_predecessor()

                LOG.info("Done.")

            time.sleep(1)

    thread = threading.Thread(target=loop, daemon=True)
    thread.start()


@APP.route("/" + NODE, methods=["POST"])
def node_info():
    payload = request.get_json(force=True)
    req = UNMARSHALLER.unmarshal(payload, NodeRequest)
    LOG.info("%s: %s", NODE, req)
    return MARSHALLER.marshal(CHORD_NODE.node(req))


@APP.route("/" + CREATE, methods=["POST"])
def create():
    payload = request.get_json(force=True)
    req = UNMARSHALLER.unmarshal(payload, CreateRequest)
    LOG.info("%s: %s", CREATE, req)
    return MARSHALLER.marshal(CHORD_NODE.create(req))


@APP.route("/" + FIND_SUCCESSOR, methods=["POST"])
def find_successor():
    payload = request.get_json(force=True)
    req = UNMARSHALLER.unmarshal(payload, FindSuccessorRequest)
    LOG.info("%s: %s", FIND_SUCCESSOR, req)
    return MARSHALLER.marshal(CHORD_NODE.find_successor(req))


@APP.route("/" + JOIN, methods=["POST"])
def join():
    payload = request.get_json(force=True)
    req = UNMARSHALLER.unmarshal(payload, JoinRequest)
    LOG.info("%s: %s", JOIN, req)
    return MARSHALLER.marshal(CHORD_NODE.join(req))


@APP.route("/" + NOTIFY, methods=["POST"])
def notify():
    payload = request.get_json(force=True)
    req = UNMARSHALLER.unmarshal(payload, NotifyRequest)
    LOG.info("%s: %s", NOTIFY, req)
    return MARSHALLER.marshal(CHORD_NODE.notify(req))


@APP.route("/" + GET_PREDECESSOR, methods=["POST"])
def get_predecessor():
    payload = request.get_json(force=True)
    req = UNMARSHALLER.unmarshal(payload, GetPredecessorRequest)
    LOG.info("%s: %s", GET_PREDECESSOR, req)
    return MARSHALLER.marshal(CHORD_NODE.get_predecessor(req))


@APP.route("/" + GET_SUCCESSOR_LIST, methods=["POST"])
def get_successor_list():
    payload = request.get_json(force=True)
    req = UNMARSHALLER.unmarshal(payload, GetSuccessorListRequest)
    LOG.info("%s: %s", GET_SUCCESSOR_LIST, req)
    return MARSHALLER.marshal(CHORD_NODE.get_successor_list(req))


@APP.route("/" + SHUTDOWN, methods=["POST"])
def shutdown():
    try:
        payload = request.get_json(force=True)
        req = UNMARSHALLER.unmarshal(payload, ShutdownRequest)
        LOG.info("%s: %s", SHUTDOWN, req)
        return MARSHALLER.marshal(CHORD_NODE.shutdown(req))
    finally:
        request.environ.get('werkzeug.server.shutdown')()


@APP.route("/" + GET_KEY, methods=["POST"])
def get():
    payload = request.get_json(force=True)
    req = UNMARSHALLER.unmarshal(payload, GetKeyRequest)
    LOG.info("%s: %s", GET_KEY, req)
    return MARSHALLER.marshal(CHORD_NODE.get(req))


@APP.route("/" + PUT_KEY, methods=["POST"])
def put():
    payload = request.get_json(force=True)
    req = UNMARSHALLER.unmarshal(payload, PutKeyRequest)
    LOG.info("%s: %s", PUT_KEY, req)
    return MARSHALLER.marshal(CHORD_NODE.put(req))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Runs a Chord server.")
    parser.add_argument("hostname", type=str, help="The server hostname.")
    parser.add_argument("port", type=int, help="The server port")
    parser.add_argument("successor_list_size", type=int, help="The successor list size")
    parser.add_argument("ring_size", type=int, help="The server port")
    args = parser.parse_args()

    hostname = args.hostname
    port = args.port
    successor_list_size = args.successor_list_size
    ring_size = args.ring_size
    node_id = f"{hostname}:{port}"

    LOG.info("Running on %s with successor list size %s and ring size %s...",
            node_id, successor_list_size, ring_size)
    CHORD_NODE = ChordNode(node_id, DictChordStorage(), successor_list_size, ring_size)
    APP.run(hostname, port)
