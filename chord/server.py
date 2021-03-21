import argparse
import logging
import threading
import time

from flask import Flask, jsonify, request
from flask.logging import create_logger

from chord.constants import (NODE, CREATE, FIND_SUCCESSOR, JOIN, NOTIFY, PREDECESSOR)
from chord.exceptions import NodeFailureException
from chord.marshaller import marshal
from chord.node import ChordNode, RemoteChordNode


APP = Flask(__name__)
CHORD_NODE = None

logging.basicConfig()
LOG = create_logger(APP)
LOG.setLevel(logging.INFO)


@APP.before_first_request
def schedule_maintenance_tasks():
    def loop():
        while True:
            if CHORD_NODE.successor:
                LOG.info("Running maintainence tasks...")

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

    thread = threading.Thread(target=loop)
    thread.start()


@APP.route(NODE)
def node_info():
    LOG.info("%s: Returning node info", NODE)
    return jsonify(marshal(CHORD_NODE.node()))


@APP.route(CREATE)
def create():
    LOG.info("%s: Creating node ring", CREATE)
    CHORD_NODE.create()
    return jsonify({})


@APP.route(FIND_SUCCESSOR)
def find_successor():
    key = int(request.args.get("key"))
    LOG.info("%s: Finding successor for %s", FIND_SUCCESSOR, key)
    return jsonify(marshal(CHORD_NODE.find_successor(key)))


@APP.route(JOIN)
def join():
    remote_node = request.args.get("node_id")
    LOG.info("%s: Joining node %s", JOIN, remote_node)

    CHORD_NODE.join(RemoteChordNode(remote_node))
    return jsonify({})


@APP.route(NOTIFY)
def notify():
    remote_node = request.args.get("node_id")
    LOG.info("%s: Notifying node %s", NOTIFY, remote_node)

    CHORD_NODE.notify(RemoteChordNode(remote_node))
    return jsonify({})


@APP.route(PREDECESSOR)
def predecessor():
    LOG.info("%s: Getting predecessor", PREDECESSOR)
    return jsonify(marshal(CHORD_NODE.predecessor))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Runs a Chord server.")
    parser.add_argument("hostname", type=str, help="The server hostname.")
    parser.add_argument("port", type=int, help="The server port")
    parser.add_argument("ring_size", type=int, help="The server port")
    args = parser.parse_args()

    hostname = args.hostname
    port = args.port
    ring_size = args.ring_size
    node_id = f"{hostname}:{port}"

    LOG.info("Running on %s with ring size %s...", node_id, ring_size)
    CHORD_NODE = ChordNode(node_id, ring_size)
    APP.run(hostname, port)
