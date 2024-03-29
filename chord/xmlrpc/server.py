import argparse
import logging
import socketserver
import sys
import threading
import time
from typing import Tuple
from xmlrpc.server import SimpleXMLRPCServer

from chord.exceptions import NodeFailureException
from chord.xmlrpc.transport import XmlRpcChordTransportFactory
from chord.node import ChordNode, RemoteChordNode
from chord.storage import DictChordStorage


CHORD_NODE = None
TRANSPORT_FACTORY = XmlRpcChordTransportFactory()

logging.basicConfig()
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)


def schedule_maintenance_tasks():
    def loop():
        while True:
            if CHORD_NODE._node.get_successor_list():
                LOG.info("Running maintenance tasks...")

                try:
                    CHORD_NODE._node.fix_fingers()
                except NodeFailureException:
                    LOG.info("Node failed while trying to fix fingers")

                try:
                    CHORD_NODE._node.stabilize()
                except NodeFailureException:
                    LOG.info("Node failed while trying to stabilize")

                CHORD_NODE._node.check_predecessor()

                LOG.info("Done.")

            time.sleep(1)

    thread = threading.Thread(target=loop, daemon=True)
    thread.start()


# Needed since XMLRPC marshals object instances to Dict when sending but
# doesn't unmarshal them when receiving.
class ChordNodeHandler:
    def __init__(self, node_id, storage, successor_list_size, ring_size):
        self._node = ChordNode(node_id, storage, successor_list_size, ring_size)

    def create(self):
        self._node.create()

    def node(self):
        node = self._node.node()
        return {
            "node_id": node.node_id,
            "fingers": [
                finger.node_id if finger else None for finger in node.fingers
            ]
        }

    def join(self, remote_node_id: str):
        node = RemoteChordNode(TRANSPORT_FACTORY, remote_node_id)
        self._node.join(node)

    def notify(self, remote_node_id: str):
        node = RemoteChordNode(TRANSPORT_FACTORY, remote_node_id)
        self._node.notify(node)

    def find_successor(self, key: int) -> Tuple[str, int]:
        node, hops = self._node.find_successor(key)
        return node.node_id, hops

    def get_predecessor(self):
        node = self._node.get_predecessor()
        if node is None:
            return None
        return node.node_id

    def get_successor_list(self):
        successor_list = self._node.get_successor_list()
        return [node.node_id for node in successor_list]

    def shutdown(self):
        self._node.shutdown()
        sys.exit(0)

    def get(self, key):
        return self._node.get(key)

    def put(self, key, value, no_redirect):
        return self._node.put(key, value, no_redirect)


# The default SimpleXMLRPCServer is single-threaded. This creates a threaded
# XMLRPC server.
class ThreadedXmlRpcServer(socketserver.ThreadingMixIn, SimpleXMLRPCServer): pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Runs a Chord server.")
    parser.add_argument("hostname", type=str, help="The server hostname.")
    parser.add_argument("port", type=int, help="The server port")
    parser.add_argument("successor_list_size", type=int, help="The successor list size")
    parser.add_argument("ring_size", type=int, help="The Chord ring size")
    args = parser.parse_args()

    hostname = args.hostname
    port = args.port
    self_successor_list_size = args.successor_list_size
    self_ring_size = args.ring_size
    self_node_id = f"{hostname}:{port}"

    LOG.info("Running on %s with successor list size %s and ring size %s...",
            self_node_id, self_successor_list_size, self_ring_size)
    CHORD_NODE = ChordNodeHandler(
            self_node_id, DictChordStorage(), self_successor_list_size, self_ring_size
    )

    schedule_maintenance_tasks()

    with ThreadedXmlRpcServer((hostname, port), allow_none=True) as server:
        server.register_instance(CHORD_NODE)
        server.serve_forever()
