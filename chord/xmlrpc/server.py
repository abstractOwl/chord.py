import argparse
import logging
import json
import socketserver
import sys
import threading
import time
from typing import Tuple
from xmlrpc.server import SimpleXMLRPCServer

from chord.constants import *
from chord.exceptions import NodeFailureException
from chord.marshal import JsonChordMarshaller, JsonChordUnmarshaller
from chord.model import *
from chord.node import ChordNode, RemoteChordNode
from chord.storage import DictChordStorage
from chord.xmlrpc.transport import XmlRpcChordTransportFactory


CHORD_NODE = None
TRANSPORT_FACTORY = XmlRpcChordTransportFactory()

MARSHALLER = JsonChordMarshaller()
UNMARSHALLER = JsonChordUnmarshaller(TRANSPORT_FACTORY)

logging.basicConfig()
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)


def schedule_maintenance_tasks():
    def loop():
        while True:
            if CHORD_NODE._node.get_successor_list(GetSuccessorListRequest()).successor_list:
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
        self._node: ChordNode = ChordNode(node_id, storage, successor_list_size, ring_size)

    def create(self, payload: dict) -> CreateResponse:
        request = UNMARSHALLER.unmarshal(payload, CreateRequest)
        LOG.info("%s: %s", CREATE, request)
        return json.loads(MARSHALLER.marshal(self._node.create(request)))

    def node(self, payload: dict) -> NodeResponse:
        request = UNMARSHALLER.unmarshal(payload, NodeRequest)
        LOG.info("%s: %s", NODE, request)
        return json.loads(MARSHALLER.marshal(self._node.node(request)))

    def join(self, payload: dict) -> JoinResponse:
        request = UNMARSHALLER.unmarshal(payload, JoinRequest)
        LOG.info("%s: %s", JOIN, request)
        return json.loads(MARSHALLER.marshal(self._node.join(request)))

    def notify(self, payload: dict) -> NotifyResponse:
        request = UNMARSHALLER.unmarshal(payload, NotifyRequest)
        LOG.info("%s: %s", NOTIFY, request)
        return json.loads(MARSHALLER.marshal(self._node.notify(request)))

    def find_successor(self, payload: dict) -> FindSuccessorResponse:
        request = UNMARSHALLER.unmarshal(payload, FindSuccessorRequest)
        LOG.info("%s: %s", FIND_SUCCESSOR, request)
        return json.loads(MARSHALLER.marshal(self._node.find_successor(request)))

    def get_predecessor(self, payload: dict) -> GetPredecessorResponse:
        request = UNMARSHALLER.unmarshal(payload, GetPredecessorRequest)
        LOG.info("%s: %s", GET_PREDECESSOR, request)
        return json.loads(MARSHALLER.marshal(self._node.get_predecessor(request)))

    def get_successor_list(self, payload: dict) -> GetSuccessorListResponse:
        request = UNMARSHALLER.unmarshal(payload, GetSuccessorListRequest)
        LOG.info("%s: %s", GET_SUCCESSOR_LIST, request)
        return json.loads(MARSHALLER.marshal(self._node.get_successor_list(request)))

    def shutdown(self, payload: dict) -> ShutdownResponse:
        try:
            request = UNMARSHALLER.unmarshal(payload, ShutdownRequest)
            LOG.info("%s: %s", SHUTDOWN, request)
            return json.loads(MARSHALLER.marshal(self._node.shutdown(request)))
        finally:
            sys.exit(0)

    def get(self, payload: dict) -> GetKeyRequest:
        request = UNMARSHALLER.unmarshal(payload, GetKeyRequest)
        LOG.info("%s: %s", GET_KEY, request)
        return json.loads(MARSHALLER.marshal(self._node.get(request)))

    def put(self, payload: dict) -> PutKeyRequest:
        request = UNMARSHALLER.unmarshal(payload, PutKeyRequest)
        LOG.info("%s: %s", PUT_KEY, request)
        return json.loads(MARSHALLER.marshal(self._node.put(request)))


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
