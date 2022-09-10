import logging
import json
import socketserver
import sys
import threading
import time
from xmlrpc.server import SimpleXMLRPCServer

import click

from chord.constants import (
        NODE, CREATE, FIND_SUCCESSOR, JOIN, NOTIFY, GET_PREDECESSOR, GET_SUCCESSOR_LIST, SHUTDOWN,
        GET_KEY, PUT_KEY
)
from chord.exceptions import NodeFailureException
from chord.marshal import JsonChordMarshaller, JsonChordUnmarshaller
from chord.model import (
        CreateRequest, CreateResponse, FindSuccessorRequest, FindSuccessorResponse, GetKeyRequest,
        GetPredecessorRequest, GetPredecessorResponse, GetSuccessorListRequest,
        GetSuccessorListResponse, JoinRequest, JoinResponse, NodeRequest, NodeResponse,
        NotifyRequest, NotifyResponse, PutKeyRequest, ShutdownRequest, ShutdownResponse
)
from chord.node import ChordNode
from chord.storage import DictChordStorage
from chord.xmlrpc.transport import XmlRpcChordTransport


logging.basicConfig()
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


# Needed since XMLRPC marshals object instances to Dict when sending but
# doesn't unmarshal them when receiving.
class ChordNodeHandler:
    def __init__(self, node_id, storage, successor_list_size, ring_size):
        transport = XmlRpcChordTransport()

        self._node: ChordNode = ChordNode(node_id, storage, successor_list_size, ring_size)
        self._marshaller = JsonChordMarshaller()
        self._unmarshaller = JsonChordUnmarshaller(transport)

    def schedule_maintenance_tasks(self):
        def loop():
            while True:
                if self._node.get_successor_list(GetSuccessorListRequest()).successor_list:
                    log.info("Running maintenance tasks...")

                    try:
                        self._node.fix_fingers()
                    except NodeFailureException:
                        log.info("Node failed while trying to fix fingers")

                    try:
                        self._node.stabilize()
                    except NodeFailureException:
                        log.info("Node failed while trying to stabilize")

                    self._node.check_predecessor()

                    log.info("Done.")

                time.sleep(1)

        thread = threading.Thread(target=loop, daemon=True)
        thread.start()

    def create(self, payload: dict) -> CreateResponse:
        request = self._unmarshaller.unmarshal(payload, CreateRequest)
        log.info("%s: %s", CREATE, request)
        return json.loads(self._marshaller.marshal(self._node.create(request)))

    def node(self, payload: dict) -> NodeResponse:
        request = self._unmarshaller.unmarshal(payload, NodeRequest)
        log.info("%s: %s", NODE, request)
        return json.loads(self._marshaller.marshal(self._node.node(request)))

    def join(self, payload: dict) -> JoinResponse:
        request = self._unmarshaller.unmarshal(payload, JoinRequest)
        log.info("%s: %s", JOIN, request)
        return json.loads(self._marshaller.marshal(self._node.join(request)))

    def notify(self, payload: dict) -> NotifyResponse:
        request = self._unmarshaller.unmarshal(payload, NotifyRequest)
        log.info("%s: %s", NOTIFY, request)
        return json.loads(self._marshaller.marshal(self._node.notify(request)))

    def find_successor(self, payload: dict) -> FindSuccessorResponse:
        request = self._unmarshaller.unmarshal(payload, FindSuccessorRequest)
        log.info("%s: %s", FIND_SUCCESSOR, request)
        return json.loads(self._marshaller.marshal(self._node.find_successor(request)))

    def get_predecessor(self, payload: dict) -> GetPredecessorResponse:
        request = self._unmarshaller.unmarshal(payload, GetPredecessorRequest)
        log.info("%s: %s", GET_PREDECESSOR, request)
        return json.loads(self._marshaller.marshal(self._node.get_predecessor(request)))

    def get_successor_list(self, payload: dict) -> GetSuccessorListResponse:
        request = self._unmarshaller.unmarshal(payload, GetSuccessorListRequest)
        log.info("%s: %s", GET_SUCCESSOR_LIST, request)
        return json.loads(self._marshaller.marshal(self._node.get_successor_list(request)))

    def shutdown(self, payload: dict) -> ShutdownResponse:
        try:
            request = self._unmarshaller.unmarshal(payload, ShutdownRequest)
            log.info("%s: %s", SHUTDOWN, request)
            return json.loads(self._marshaller.marshal(self._node.shutdown(request)))
        finally:
            sys.exit(0)

    def get(self, payload: dict) -> GetKeyRequest:
        request = self._unmarshaller.unmarshal(payload, GetKeyRequest)
        log.info("%s: %s", GET_KEY, request)
        return json.loads(self._marshaller.marshal(self._node.get(request)))

    def put(self, payload: dict) -> PutKeyRequest:
        request = self._unmarshaller.unmarshal(payload, PutKeyRequest)
        log.info("%s: %s", PUT_KEY, request)
        return json.loads(self._marshaller.marshal(self._node.put(request)))


# The default SimpleXMLRPCServer is single-threaded. This creates a threaded
# XMLRPC server.
class ThreadedXmlRpcServer(socketserver.ThreadingMixIn, SimpleXMLRPCServer):
    pass


@click.command()
@click.argument("hostname", required=True)
@click.argument("port", required=True)
@click.argument("successor_list_size", required=True)
@click.argument("ring_size", required=True)
def run(hostname: str, port: str, successor_list_size: str, ring_size: str):
    node_id = f"{hostname}:{port}"
    port_num = int(port)
    successor_list_size_num = int(successor_list_size)
    ring_size_num = int(ring_size)

    log.info("Running on %s with successor list size %s and ring size %s...",
            node_id, successor_list_size_num, ring_size_num)
    node = ChordNodeHandler(
            node_id, DictChordStorage(), successor_list_size_num, ring_size_num
    )
    node.schedule_maintenance_tasks()

    with ThreadedXmlRpcServer((hostname, port_num), allow_none=True) as server:
        server.register_instance(node)
        server.serve_forever()


if __name__ == '__main__':
    run()
