import logging
import json
import socketserver
import sys
from xmlrpc.server import SimpleXMLRPCServer

import click

from chord.constants import (
        NODE, CREATE, FIND_SUCCESSOR, JOIN, NOTIFY, GET_PREDECESSOR, GET_SUCCESSOR_LIST, SHUTDOWN,
        GET_KEY, PUT_KEY
)
from chord.marshal import JsonChordMarshaller
from chord.model import (
        CreateRequest, CreateResponse, FindSuccessorRequest, FindSuccessorResponse, GetKeyRequest,
        GetPredecessorRequest, GetPredecessorResponse, GetSuccessorListRequest,
        GetSuccessorListResponse, JoinRequest, JoinResponse, NodeRequest, NodeResponse,
        NotifyRequest, NotifyResponse, PutKeyRequest, ShutdownRequest, ShutdownResponse
)
from chord.node import ChordConfig, ChordNode
from chord.storage import DictChordStorage
from chord.xmlrpc.transport import XmlRpcChordTransport


logging.basicConfig()
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


# Needed since XMLRPC marshals object instances to Dict when sending but
# doesn't unmarshal them when receiving.
class ChordNodeProxy:
    """A marshalling proxy which relays calls to an underlying :class:`ChordNode`."""
    def __init__(self, node_id, storage, config):
        transport = XmlRpcChordTransport()

        self._node: ChordNode = ChordNode(node_id, storage, config)
        self._marshaller = JsonChordMarshaller(transport)

    def schedule_maintenance_tasks(self) -> None:
        self._node.schedule_maintenance_tasks()

    def create(self, payload: dict) -> CreateResponse:
        request = self._marshaller.unmarshal(payload, CreateRequest)
        log.info("%s: %s", CREATE, request)
        return json.loads(self._marshaller.marshal(self._node.create(request)))

    def node(self, payload: dict) -> NodeResponse:
        request = self._marshaller.unmarshal(payload, NodeRequest)
        log.info("%s: %s", NODE, request)
        return json.loads(self._marshaller.marshal(self._node.node(request)))

    def join(self, payload: dict) -> JoinResponse:
        request = self._marshaller.unmarshal(payload, JoinRequest)
        log.info("%s: %s", JOIN, request)
        return json.loads(self._marshaller.marshal(self._node.join(request)))

    def notify(self, payload: dict) -> NotifyResponse:
        request = self._marshaller.unmarshal(payload, NotifyRequest)
        log.info("%s: %s", NOTIFY, request)
        return json.loads(self._marshaller.marshal(self._node.notify(request)))

    def find_successor(self, payload: dict) -> FindSuccessorResponse:
        request = self._marshaller.unmarshal(payload, FindSuccessorRequest)
        log.info("%s: %s", FIND_SUCCESSOR, request)
        return json.loads(self._marshaller.marshal(self._node.find_successor(request)))

    def get_predecessor(self, payload: dict) -> GetPredecessorResponse:
        request = self._marshaller.unmarshal(payload, GetPredecessorRequest)
        log.info("%s: %s", GET_PREDECESSOR, request)
        return json.loads(self._marshaller.marshal(self._node.get_predecessor(request)))

    def get_successor_list(self, payload: dict) -> GetSuccessorListResponse:
        request = self._marshaller.unmarshal(payload, GetSuccessorListRequest)
        log.info("%s: %s", GET_SUCCESSOR_LIST, request)
        return json.loads(self._marshaller.marshal(self._node.get_successor_list(request)))

    def shutdown(self, payload: dict) -> ShutdownResponse:
        try:
            request = self._marshaller.unmarshal(payload, ShutdownRequest)
            log.info("%s: %s", SHUTDOWN, request)
            return json.loads(self._marshaller.marshal(self._node.shutdown(request)))
        finally:
            sys.exit(0)

    def get(self, payload: dict) -> GetKeyRequest:
        request = self._marshaller.unmarshal(payload, GetKeyRequest)
        log.info("%s: %s", GET_KEY, request)
        return json.loads(self._marshaller.marshal(self._node.get(request)))

    def put(self, payload: dict) -> PutKeyRequest:
        request = self._marshaller.unmarshal(payload, PutKeyRequest)
        log.info("%s: %s", PUT_KEY, request)
        return json.loads(self._marshaller.marshal(self._node.put(request)))


class ThreadedXmlRpcServer(socketserver.ThreadingMixIn, SimpleXMLRPCServer):
    """An extended :class:`SimpleXMLRPCServer` which supports multi-threading."""


@click.command()
@click.argument("hostname", required=True)
@click.argument("port", required=True)
@click.argument("ring_size", required=True)
def run(hostname: str, port: str, ring_size: str):
    node_id = f"{hostname}:{port}"
    port_num = int(port)
    ring_size_num = int(ring_size)

    log.info("Running on %s with ring size %s...", node_id, ring_size_num)
    node = ChordNodeProxy(node_id, DictChordStorage(), ChordConfig(ring_size_num))
    node.schedule_maintenance_tasks()

    with ThreadedXmlRpcServer((hostname, port_num), allow_none=True) as server:
        server.register_instance(node)
        server.serve_forever()


if __name__ == '__main__':
    run()
