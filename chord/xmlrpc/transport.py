import json

from typing import Dict, Tuple, Type
from xmlrpc.client import ServerProxy

from chord.exceptions import NodeFailureException
from chord.marshal import JsonChordMarshaller, JsonChordUnmarshaller
from chord.model import BaseRequest, BaseResponse
from chord.node import ChordNode


class XmlRpcChordTransport:
    """
    XML-RPC transport implementation for Chord. node_id is {hostname}:{port}.
    """
    def __init__(self, transport_factory, node_id: int):
        self._transport_factory = transport_factory
        self.node_id = node_id
        self.marshaller = JsonChordMarshaller()
        self.unmarshaller = JsonChordUnmarshaller(transport_factory)

    def _get_client(self):
        try:
            return ServerProxy(f"http://{self.node_id}", allow_none=True)
        except ConnectionError as ex:
            raise NodeFailureException(f"Failed to connect: {self.node_id}/{command}") from ex

    def make_request(self, command: str, request: BaseRequest, response_cls: Type[BaseResponse]) -> Dict:
        client = self._get_client()
        fn = getattr(client, command)
        try:
            params = self.marshaller.marshal(request)
            obj = fn(json.loads(params))
            return self.unmarshaller.unmarshal(obj, response_cls)

        except ConnectionError as ex:
            raise NodeFailureException(f"Failed: {self.node_id}/{command}") from ex


class XmlRpcChordTransportFactory:
    def new_transport(self, node_id: int):
        return XmlRpcChordTransport(self, node_id)
