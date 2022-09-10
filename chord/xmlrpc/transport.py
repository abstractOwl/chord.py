import json

from typing import Callable, Type
from xmlrpc.client import ServerProxy

from chord.exceptions import NodeFailureException
from chord.marshal import (
        ChordMarshaller, ChordUnmarshaller, JsonChordMarshaller, JsonChordUnmarshaller
)
from chord.model import BaseRequest, BaseResponse
from chord.transport import ChordConnection, ChordTransport


class XmlRpcChordConnection(ChordConnection):
    """
    XML-RPC connection implementation for Chord. node_id is {hostname}:{port}.
    """
    def __init__(self, marshaller: ChordMarshaller, unmarshaller: ChordUnmarshaller, node_id: str):
        self._marshaller = marshaller
        self._unmarshaller = unmarshaller
        self._node_id = node_id

    def _get_client(self) -> ServerProxy:
        try:
            return ServerProxy(f"http://{self._node_id}", allow_none=True)
        except ConnectionError as ex:
            raise NodeFailureException(f"Failed to connect: {self._node_id}") from ex

    def make_request(
            self, command: str, request: BaseRequest, response_cls: Type[BaseResponse]
    ) -> BaseResponse:
        client = self._get_client()
        command_fn: Callable = getattr(client, command)
        try:
            params = self._marshaller.marshal(request)
            obj = command_fn(json.loads(params))
            return self._unmarshaller.unmarshal(obj, response_cls)

        except ConnectionError as ex:
            raise NodeFailureException(f"Failed: {self._node_id}/{command}") from ex


class XmlRpcChordTransport(ChordTransport):
    def __init__(self):
        self._marshaller = JsonChordMarshaller()
        self._unmarshaller = JsonChordUnmarshaller(self)

    def create_connection(self, node_id: str) -> ChordConnection:
        return XmlRpcChordConnection(self._marshaller, self._unmarshaller, node_id)
