from typing import Dict, Tuple
from xmlrpc.client import ServerProxy

from chord.exceptions import NodeFailureException
from chord.node import ChordNode


def translate_faults(func):
    def _inner(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (ConnectionError) as ex:
            raise NodeFailureException() from ex
    return _inner


class XmlRpcChordTransport:
    """
    HTTP transport implementation for Chord. node_id is {hostname}:{port}.
    """
    def __init__(self, transport_factory, node_id: int):
        self._transport_factory = transport_factory
        self.node_id = node_id

    @translate_faults
    def _get_client(self):
        return ServerProxy(f"http://{self.node_id}", allow_none=True)

    @translate_faults
    def node(self) -> Dict:
        return self._get_client().node()

    @translate_faults
    def create(self):
        self._get_client().create()

    @translate_faults
    def find_successor(self, key: int) -> Tuple[str, int]:
        return self._get_client().find_successor(key)

    @translate_faults
    def join(self, remote_node: ChordNode):
        self._get_client().join(remote_node.node_id)

    @translate_faults
    def notify(self, remote_node: ChordNode):
        self._get_client().notify(remote_node.node_id)

    @translate_faults
    def get_predecessor(self) -> Dict:
        return self._get_client().get_predecessor()

    @translate_faults
    def get_successor_list(self) -> Dict:
        return self._get_client().get_successor_list()

    @translate_faults
    def shutdown(self) -> Dict:
        return self._get_client().shutdown()

    @translate_faults
    def get(self, key: str) -> str:
        return self._get_client().get(key)

    @translate_faults
    def put(self, key: str, value: str, no_redirect: bool=False):
        return self._get_client().put(key, value, no_redirect)


class XmlRpcChordTransportFactory:
    def new_transport(self, node_id: int):
        return XmlRpcChordTransport(self, node_id)
