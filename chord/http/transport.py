from typing import Dict, List, Optional, Tuple

import requests

from chord.exceptions import NodeFailureException
from chord.http.constants import (
        NODE, CREATE, FIND_SUCCESSOR, JOIN, NOTIFY, GET_PREDECESSOR, GET_SUCCESSOR_LIST, SHUTDOWN,
        GET, PUT, TIMEOUT
)
from chord.node import ChordNode


class HttpChordTransport:
    """
    HTTP transport implementation for Chord. node_id is {hostname}:{port}.
    """
    def __init__(self, node_id: str):
        self.node_id = node_id

    def _make_request(self, command: str, **params):
        try:
            return requests.get(
                    f"http://{self.node_id}{command}",
                    params=params,
                    timeout=TIMEOUT
            ).json()
        except requests.exceptions.RequestException as ex:
            raise NodeFailureException(f"Failed: {self.node_id}{command}") from ex

    def node(self) -> Dict:
        return self._make_request(NODE)

    def create(self):
        self._make_request(CREATE)

    def find_successor(self, key: int) -> Tuple[str, int]:
        result = self._make_request(FIND_SUCCESSOR, key=key)
        hops = result["hops"]
        return result["successor"]["node_id"], hops

    def join(self, remote_node: ChordNode):
        self._make_request(JOIN, node_id=remote_node.node_id)

    def notify(self, remote_node: ChordNode):
        self._make_request(NOTIFY, node_id=remote_node.node_id)

    def get_predecessor(self) -> Optional[str]:
        predecessor = self._make_request(GET_PREDECESSOR)
        if "node_id" in predecessor:
            return predecessor["node_id"]
        return None

    def get_successor_list(self) -> List[str]:
        return self._make_request(GET_SUCCESSOR_LIST)

    def shutdown(self) -> Dict:
        return self._make_request(SHUTDOWN)

    def get(self, key: str) -> str:
        return self._make_request(GET, key=key)

    def put(self, key: str, value: str, no_redirect: bool=False):
        return self._make_request(PUT, key=key, value=value, no_redirect=no_redirect)


class HttpChordTransportFactory:
    def new_transport(self, node_id: str):
        return HttpChordTransport(node_id)
