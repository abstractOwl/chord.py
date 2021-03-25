from typing import Dict

import requests

from chord.constants import (
        NODE, CREATE, FIND_SUCCESSOR, JOIN, NOTIFY, PREDECESSOR, SHUTDOWN, GET, PUT, TIMEOUT
)
from chord.exceptions import NodeFailureException


class HttpChordTransport:
    """
    HTTP transport implementation for Chord. node_id is {hostname}:{port}.
    """
    def __init__(self, node_id: int):
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

    def find_successor(self, key: int) -> (str, int):
        result = self._make_request(FIND_SUCCESSOR, key=key)
        hops = result["hops"]
        if "node_id" in result["successor"]:
            return result["successor"]["node_id"], hops
        return None, hops

    def join(self, remote_node: "ChordNode"):
        self._make_request(JOIN, node_id=remote_node.node_id)

    def notify(self, remote_node: "ChordNode"):
        self._make_request(NOTIFY, node_id=remote_node.node_id)

    def predecessor(self) -> Dict:
        predecessor = self._make_request(PREDECESSOR)
        if "node_id" in predecessor:
            return predecessor["node_id"]
        return None

    def shutdown(self) -> Dict:
        return self._make_request(SHUTDOWN)

    def get(self, key: str) -> str:
        return self._make_request(GET, key=key)

    def put(self, key: str, value: str, no_redirect: bool=False):
        return self._make_request(PUT, key=key, value=value, no_redirect=no_redirect)


class HttpChordTransportFactory:
    def new_transport(self, node_id: int):
        return HttpChordTransport(node_id)
