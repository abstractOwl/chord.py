from typing import Dict

import requests

from chord.constants import (NODE, CREATE, FIND_SUCCESSOR, JOIN, NOTIFY, PREDECESSOR, TIMEOUT)
from chord.exceptions import NodeFailureException


class HttpChordTransport:
    """
    HTTP transport implementation for Chord. node_id is {hostname}:{port}.
    """
    def __init__(self, node_id):
        self.node_id = node_id

    def _make_request(self, command, **params):
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

    def find_successor(self, key: int) -> Dict:
        return self._make_request(FIND_SUCCESSOR, key=key)

    def join(self, remote_node: "ChordNode"):
        self._make_request(JOIN, node_id=remote_node.node_id)

    def notify(self, remote_node: "ChordNode"):
        self._make_request(NOTIFY, node_id=remote_node.node_id)

    def predecessor(self) -> Dict:
        return self._make_request(PREDECESSOR)
