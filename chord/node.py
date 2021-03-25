"""
Implements the Chord node.

See https://pdos.csail.mit.edu/papers/ton:chord/paper-ton.pdf
"""
from collections import deque
from hashlib import sha256
from typing import Dict, Union
import logging

from chord.exceptions import NodeFailureException
from chord.transport import HttpChordTransport

logger = logging.getLogger(__name__)


class ChordNode:
    """ Base Chord DHT Node implementation. """
    def __init__(self, node_id, storage, ring_size):
        self.node_id = node_id
        self._predecessor = None
        self._storage = storage
        self._next = 0
        self._is_shutdown = False

        self.ring_size = ring_size
        self.fingers = deque([None] * ring_size, ring_size)

    def __eq__(self, other):
        return isinstance(other, ChordNode) and self.node_id == other.node_id

    def __repr__(self):
        bucket = repr(self._bucketize(self.node_id))
        return f"{__name__}({self.node_id}, {bucket}, {self.ring_size})"

    def _bucketize(self, string: str):
        """
        Returns the consistent hash for a string.
        :param key: The key to hash
        """
        digest = sha256(string.encode()).digest()
        bucket = int.from_bytes(digest, 'big') % 2 ** self.ring_size
        return bucket

    @staticmethod
    def _between(target: int, lower: int, higher: int) -> bool:
        """
        Returns True if the target falls between the lower and higher buckets.
        """
        if lower < higher:
            return lower < target < higher

        # Wrap-around case, also True in cases where lower == higher
        return lower < target or target < higher

    def _between_nodes(
            self,
            target_node: "ChordNode",
            lower_node: "ChordNode",
            higher_node: "ChordNode") -> bool:
        """
        Returns True if the target node falls between the lower and higher
        nodes.
        """
        return self._between(
                self._bucketize(target_node.node_id),
                self._bucketize(lower_node.node_id),
                self._bucketize(higher_node.node_id)
        )

    def create(self):
        """ Creates a Chord ring. """
        if self.successor:
            raise RuntimeError("Node already initialized")
        self.successor = self

    def join(self, remote_node: "ChordNode"):
        """
        Joins a Chord ring.
        :param remote_node: The remote ChordNode to connect to
        """
        if self.successor:
            raise RuntimeError("Node already initialized")
        self.successor, _ = remote_node.find_successor(self._bucketize(self.node_id))

    def node(self) -> "ChordNode":
        """ Returns this ChordNode. Essentially used as a ping operation. """
        return self

    def is_alive(self) -> bool:
        """ Return True if this node is alive. """
        return True

    def stabilize(self):
        """
        Verifies this node's successor and tells the successor about this node.
        """
        possible_successor = self.successor.predecessor

        if (possible_successor and self._between_nodes(possible_successor, self, self.successor)
                and possible_successor.is_alive()):
            self.successor = possible_successor

        if self.successor != self and not self._is_shutdown:
            self.successor.notify(self)

    def notify(self, remote_node):
        """
        Notify this node that a remote node thinks this is its predecessor.
        """
        if (self.predecessor is None
                or self._between_nodes(remote_node, self.predecessor, self)):
            self.predecessor = remote_node
        elif not self.predecessor.is_alive():
            self.predecessor = remote_node

    def fix_fingers(self):
        """ Refreshes finger table entries. """
        finger_bucket = self._bucketize(self.node_id)
        finger_bucket += 2 ** self._next
        finger_bucket %= 2 ** self.ring_size

        self.fingers[self._next], _ = self.find_successor(finger_bucket)

        self._next = (self._next + 1) % self.ring_size

    def check_predecessor(self):
        """ Check that the predecessor is still alive. """
        if self.predecessor and not self.predecessor.is_alive():
            self.predecessor = None

    def find_successor(self, key: Union[int, str]) -> ("ChordNode", int):
        """
        Returns the successor node for a given key or bucket.
        :param key: A string key or numeric bucket
        """
        if isinstance(key, str):
            key = self._bucketize(key)

        key %= 2 ** self.ring_size

        if (self._between(
                key,
                self._bucketize(self.node_id),
                self._bucketize(self.successor.node_id)
        ) or self._bucketize(self.successor.node_id) == key):
            if not self.successor.is_alive():
                return self, 1

            return self.successor, 1

        closest = self.closest_preceding_node(key)
        if closest == self:
            return self, 1
        target, hops = closest.find_successor(key)
        return target, hops + 1

    def closest_preceding_node(self, key: int) -> "ChordNode":
        """
        Returns the closest node in the finger table that precedes the key in
        the Chord ring.
        :param key: A string key
        """
        for finger in iter(reversed(self.fingers)):
            if finger and self._between(
                self._bucketize(finger.node_id),
                self._bucketize(self.node_id),
                key
            ) and finger.is_alive():
                return finger
        return self

    def shutdown(self) -> Dict:
        """ Shuts down the node gracefully. """
        self._is_shutdown = True
        self._predecessor = self.successor
        self.successor.notify(self.predecessor)

        for key in self._storage.list():
            self.successor.put(key, self._storage.get(key), True)

        return {}

    @property
    def successor(self) -> "ChordNode":
        """ Returns the successor node. """
        return self.fingers[0]

    @successor.setter
    def successor(self, value):
        self.fingers[0] = value

    @property
    def predecessor(self) -> "ChordNode":
        """ Returns the predecessor node. """
        return self._predecessor

    @predecessor.setter
    def predecessor(self, value):
        self._predecessor = value

    def get(self, key: str) -> Dict:
        """
        Returns the value for a key from Chord storage.
        :param key: The string key to retrieve a value for
        """
        node, hops = self.find_successor(key)
        if node == self:
            return {
                "hops": hops,
                "storage_node": self.node_id,
                "value": self._storage.get(key)
            }
        return node.get(key)

    def put(self, key: str, value: str, no_redirect: bool=False) -> Dict:
        """
        Puts a value into Chord storage.
        :param key: The string key to store
        :param value: The string value to associate with the key
        :param no_redirect: True if the specified value should be posted
                            directly to the storage of this node, rather
                            than to the key's successor.
        """
        node, hops = self.find_successor(key)
        if node == self or no_redirect:
            self._storage.put(key, value)
            return {
                "hops": hops,
                "storage_node": self.node_id
            }
        return node.put(key, value)


class RemoteChordNode(ChordNode):
    """ ChordNode adapter for remote operations. """

    def __init__(self, transport_factory, node_id: str):
        super().__init__(node_id, None, 0)
        self._transport_factory = transport_factory
        self._transport = transport_factory.new_transport(node_id)

    def __repr__(self):
        return f"{__name__}({self.node_id})"

    def node(self) -> "ChordNode":
        return self._transport.node()

    def is_alive(self) -> bool:
        try:
            self.node()
            return True
        except NodeFailureException:
            return False

    def create(self):
        self._transport.create()

    def find_successor(self, key: int) -> ("ChordNode", int):
        node_id, hops = self._transport.find_successor(key)

        if node_id is None:
            return None, hops
        return RemoteChordNode(self._transport_factory, node_id), hops

    def join(self, remote_node: "ChordNode"):
        self._transport.join(remote_node)

    def notify(self, remote_node: "ChordNode"):
        self._transport.notify(remote_node)

    @property
    def predecessor(self) -> "ChordNode":
        node_id = self._transport.predecessor()
        if node_id is None:
            return None
        return RemoteChordNode(self._transport_factory, node_id)

    def shutdown(self) -> Dict:
        return self._transport.shutdown()

    def get(self, key: str) -> str:
        return self._transport.get(key)

    def put(self, key: str, value: str, no_redirect: bool=False):
        return self._transport.put(key, value, no_redirect=no_redirect)

    def stabilize(self):
        raise NotImplementedError

    def fix_fingers(self):
        raise NotImplementedError

    def check_predecessor(self):
        raise NotImplementedError

    def closest_preceding_node(self, key: int) -> "ChordNode":
        raise NotImplementedError

    @predecessor.setter
    def predecessor(self, value):
        raise NotImplementedError

    @property
    def successor(self):
        raise NotImplementedError

    @successor.setter
    def successor(self, value):
        raise NotImplementedError
