"""
Implements the Chord node.

See https://pdos.csail.mit.edu/papers/ton:chord/paper-ton.pdf
"""
from collections import deque
from hashlib import sha256
from typing import Union
import logging

from chord.exceptions import NodeFailureException
from chord.transport import HttpChordTransport

logger = logging.getLogger(__name__)


class ChordNode:
    """ Base Chord DHT Node implementation. """
    def __init__(self, node_id, ring_size):
        self.node_id = node_id
        self._predecessor = None
        self._successor = None
        self._next = 0

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
        bucket = int.from_bytes(digest, 'big') % self.ring_size
        return bucket

    @staticmethod
    def _between(target: int, lower: int, higher: int) -> bool:
        """
        Returns True if the target falls between the lower and higher buckets.
        """
        if lower == higher:  # Special case when node owns entire ring
            return True

        if lower > higher:  # Wrap-around case
            return lower < target or target < higher

        return lower < target < higher

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
        self._successor = self

    def join(self, remote_node: "ChordNode"):
        """
        Joins a Chord ring.
        :param remote_node: The remote ChordNode to connect to
        """
        if self.successor:
            raise RuntimeError("Node already initialized")
        self._successor = remote_node.find_successor(self._bucketize(self.node_id))
        self.successor.notify(self)

    def node(self) -> "ChordNode":
        """ Returns this ChordNode. """
        return self

    def stabilize(self):
        """
        Verifies this node's successor and tells the successor about this node.
        """
        possible_successor = self.successor.predecessor

        if possible_successor and self._between_nodes(possible_successor, self, self.successor):
            self._successor = possible_successor
            self.successor.notify(self)

    def notify(self, remote_node):
        """
        Notify this node that a remote node thinks this is its predecessor.
        """
        if (self._predecessor is None
                or self._between_nodes(remote_node, self._predecessor, self)):
            self._predecessor = remote_node

            if self.successor == self:  # Base case: set successor when expanding from 1 to 2 nodes
                self._successor = remote_node

    def fix_fingers(self):
        """ Refreshes finger table entries. """
        finger_bucket = self._bucketize(self.node_id)
        finger_bucket += 2 ** (self._next - 1)
        finger_bucket = finger_bucket % self.ring_size

        self.fingers[self._next] = self.find_successor(finger_bucket)

        self._next = (self._next + 1) % self.ring_size

    def check_predecessor(self):
        """ Check that the predecessor is still alive. """
        if self.predecessor:
            try:
                self.predecessor.node()
            except NodeFailureException:
                self._predecessor = None

    def find_successor(self, key: Union[int, str]) -> "ChordNode":
        """
        Returns the successor node for a given key or bucket.
        :param key: A string key or numeric bucket
        """
        if isinstance(key, str):
            key = self._bucketize(key)

        key %= self.ring_size

        if (self._between(
                key,
                self._bucketize(self.node_id),
                self._bucketize(self.successor.node_id)
        ) or self._bucketize(self.successor.node_id) == key):
            return self.successor

        closest = self.closest_preceding_node(key)
        if closest == self:
            return self
        return closest.find_successor(key)

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
                    key):
                return finger
        return self

    @property
    def successor(self) -> "ChordNode":
        """ Returns the successor node. """
        return self._successor

    @property
    def predecessor(self) -> "ChordNode":
        """ Returns the predecessor node. """
        return self._predecessor


class RemoteChordNode(ChordNode):
    """ ChordNode adapter for remote operations. """

    def __init__(self, node_id: str):
        super().__init__(node_id, 0)
        self._transport = HttpChordTransport(node_id)

    def __repr__(self):
        return f"{__name__}({self.node_id})"

    def node(self) -> "ChordNode":
        return self._transport.node()

    def create(self):
        self._transport.create()

    def find_successor(self, key: int) -> "ChordNode":
        successor = self._transport.find_successor(key)
        if successor is None:
            return None
        return RemoteChordNode(successor)

    def join(self, remote_node: "ChordNode"):
        self._transport.join(remote_node)

    def notify(self, remote_node: "ChordNode"):
        self._transport.notify(remote_node)

    @property
    def predecessor(self) -> "ChordNode":
        predecessor = self._transport.predecessor()
        if predecessor is None:
            return None
        return RemoteChordNode(predecessor)

    def stabilize(self):
        raise NotImplementedError

    def fix_fingers(self):
        raise NotImplementedError

    def check_predecessor(self):
        raise NotImplementedError

    def closest_preceding_node(self, key: int) -> "ChordNode":
        raise NotImplementedError

    @property
    def successor(self):
        raise NotImplementedError
