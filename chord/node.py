"""
Implements the Chord node.

See https://pdos.csail.mit.edu/papers/ton:chord/paper-ton.pdf
"""
from hashlib import sha256
from typing import Dict, Iterator, List, Optional, Tuple, Union
import logging

from chord.exceptions import NodeFailureException


logger = logging.getLogger(__name__)


class ChordNode:
    """ Base Chord DHT Node implementation. """
    def __init__(self, node_id, storage, successor_list_size, ring_size):
        self.node_id = node_id
        self.predecessor = None
        self._storage = storage
        self._next = 0
        self._is_shutdown = False
        self._successor_list_size = successor_list_size

        self.ring_size = ring_size
        self.fingers = [None] * ring_size
        self.successor_list = None

    def __eq__(self, other):
        return isinstance(other, ChordNode) and self.node_id == other.node_id

    def __hash__(self):
        return hash(self.node_id)

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
        if self.get_successor_list():
            raise RuntimeError("Node already initialized")
        self.successor_list = [self] * self._successor_list_size

    def join(self, remote_node: "ChordNode"):
        """
        Joins a Chord ring.
        :param remote_node: The remote ChordNode to connect to
        """
        if self.get_successor_list():
            raise RuntimeError("Node already initialized")
        successor, _ = remote_node.find_successor(self._bucketize(self.node_id))
        self.successor_list = successor.get_successor_list()

    def node(self) -> "ChordNode":
        """ Returns this ChordNode. Essentially used as a ping operation. """
        return self

    def is_alive(self) -> bool:
        """ Return True if this node is alive. """
        return not self._is_shutdown

    def stabilize(self):
        """
        Verifies this node's successor and tells the successor about this node.
        """
        try:
            current_successor = self.get_successor()

            possible_successor = current_successor.get_predecessor()
            successor_successor_list = current_successor.get_successor_list()

            self.successor_list = [current_successor, *successor_successor_list[:-1]]
            if (possible_successor
                    and self._between_nodes(possible_successor, self, current_successor)):
                try:
                    new_successor_list = possible_successor.get_successor_list()
                    self.successor_list = [possible_successor, *new_successor_list[:-1]]
                except NodeFailureException:
                    # possible_successor is dead, no change
                    pass

        except NodeFailureException:
            # Remove dead successor
            self.successor_list = [*self.successor_list[1:], self]

        self.get_successor().notify(self)

    def notify(self, remote_node):
        """
        Notify this node that a remote node thinks this is its predecessor.
        """
        if (self.get_predecessor() is None
                or self._between_nodes(remote_node, self.get_predecessor(), self)
                or not self.get_predecessor().is_alive()):
            self.set_predecessor(remote_node)

    def fix_fingers(self):
        """ Refreshes finger table entries. """
        finger_bucket = self._bucketize(self.node_id)
        finger_bucket += 2 ** self._next
        finger_bucket %= 2 ** self.ring_size

        self.fingers[self._next], _ = self.find_successor(finger_bucket)

        self._next = (self._next + 1) % self.ring_size

    def check_predecessor(self):
        """ Check that the predecessor is still alive. """
        if self.get_predecessor() and not self.get_predecessor().is_alive():
            self.set_predecessor(None)

    def find_successor(self, key: Union[int, str]) -> Tuple["ChordNode", int]:
        """
        Returns the successor node for a given key or bucket.
        :param key: A string key or numeric bucket
        """
        key_bucket = self._bucketize(key) if isinstance(key, str) else key
        key_bucket %= 2 ** self.ring_size

        current_successor = self.get_successor()
        if (self._between(
                key_bucket,
                self._bucketize(self.node_id),
                self._bucketize(current_successor.node_id)
        ) or self._bucketize(current_successor.node_id) == key):
            try:
                current_successor.node()
                return current_successor, 1
            except NodeFailureException:
                return self, 1

        for closest in self.closest_preceding_nodes(key_bucket):
            if closest == self:
                return self, 1

            try:
                target, hops = closest.find_successor(key_bucket)
                return target, hops + 1
            except NodeFailureException:
                pass  # Dead node, try the next one!

        raise RuntimeError("Found no successor, this should never happen!")

    def closest_preceding_nodes(self, key: int) -> Iterator["ChordNode"]:
        """
        Returns the node in either the finger table or successor list that most
        immediately precedes the key in the Chord ring.
        :param key: A string key
        """
        succeeding_nodes = sorted(
                set(self.successor_list + self.fingers) - {None},
                key=lambda node: self._bucketize(node.node_id)
        )

        for successor in iter(reversed(succeeding_nodes)):
            if successor and self._between(
                self._bucketize(successor.node_id),
                self._bucketize(self.node_id),
                key
            ):
                yield successor

        yield self

    def shutdown(self) -> Dict:
        """ Shuts down the node gracefully. """
        for successor in self.get_successor_list():
            try:
                self._is_shutdown = True
                self.set_predecessor(successor)
                successor.notify(self.get_predecessor())

                for key in self._storage.list():
                    successor.put(key, self._storage.get(key), True)

                return {}

            except NodeFailureException:
                pass

        raise RuntimeError("Didn't find any alive nodes to handoff to!")

    def get_successor(self) -> "ChordNode":
        """ Return this node's successor. """
        return self.successor_list[0]

    def get_successor_list(self) -> List["ChordNode"]:
        """ Returns this node's successor list. """
        return self.successor_list

    # Not using @properties for these since RPC doesn't work well with them
    def get_predecessor(self) -> Optional["ChordNode"]:
        """ Returns the predecessor node. """
        return self.predecessor

    def set_predecessor(self, value):
        """ Sets this node's predecessor. """
        self.predecessor = value

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
        super().__init__(node_id, None, 0, 0)
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

    def find_successor(self, key: Union[int, str]) -> Tuple[ChordNode, int]:
        node_id, hops = self._transport.find_successor(key)
        return RemoteChordNode(self._transport_factory, node_id), hops

    def join(self, remote_node: "ChordNode"):
        self._transport.join(remote_node)

    def notify(self, remote_node: "ChordNode"):
        self._transport.notify(remote_node)

    def get_predecessor(self) -> Optional["ChordNode"]:
        node_id = self._transport.get_predecessor()
        if node_id is None:
            return None
        return RemoteChordNode(self._transport_factory, node_id)

    def set_predecessor(self, value):
        raise NotImplementedError

    def get_successor_list(self):
        return [RemoteChordNode(self._transport_factory, node_id)
                for node_id in self._transport.get_successor_list()]

    def shutdown(self) -> Dict:
        return self._transport.shutdown()

    def get(self, key: str) -> Dict[str, str]:
        return self._transport.get(key)

    def put(self, key: str, value: str, no_redirect: bool=False):
        return self._transport.put(key, value, no_redirect=no_redirect)

    def stabilize(self):
        raise NotImplementedError

    def fix_fingers(self):
        raise NotImplementedError

    def check_predecessor(self):
        raise NotImplementedError

    def closest_preceding_nodes(self, key: int) -> Iterator["ChordNode"]:
        raise NotImplementedError
