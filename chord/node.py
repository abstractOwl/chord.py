"""
Implements the Chord node.

See https://pdos.csail.mit.edu/papers/ton:chord/paper-ton.pdf
"""
from __future__ import annotations
from hashlib import sha256
from typing import Dict, Iterator, List, Optional, Tuple, Union
import logging

from chord.constants import *
from chord.exceptions import NodeFailureException
from chord.model import *
from chord.storage import DictChordStorage


logger = logging.getLogger(__name__)


class ChordNode:
    """ Base Chord DHT Node implementation. """
    def __init__(self, node_id: str, storage: DictChordStorage, successor_list_size: int, ring_size: int):
        # TODO: Split options into protocol, config, storage_handler
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

    def create(self, request: CreateRequest) -> CreateResponse:
        """ Creates a Chord ring. """
        if self.get_successor_list(GetSuccessorListRequest()).successor_list:
            raise RuntimeError("Node already initialized")
        self.successor_list = [self] * self._successor_list_size
        return CreateResponse()

    def join(self, request: JoinRequest) -> JoinResponse:
        """
        Joins a Chord ring.
        :param remote_node: The remote ChordNode to connect to
        """
        if self.get_successor_list(GetSuccessorListRequest()).successor_list:
            raise RuntimeError("Node already initialized")
        successor = request.remote_node.find_successor(FindSuccessorRequest(self._bucketize(self.node_id))).node
        self.successor_list = successor.get_successor_list(GetSuccessorListRequest()).successor_list
        return JoinResponse()

    def node(self, request: NodeRequest) -> NodeResponse:
        """ Returns this ChordNode. Essentially used as a ping operation. """
        return NodeResponse(self)

    def is_alive(self) -> bool:
        """ Return True if this node is alive. """
        return not self._is_shutdown

    def stabilize(self):
        """
        Verifies this node's successor and tells the successor about this node.
        """
        try:
            current_successor = self._get_successor()

            possible_successor = current_successor.get_predecessor(GetPredecessorRequest()).node
            successor_successor_list = current_successor.get_successor_list(GetSuccessorListRequest()).successor_list

            self.successor_list = [current_successor, *successor_successor_list[:-1]]
            if (possible_successor
                    and self._between_nodes(possible_successor, self, current_successor)):
                try:
                    new_successor_list = possible_successor.get_successor_list(GetSuccessorListRequest()).successor_list
                    self.successor_list = [possible_successor, *new_successor_list[:-1]]
                except NodeFailureException:
                    # possible_successor is dead, no change
                    pass

        except NodeFailureException:
            # Remove dead successor
            self.successor_list = [*self.successor_list[1:], self]

        self._get_successor().notify(NotifyRequest(self))

    def notify(self, request: NotifyRequest) -> NotifyResponse:
        """
        Notify this node that a remote node thinks this is its predecessor.
        """
        predecessor = self.get_predecessor(GetPredecessorRequest()).node
        if (predecessor is None
                or self._between_nodes(request.remote_node, predecessor, self)
                or not predecessor.is_alive()):
            self.set_predecessor(request.remote_node)
        return NotifyResponse()

    def fix_fingers(self):
        """ Refreshes finger table entries. """
        finger_bucket = self._bucketize(self.node_id)
        finger_bucket += 2 ** self._next
        finger_bucket %= 2 ** self.ring_size

        self.fingers[self._next] = self.find_successor(FindSuccessorRequest(finger_bucket)).node

        self._next = (self._next + 1) % self.ring_size

    def check_predecessor(self):
        """ Check that the predecessor is still alive. """
        predecessor = self.get_predecessor(GetPredecessorRequest()).node
        if predecessor and not predecessor.is_alive():
            self.set_predecessor(None)

    def find_successor(self, request: FindSuccessorRequest) -> FindSuccessorResponse:
        """
        Returns the successor node for a given key or bucket.
        :param key: A string key or numeric bucket
        """
        key_bucket = request.key if isinstance(request.key, int) else self._bucketize(str(request.key))
        key_bucket %= 2 ** self.ring_size

        current_successor = self._get_successor()
        if (self._between(
                key_bucket,
                self._bucketize(self.node_id),
                self._bucketize(current_successor.node_id)
        ) or self._bucketize(current_successor.node_id) == request.key):
            if current_successor.is_alive():
                return FindSuccessorResponse(current_successor, 1)
            else:
                return FindSuccessorResponse(self, 1)

        for closest in self.closest_preceding_nodes(key_bucket):
            if closest == self:
                return FindSuccessorResponse(self, 1)

            try:
                response = closest.find_successor(FindSuccessorRequest(key_bucket))
                return FindSuccessorResponse(response.node, response.hops + 1)
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

    def shutdown(self, request: ShutdownRequest) -> ShutdownResponse:
        """ Shuts down the node gracefully. """
        successor_list = self.get_successor_list(GetSuccessorListRequest()).successor_list
        for successor in successor_list:
            try:
                self._is_shutdown = True
                self.set_predecessor(successor)
                successor.notify(NotifyRequest(self.get_predecessor(GetPredecessorRequest()).node))

                for key in self._storage.list():
                    successor.put(PutKeyRequest(key, self._storage.get(key), True))

                return ShutdownResponse()

            except NodeFailureException:
                pass

        raise RuntimeError("Didn't find any alive nodes to handoff to!")

    def _get_successor(self) -> ChordNode:
        """ Return this node's successor. """
        return self.successor_list[0]

    def get_successor_list(self, request: GetSuccessorListRequest) -> GetSuccessorListResponse:
        """ Returns this node's successor list. """
        return GetSuccessorListResponse(self.successor_list)

    # Not using @properties for these since RPC doesn't work well with them
    def get_predecessor(self, request: GetPredecessorRequest) -> GetPredecessorResponse:
        """ Returns the predecessor node. """
        return GetPredecessorResponse(self.predecessor)

    def set_predecessor(self, value: ChordNode):
        """ Sets this node's predecessor. """
        self.predecessor = value

    def get(self, request: GetKeyRequest) -> GetKeyResponse:
        """
        Returns the value for a key from Chord storage.
        :param key: The string key to retrieve a value for
        """
        response = self.find_successor(FindSuccessorRequest(request.key))
        if response.node == self:
            if self._storage.has(request.key):
                return GetKeyResponse(self, response.hops, self._storage.get(request.key), True)
            else:
                return GetKeyResponse(self, response.hops, None, False)
        return response.node.get(request)

    def put(self, request: PutKeyRequest) -> PutKeyResponse:
        """
        Puts a value into Chord storage.
        :param key: The string key to store
        :param value: The string value to associate with the key
        :param no_redirect: True if the specified value should be posted
                            directly to the storage of this node, rather
                            than to the key's successor.
        """
        response = self.find_successor(FindSuccessorRequest(request.key))
        if response.node == self or request.no_redirect:
            self._storage.put(request.key, request.value)
            return PutKeyResponse(self, response.hops)
        return response.node.put(request)


class RemoteChordNode(ChordNode):
    """ ChordNode proxy for remote operations. """

    def __init__(self, transport_factory, node_id: str):
        super().__init__(node_id, None, 0, 0)
        self._transport_factory = transport_factory
        self._transport = transport_factory.new_transport(node_id)

    def __repr__(self):
        return f"{__name__}({self.node_id})"

    def node(self, request: NodeRequest) -> NodeResponse:
        return self._transport.make_request(NODE, request, NodeResponse)

    def is_alive(self) -> bool:
        try:
            self.node(NodeRequest())
            return True
        except NodeFailureException:
            return False

    def create(self, request: CreateRequest) -> CreateResponse:
        return self._transport.make_request(CREATE, request, CreateResponse)

    def find_successor(self, request: FindSuccessorRequest) -> FindSuccessorResponse:
        return self._transport.make_request(FIND_SUCCESSOR, request, FindSuccessorResponse)

    def join(self, request: JoinRequest) -> JoinResponse:
        return self._transport.make_request(JOIN, request, JoinResponse)

    def notify(self, request: NotifyRequest) -> NotifyResponse:
        return self._transport.make_request(NOTIFY, request, NotifyResponse)

    def get_predecessor(self, request: GetPredecessorRequest) -> GetPredecessorResponse:
        return self._transport.make_request(GET_PREDECESSOR, request, GetPredecessorResponse)

    def set_predecessor(self, value: ChordNode):
        raise NotImplementedError

    def _get_successor(self) -> ChordNode:
        raise NotImplementedError

    def get_successor_list(self, request: GetSuccessorListRequest) -> GetSuccessorListResponse:
        return self._transport.make_request(GET_SUCCESSOR_LIST, request, GetSuccessorListResponse)

    def shutdown(self, request: ShutdownRequest) -> ShutdownResponse:
        return self._transport.make_request(SHUTDOWN, request, ShutdownResponse)

    def get(self, request: GetKeyRequest) -> GetKeyResponse:
        return self._transport.make_request(GET_KEY, request, GetKeyResponse)

    def put(self, request: PutKeyRequest) -> PutKeyResponse:
        return self._transport.make_request(PUT_KEY, request, PutKeyResponse)

    def stabilize(self):
        raise NotImplementedError

    def fix_fingers(self):
        raise NotImplementedError

    def check_predecessor(self):
        raise NotImplementedError

    def closest_preceding_nodes(self, key: int) -> Iterator["ChordNode"]:
        raise NotImplementedError
