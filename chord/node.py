"""
Implements the Chord node.

See https://pdos.csail.mit.edu/papers/ton:chord/paper-ton.pdf
"""
from __future__ import annotations
from collections.abc import Iterator
from dataclasses import dataclass
from hashlib import sha256
import logging
import threading
import time
from typing import Optional

from chord.constants import (
        CREATE, NODE, NOTIFY, FIND_SUCCESSOR, GET_PREDECESSOR, GET_SUCCESSOR_LIST,
        JOIN, SHUTDOWN, GET_KEY, PUT_KEY
)
from chord.exceptions import NodeFailureException
from chord.model import (
        NodeRequest, NodeResponse, CreateRequest, CreateResponse, FindSuccessorRequest,
        FindSuccessorResponse, JoinRequest, JoinResponse, NotifyRequest, NotifyResponse,
        GetPredecessorRequest, GetPredecessorResponse, GetSuccessorListRequest,
        GetSuccessorListResponse, ShutdownRequest, ShutdownResponse, GetKeyRequest, GetKeyResponse,
        PutKeyRequest, PutKeyResponse
)
from chord.storage import ChordStorage, NullChordStorage


logger = logging.getLogger(__name__)


@dataclass
class ChordConfig:
    """Chord node configuration options"""
    ring_size: int = 8


class ChordNode:
    """Base Chord DHT node implementation."""
    def __init__(self, node_id: str, storage: ChordStorage, config: ChordConfig):
        self.node_id = node_id
        self._storage = storage
        self._config = config

        self._next_finger_to_fix = 0
        self._is_shutdown = True

        self._predecessor: Optional[ChordNode] = None
        self._fingers: list[Optional[ChordNode]] = [None] * config.ring_size
        self._successor_list: list[ChordNode] = []

    def schedule_maintenance_tasks(self, interval_seconds: int=1):
        """Schedules the stabilization tasks in a separate thread.

        These tasks are: `fix_fingers`, `stabilize`, and `check_predecessor`.

        :param interval_seconds: The rate at which to run the tasks, in seconds.
        """
        # TODO: Separate these tasks and figure out the proper rates to run these at
        def loop():
            while True:
                if not self._is_shutdown:
                    start_time = time.time()
                    logger.debug("Running maintenance tasks...")

                    try:
                        self.fix_fingers()
                    except NodeFailureException:
                        logger.info("Node failed while trying to fix fingers")

                    try:
                        self.stabilize()
                    except NodeFailureException:
                        logger.info("Node failed while trying to stabilize")

                    self.check_predecessor()

                    elapsed_seconds = time.time() - start_time
                    logger.debug("Finished maintenance tasks in %f seconds", elapsed_seconds)

                time.sleep(interval_seconds)

        thread = threading.Thread(target=loop, daemon=True)
        thread.start()

    def __eq__(self, other) -> bool:
        return isinstance(other, ChordNode) and self.node_id == other.node_id

    def __hash__(self) -> int:
        return hash(self.node_id)

    def __repr__(self) -> str:
        bucket = repr(self._bucketize(self.node_id))
        return f"{__name__}({self.node_id}, {bucket}, {self._config.ring_size})"

    def _bucketize(self, string: str) -> int:
        """Hashes a given string to an integer bucket using consistent hashing.

        Strings will be hashed to the same bucket as long as `ring_size` stays the same.

        :param string: The string to hash
        :return: The bucket `string` was hashed to
        """
        digest = sha256(string.encode()).digest()
        bucket = int.from_bytes(digest, 'big') % 2 ** self._config.ring_size
        return bucket

    @staticmethod
    def _between(target: int, lower: int, higher: int) -> bool:
        """Determines whether a target falls between the lower and higher buckets (non-inclusive).

        :param target: The bucket to compare
        :param lower: The lower bucket. This can be higher in value than `higher` in instances
                      where the comparison wraps around.
        :param higher: The higher bucket
        :return: `True` if the `target` bucket falls between `lower` and `higher`
        """
        if lower < higher:
            return lower < target < higher

        # Wrap-around case, also True in cases where lower == higher
        return lower < target or target < higher

    def _between_nodes(
            self, target_node: ChordNode, lower_node: ChordNode, higher_node: ChordNode
    ) -> bool:
        """Determines whether a target node falls between the lower and higher nodes.

        :param target_node: The target node to compare
        :param lower_node: The lower node
        :param higher_node: The higher node
        :return: `True` if the `target_node` falls between `lower_node` and `higher_node`
        """
        return self._between(
                self._bucketize(target_node.node_id),
                self._bucketize(lower_node.node_id),
                self._bucketize(higher_node.node_id)
        )

    def create(self, request: CreateRequest) -> CreateResponse:
        """Starts a single node Chord ring containing this node.

        Initializes all entries in the successor list to this node.

        :param request: The :class:`CreateRequest` request
        :return: The :class:`CreateResponse` response
        """
        if not self._is_shutdown:
            raise RuntimeError("Node already initialized")

        self._successor_list = [self] * self._config.ring_size
        self._is_shutdown = False
        return CreateResponse()

    def join(self, request: JoinRequest) -> JoinResponse:
        """Joins an existing Chord ring.

        Finds this node's successor `s` in the ring and copies the `s`'s successor list as its own.

        :param request: The :class:`JoinRequest` request
        :return: The :class:`JoinResponse` response
        """
        # TODO: Add check that both nodes are configured with the same ring size before joining
        #       since it is used for bucketing. Can be done by adding ring size to the node id
        if not self._is_shutdown:
            raise RuntimeError("Node already initialized")

        successor = request.remote_node.find_successor(
                FindSuccessorRequest(self._bucketize(self.node_id))
        ).node
        self._successor_list = successor.get_successor_list(
                GetSuccessorListRequest()
        ).successor_list

        self._is_shutdown = False
        return JoinResponse()

    def node(self, request: NodeRequest) -> NodeResponse:
        """Returns this Chord node.

        This is essentially used as a ping operation.

        :param request: The :class:`NodeRequest` request
        :return: The :class:`NodeResponse` response with this :class:`ChordNode`
        """
        return NodeResponse(self, not self._is_shutdown)

    def is_alive(self) -> bool:
        """Returns `True` if this node is alive."""
        return not self._is_shutdown

    def stabilize(self) -> None:
        """
        Checks whether there is a closer successor for this node and tells the successor about
        this node.
        """
        try:
            current_successor = self._get_successor()

            possible_successor = current_successor.get_predecessor(GetPredecessorRequest()).node
            successor_successor_list = current_successor.get_successor_list(
                    GetSuccessorListRequest()
            ).successor_list

            self._successor_list = [current_successor, *successor_successor_list[:-1]]
            if (possible_successor
                    and self._between_nodes(possible_successor, self, current_successor)):
                try:
                    new_successor_list = possible_successor.get_successor_list(
                            GetSuccessorListRequest()
                    ).successor_list
                    self._successor_list = [possible_successor, *new_successor_list[:-1]]
                except NodeFailureException:
                    # possible_successor is dead, no change
                    pass

        except NodeFailureException:
            # Remove dead successor
            self._successor_list = [*self._successor_list[1:], self]

        self._get_successor().notify(NotifyRequest(self))

    def notify(self, request: NotifyRequest) -> NotifyResponse:
        """Notifies a node that a remote node might be its predecessor.

        :param: the :class:`NotifyRequest` request
        :return: The :class:`NotifyResponse` response
        """
        if request.remote_node:
            predecessor = self.get_predecessor(GetPredecessorRequest()).node
            if (predecessor is None
                    or self._between_nodes(request.remote_node, predecessor, self)  # type:ignore
                    or not predecessor.is_alive()):
                self._set_predecessor(request.remote_node)
        else:
            # If remote node is shutdown with a null predecessor, it will notify this node with
            # a None `remote_node`. In this case, set our predecessor to None.
            self._set_predecessor(None)

        return NotifyResponse()

    def fix_fingers(self) -> None:
        """Refreshes the next finger table entry.

        Queries `2^i` buckets from this node in the Chord ring to check for a closer finger node.
        """
        finger_bucket = self._bucketize(self.node_id)
        finger_bucket += 2 ** self._next_finger_to_fix
        finger_bucket %= 2 ** self._config.ring_size

        self._fingers[self._next_finger_to_fix] = self.find_successor(
                FindSuccessorRequest(finger_bucket)
        ).node

        self._next_finger_to_fix = (self._next_finger_to_fix + 1) % self._config.ring_size

    def check_predecessor(self) -> None:
        """Checks that the predecessor node is still alive."""
        predecessor = self.get_predecessor(GetPredecessorRequest()).node
        if predecessor and not predecessor.is_alive():
            self._set_predecessor(None)

    def find_successor(self, request: FindSuccessorRequest) -> FindSuccessorResponse:
        """
        Finds the successor node for a given key or bucket.
        :param request: The :class:`FindSuccessorRequest` request with the key or bucket to look up
        :return: The :class:`FindSuccessorResponse` response with the successor node and number of
                 nodes the lookup traversed
        """
        if isinstance(request.key, int):
            key_bucket = request.key
        else:
            key_bucket = self._bucketize(str(request.key))

        key_bucket %= 2 ** self._config.ring_size

        current_successor = self._get_successor()
        if (self._between(
                key_bucket,
                self._bucketize(self.node_id),
                self._bucketize(current_successor.node_id)
        ) or self._bucketize(current_successor.node_id) == request.key):
            if current_successor.is_alive():
                return FindSuccessorResponse(current_successor, 0)
            return FindSuccessorResponse(self, 0)

        for closest in self.closest_preceding_nodes(key_bucket):
            if closest == self:
                return FindSuccessorResponse(self, 0)

            try:
                response = closest.find_successor(FindSuccessorRequest(key_bucket))
                return FindSuccessorResponse(response.node, response.hops + 1)
            except NodeFailureException:
                pass  # Dead node, try the next one!

        raise RuntimeError("Found no successor, this should never happen!")

    def closest_preceding_nodes(self, target: int) -> Iterator[ChordNode]:
        """Creates a generator which yields nodes from this node's finger table and successor
        list in order from closest to furthest from the target bucket.

        :param bucket: The bucket
        :return: a generator which yields :class:`ChordNode`s
        """
        real_fingers = [f for f in self._fingers if f is not None]  # for mypy
        succeeding_nodes = sorted(
                set(self._successor_list + real_fingers),
                key=lambda node: self._bucketize(node.node_id)
        )

        for successor in iter(reversed(succeeding_nodes)):
            if successor and self._between(
                self._bucketize(successor.node_id),
                self._bucketize(self.node_id),
                target
            ):
                yield successor

        yield self

    def shutdown(self, request: ShutdownRequest) -> ShutdownResponse:
        """Shuts down the node gracefully.

        Finds the first alive successor in the node's successor list, notifies it to take the
        notifying node's predecessor as its predecessor, and shifts the node's keys to the
        successor.

        :param request: the :class:`ShutdownRequest` request
        :return: the :class:`ShutdownResponse` response
        """
        self._is_shutdown = True

        successor_list = self.get_successor_list(GetSuccessorListRequest()).successor_list
        for successor in successor_list:
            try:
                successor.notify(NotifyRequest(self.get_predecessor(GetPredecessorRequest()).node))

                for key in self._storage.list():
                    successor.put(PutKeyRequest(key, self._storage.get(key), True))

                return ShutdownResponse()

            except NodeFailureException:
                pass

        logger.error("Could not find any alive nodes to handoff to!")
        return ShutdownResponse()

    def _get_successor(self) -> ChordNode:
        """Returns this node's first successor."""
        return self._successor_list[0]

    def get_successor_list(self, request: GetSuccessorListRequest) -> GetSuccessorListResponse:
        """Returns this node's successor list.

        :param request: the :class:`GetSuccessorListRequest` request
        :return: the :class:`GetSuccessorListResponse` response containing the successor list of
                 :class:`ChordNode`s
        """
        return GetSuccessorListResponse(self._successor_list)

    # Not using @properties for these since RPC doesn't work well with them
    def get_predecessor(self, request: GetPredecessorRequest) -> GetPredecessorResponse:
        """Returns the predecessor node.

        :param request: the :class:`GetPredecessorRequest` request
        :return: the :class:`GetPredecessorResponse` response
        """
        return GetPredecessorResponse(self._predecessor)

    def _set_predecessor(self, node: Optional[ChordNode]) -> None:
        """Sets this node's predecessor node.

        :param node: the :class:`ChordNode` to set or `None`
        """
        self._predecessor = node

    def get(self, request: GetKeyRequest) -> GetKeyResponse:
        """Recursively looks up the value for a key from the Chord ring.

        :param request: the :class:`GetKeyRequest` request with the key to look up
        :return: the :class:`GetKeyResponse` response with the key-value (if found) and number of
                 nodes traversed.
        """
        response = self.find_successor(FindSuccessorRequest(request.key))

        if response.node == self:
            if not self._storage.has(request.key):
                return GetKeyResponse(self, response.hops, None, False)
            return GetKeyResponse(self, response.hops, self._storage.get(request.key), True)

        return response.node.get(request)

    def put(self, request: PutKeyRequest) -> PutKeyResponse:
        """Recursively stores a key/value pair in the Chord ring.

        :param request: the :class:`PutKeyRequest` request with the key/value pair to store
        :return: the :class:`PutKeyResponse` response with the number of nodes traversed
        """
        response = self.find_successor(FindSuccessorRequest(request.key))

        if response.node == self or request.no_redirect:
            self._storage.put(request.key, request.value)
            return PutKeyResponse(self, response.hops)

        return response.node.put(request)


class RemoteChordNode(ChordNode):
    """Proxy ChordNode to perform remote operations."""

    def __init__(self, transport, node_id: str):
        super().__init__(node_id, NullChordStorage(), ChordConfig())
        self._transport = transport
        self._connection = transport.create_connection(node_id)

    def __repr__(self) -> str:
        return f"{__name__}({self.node_id})"

    def node(self, request: NodeRequest) -> NodeResponse:
        return self._connection.make_request(NODE, request, NodeResponse)

    def is_alive(self) -> bool:
        try:
            response = self.node(NodeRequest())
            return response.is_alive
        except NodeFailureException:
            return False

    def create(self, request: CreateRequest) -> CreateResponse:
        return self._connection.make_request(CREATE, request, CreateResponse)

    def find_successor(self, request: FindSuccessorRequest) -> FindSuccessorResponse:
        return self._connection.make_request(FIND_SUCCESSOR, request, FindSuccessorResponse)

    def join(self, request: JoinRequest) -> JoinResponse:
        return self._connection.make_request(JOIN, request, JoinResponse)

    def notify(self, request: NotifyRequest) -> NotifyResponse:
        return self._connection.make_request(NOTIFY, request, NotifyResponse)

    def get_predecessor(self, request: GetPredecessorRequest) -> GetPredecessorResponse:
        return self._connection.make_request(GET_PREDECESSOR, request, GetPredecessorResponse)

    def _set_predecessor(self, node: Optional[ChordNode]) -> None:
        raise NotImplementedError

    def _get_successor(self) -> ChordNode:
        raise NotImplementedError

    def get_successor_list(self, request: GetSuccessorListRequest) -> GetSuccessorListResponse:
        return self._connection.make_request(GET_SUCCESSOR_LIST, request, GetSuccessorListResponse)

    def shutdown(self, request: ShutdownRequest) -> ShutdownResponse:
        return self._connection.make_request(SHUTDOWN, request, ShutdownResponse)

    def get(self, request: GetKeyRequest) -> GetKeyResponse:
        return self._connection.make_request(GET_KEY, request, GetKeyResponse)

    def put(self, request: PutKeyRequest) -> PutKeyResponse:
        return self._connection.make_request(PUT_KEY, request, PutKeyResponse)

    def stabilize(self) -> None:
        raise NotImplementedError

    def fix_fingers(self) -> None:
        raise NotImplementedError

    def check_predecessor(self) -> None:
        raise NotImplementedError

    def closest_preceding_nodes(self, target: int) -> Iterator[ChordNode]:
        raise NotImplementedError
