import argparse
from math import ceil
from random import choice, choices
from time import sleep
from threading import Thread
from typing import Dict, List, Optional, Tuple
from uuid import uuid4

from chord.exceptions import NodeFailureException
from chord.node import ChordNode, RemoteChordNode
from chord.storage import DictChordStorage


nodes: Dict[str, Optional[ChordNode]] = {}
ITERATIONS: int = 10000
joined_list: List[str] = []


def main():
    parser = argparse.ArgumentParser(description="Simulates a Chord ring.")
    parser.add_argument("nodes", type=int, help="Number of nodes to simulate.")
    parser.add_argument("successor_list_size", type=int, help="Successor list size")
    parser.add_argument("ring_size", type=int, help="Chord ring size.")

    args = parser.parse_args()
    successor_list_size = args.successor_list_size
    ring_size = args.ring_size

    transport_factory = LocalChordTransportFactory()

    # Start maintenance thread
    def run_maintenance_tasks():
        while True:
            for node_id in joined_list:
                node = nodes[node_id]
                if node.get_successor_list():
                    try:
                        node.fix_fingers()
                        node.stabilize()
                        node.check_predecessor()
                    except NodeFailureException:
                        pass

            sleep(0.01)
    Thread(target=run_maintenance_tasks, daemon=True).start()

    # Init nodes
    print("Initiating node ring...")
    create_node(transport_factory, successor_list_size, ring_size)
    for _ in range(args.nodes - 1):
        create_node(
                transport_factory,
                successor_list_size,
                ring_size,
                choice(joined_list)
        )
    print("=> Done")

    # Wait for ring to stabilize
    print("Waiting 10 seconds for ring to stabilize...")
    sleep(10)

    # Make random find_successor calls, recording hops
    print("Running first simulation")
    hops_list = []
    for i in range(ITERATIONS):
        _, hops = nodes[choice(joined_list)].find_successor(uuid4().hex)
        hops_list.append(hops)

        if i % 1000 == 0:
            print("=> Completed %d lookups so far" % i)
    print_stats(hops_list)

    # Make random find_successor calls, while adding/killing nodes randomly,
    # recording hops
    print("Running simulation with random joins and failures")
    hops_list = []
    failed = 0
    for i in range(ITERATIONS):
        try:
            _, hops = nodes[choice(joined_list)].find_successor(uuid4().hex)
            hops_list.append(hops)
        except NodeFailureException:
            failed += 1

        # TODO Debug node shutdown
        #if choices([True, False], weights=[1, 999]):
        #    node_id = choice(joined_list)
        #    print("Shutting down ", node_id)
        #    nodes[node_id].shutdown()
        #    sleep(0.05)

        if choices([True, False], weights=[5, 95]):
            create_node(
                transport_factory,
                successor_list_size,
                ring_size,
                choice(joined_list)
            )

        if i % 1000 == 0:
            print("=> Completed %d lookups so far" % i)

    print_stats(hops_list)
    print("Failed calls", failed)


class LocalChordTransport:
    def __init__(self, transport_factory, node_id):
        self.node_id = node_id
        self._transport_factory = transport_factory

    def _get_node(self):
        if self.node_id in joined_list and nodes[self.node_id]:
            return nodes[self.node_id]
        raise NodeFailureException(self.node_id)

    def node(self) -> RemoteChordNode:
        node = self._get_node().node()
        node_id = node.node_id if node is not None else None
        return RemoteChordNode(self._transport_factory, node_id)

    def create(self):
        self._get_node().create()

    def find_successor(self, key: int) -> Tuple[str, int]:
        node, hops = self._get_node().find_successor(key)
        node_id = node.node_id if node is not None else None
        return node_id, hops

    def join(self, remote_node: "ChordNode"):
        self._get_node().join(remote_node)

    def notify(self, remote_node: "ChordNode"):
        self._get_node().notify(remote_node)

    def get_predecessor(self) -> Dict:
        node = self._get_node().get_predecessor()
        node_id = node.node_id if node is not None else None
        return node_id

    def get_successor_list(self) -> List["ChordNode"]:
        return [node.node_id for node in self._get_node().get_successor_list()]

    def shutdown(self) -> Dict:
        result = self._get_node().shutdown()
        nodes[self.node_id] = None
        joined_list.remove(self.node_id)
        return result

    def get(self, key: str) -> Dict[str, str]:
        return self._get_node().get(key)

    def put(self, key: str, value: str, no_redirect: bool=False):
        return self._get_node().put(key, value, no_redirect)


class LocalChordTransportFactory:
    def new_transport(self, node_id: str):
        return LocalChordTransport(self, node_id)


def avg(in_list):
    return sum(in_list) / len(in_list)


def percentile(pct, in_list):
    return in_list[ceil(len(in_list) * pct / 100) - 1]


def print_stats(in_list):
    sorted_in_list = sorted(in_list)
    print("avg",   avg(sorted_in_list))
    print("p50",   percentile(50, sorted_in_list))
    print("p90",   percentile(90, sorted_in_list))
    print("p99",   percentile(99, sorted_in_list))
    print("p99.9", percentile(99.9, sorted_in_list))


def create_node(transport_factory, successor_list_size, ring_size, join_node_id=None):
    node_id = uuid4().hex
    node = ChordNode(node_id, DictChordStorage(), successor_list_size, ring_size)
    nodes[node_id] = node
    joined_list.append(node_id)

    if join_node_id:
        node.join(RemoteChordNode(transport_factory, join_node_id))
    else:
        nodes[node_id].create()

    return node_id


if __name__ == '__main__':
    main()
