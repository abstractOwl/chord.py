import argparse
from math import ceil
from random import choice, choices
from time import sleep
from threading import Thread
from typing import Dict, Tuple, Union
from uuid import uuid4

from chord.exceptions import NodeFailureException
from chord.node import ChordNode, RemoteChordNode


nodes: Dict[str, ChordNode] = {}
ITERATIONS: int = 10000


def main():
    parser = argparse.ArgumentParser(description="Simulates a Chord ring.")
    parser.add_argument("nodes", type=int, help="Number of nodes to simulate.")
    parser.add_argument("ring_size", type=int, help="Chord ring size.")

    args = parser.parse_args()
    ring_size = args.ring_size

    transport_factory = LocalChordTransportFactory()
    joined_list = []

    # Start maintenance thread
    running = True
    def run_maintenance_tasks():
        while running:
            for node_id in joined_list:
                node = nodes[node_id]
                if node.is_alive():
                    node.fix_fingers()
                    node.stabilize()
                    node.check_predecessor()

            sleep(0.1)
    Thread(target=run_maintenance_tasks).start()

    # Init nodes
    print("Initiating node ring...")
    joined_list.append(create_node(transport_factory, ring_size))
    for _ in range(args.nodes - 1):
        node_id = create_node(
                transport_factory,
                ring_size,
                choice(joined_list)
        )
        joined_list.append(node_id)
    print("=> Done")

    # Wait for ring to stabilize
    print("Waiting 30 seconds for ring to stabilize...")
    sleep(30)

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
    for i in range(ITERATIONS):
        _, hops = nodes[choice(joined_list)].find_successor(uuid4().hex)
        hops_list.append(hops)

        # TODO: Enable after successor lists are implemented
        # if choices([True, False], weights=[5, 95]):
        #     # choice(list(nodes.values())).shutdown()
        #     node_id = choice(joined_list)
        #     joined_list.remove(node_id)
        #     nodes[node_id] = DeadChordNode(transport_factory, node_id)

        if choices([True, False], weights=[5, 95]):
            joined_list.append(
                    create_node(
                        transport_factory,
                        ring_size,
                        choice(list(nodes.keys()))
                    )
            )

        if i % 1000 == 0:
            print("=> Completed %d lookups so far" % i)

    print_stats(hops_list)


    running = False


class LocalChordTransport:
    def __init__(self, transport_factory, node_id):
        self.node_id = node_id
        self._transport_factory = transport_factory

    def node(self) -> RemoteChordNode:
        node = nodes[self.node_id].node()
        node_id = node.node_id if node is not None else None
        return RemoteChordNode(self._transport_factory, node_id)

    def create(self):
        nodes[self.node_id].create()

    def find_successor(self, key: int) -> Tuple[str, int]:
        node, hops = nodes[self.node_id].find_successor(key)
        node_id = node.node_id if node is not None else None
        return node_id, hops

    def join(self, remote_node: "ChordNode"):
        nodes[self.node_id].join(remote_node)

    def notify(self, remote_node: "ChordNode"):
        nodes[self.node_id].notify(remote_node)

    def predecessor(self) -> Dict:
        node = nodes[self.node_id].predecessor
        node_id = node.node_id if node is not None else None
        return node_id

    def shutdown(self) -> Dict:
        result = nodes[self.node_id].shutdown()
        nodes[self.node_id] = DeadChordNode(
                self._transport_factory,
                self.node_id
        )
        return result

    def get(self, key: str) -> Dict[str, str]:
        return nodes[self.node_id].get(key)

    def put(self, key: str, value: str, no_redirect: bool=False):
        return nodes[self.node_id].put(key, value, no_redirect)


class LocalChordTransportFactory:
    def new_transport(self, node_id):
        return LocalChordTransport(self, node_id)


class DeadChordNode(RemoteChordNode):
    def __init__(self, transport_factory, node_id):
        super().__init__(transport_factory, node_id)
        self.node_id = node_id

    def node(self):
        raise NodeFailureException()

    def is_alive(self) -> bool:
        return False

    def create(self):
        raise NodeFailureException()

    def find_successor(self, key: Union[int, str]) -> Tuple[ChordNode, int]:
        raise NodeFailureException()

    def join(self, remote_node: "ChordNode"):
        raise NodeFailureException()

    def notify(self, remote_node: "ChordNode"):
        raise NodeFailureException()

    @property
    def predecessor(self) -> "ChordNode":
        raise NodeFailureException()

    @predecessor.setter
    def predecessor(self, value):
        raise NotImplementedError

    def shutdown(self) -> Dict:
        raise NotImplementedError

    def get(self, key: str) -> Dict[str, str]:
        raise NotImplementedError

    def put(self, key: str, value: str, no_redirect: bool=False):
        raise NotImplementedError

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
        raise NodeFailureException()

    @successor.setter
    def successor(self, value):
        raise NotImplementedError


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


def create_node(transport_factory, ring_size, join_node_id=None):
    node_id = uuid4().hex
    node = ChordNode(node_id, None, ring_size)
    nodes[node_id] = node

    if join_node_id:
        node.join(RemoteChordNode(transport_factory, join_node_id))
    else:
        nodes[node_id].create()

    return node_id


if __name__ == '__main__':
    main()
