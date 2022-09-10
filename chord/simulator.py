import json
from math import ceil
from random import choice, choices
from time import sleep
from threading import Thread
from typing import Optional, Type
from uuid import uuid4

import click

from chord.exceptions import NodeFailureException
from chord.marshal import JsonChordMarshaller, JsonChordUnmarshaller
from chord.model import (
        BaseRequest, BaseResponse, NodeRequest, NodeResponse, CreateRequest, CreateResponse,
        FindSuccessorRequest, FindSuccessorResponse, JoinRequest, JoinResponse, NotifyRequest,
        NotifyResponse, GetPredecessorRequest, GetPredecessorResponse, GetSuccessorListRequest,
        GetSuccessorListResponse, ShutdownRequest, ShutdownResponse, GetKeyRequest, PutKeyRequest
)
from chord.node import ChordNode, RemoteChordNode
from chord.storage import DictChordStorage


nodes: dict[str, Optional[ChordNode]] = {}
ITERATIONS: int = 1000
joined_list: list[str] = []


@click.command()
@click.option("--num-nodes", type=int, required=True, help="Number of nodes to simulate")
@click.option("--ring-size", type=int, required=True, help="Chord ring size")
def main(num_nodes: int, ring_size: int):
    transport = LocalChordTransport()

    # Start maintenance thread
    def run_maintenance_tasks():
        while True:
            for node_id in joined_list:
                node = nodes[node_id]._node
                if node.get_successor_list(GetSuccessorListRequest()).successor_list:
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
    create_node(transport, ring_size)
    for _ in range(num_nodes - 1):
        create_node(
                transport,
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
        response = RemoteChordNode(transport, choice(joined_list)).find_successor(
                FindSuccessorRequest(uuid4().hex)
        )
        hops_list.append(response.hops)

        if i % 100 == 0:
            print("=> Completed %d lookups so far" % i)
    print_stats(hops_list)

    print()
    input("Press enter to continue.")
    print()

    # Make random find_successor calls, while adding/killing nodes randomly,
    # recording hops
    print("Running simulation with random joins and failures")
    hops_list = []
    failed = 0
    for i in range(ITERATIONS):
        try:
            response = RemoteChordNode(transport, choice(joined_list)).find_successor(
                    FindSuccessorRequest(uuid4().hex)
            )
            hops_list.append(response.hops)
        except NodeFailureException:
            failed += 1

        if choices([True, False], weights=[5, 95])[0]:
            node_id = choice(joined_list)
            print("Shutting down ", node_id)
            RemoteChordNode(transport, node_id).shutdown(ShutdownRequest())

        if choices([True, False], weights=[5, 95])[0]:
            create_node(
                transport,
                ring_size,
                choice(joined_list)
            )

        sleep(0.05)

        if i % 100 == 0:
            print("=> Completed %d lookups so far" % i)

    print_stats(hops_list)
    print("Failed calls", failed)


class LocalChordConnection:
    def __init__(self, transport, node_id):
        self.node_id = node_id
        self._transport = transport
        self.marshaller = JsonChordMarshaller()
        self.unmarshaller = JsonChordUnmarshaller(transport)

    def _get_node(self):
        if (self.node_id in joined_list
                and nodes[self.node_id]
                and nodes[self.node_id]._node.is_alive()):
            return nodes[self.node_id]
        raise NodeFailureException(self.node_id)

    def make_request(
            self, command: str, request: BaseRequest, response_cls: Type[BaseResponse]
    ) -> dict:
        client = self._get_node()
        command_fn = getattr(client, command)

        params = self.marshaller.marshal(request)
        obj = command_fn(json.loads(params))
        return self.unmarshaller.unmarshal(obj, response_cls)


class LocalChordTransport:
    def create_connection(self, node_id: str):
        return LocalChordConnection(self, node_id)


class LocalChordHandler:
    def __init__(self, node_id, transport, storage, ring_size):
        self._node_id = node_id
        self._node: ChordNode = ChordNode(node_id, storage, ring_size)
        self._marshaller = JsonChordMarshaller()
        self._unmarshaller = JsonChordUnmarshaller(transport)

    def create(self, payload: dict) -> CreateResponse:
        request = self._unmarshaller.unmarshal(payload, CreateRequest)
        return json.loads(self._marshaller.marshal(self._node.create(request)))

    def node(self, payload: dict) -> NodeResponse:
        request = self._unmarshaller.unmarshal(payload, NodeRequest)
        return json.loads(self._marshaller.marshal(self._node.node(request)))

    def join(self, payload: dict) -> JoinResponse:
        request = self._unmarshaller.unmarshal(payload, JoinRequest)
        return json.loads(self._marshaller.marshal(self._node.join(request)))

    def notify(self, payload: dict) -> NotifyResponse:
        request = self._unmarshaller.unmarshal(payload, NotifyRequest)
        return json.loads(self._marshaller.marshal(self._node.notify(request)))

    def find_successor(self, payload: dict) -> FindSuccessorResponse:
        request = self._unmarshaller.unmarshal(payload, FindSuccessorRequest)
        return json.loads(self._marshaller.marshal(self._node.find_successor(request)))

    def get_predecessor(self, payload: dict) -> GetPredecessorResponse:
        request = self._unmarshaller.unmarshal(payload, GetPredecessorRequest)
        return json.loads(self._marshaller.marshal(self._node.get_predecessor(request)))

    def get_successor_list(self, payload: dict) -> GetSuccessorListResponse:
        request = self._unmarshaller.unmarshal(payload, GetSuccessorListRequest)
        return json.loads(self._marshaller.marshal(self._node.get_successor_list(request)))

    def shutdown(self, payload: dict) -> ShutdownResponse:
        try:
            request = self._unmarshaller.unmarshal(payload, ShutdownRequest)
            return json.loads(self._marshaller.marshal(self._node.shutdown(request)))
        finally:
            del nodes[self._node_id]
            joined_list.remove(self._node_id)

    def get(self, payload: dict) -> GetKeyRequest:
        request = self._unmarshaller.unmarshal(payload, GetKeyRequest)
        return json.loads(self._marshaller.marshal(self._node.get(request)))

    def put(self, payload: dict) -> PutKeyRequest:
        request = self._unmarshaller.unmarshal(payload, PutKeyRequest)
        return json.loads(self._marshaller.marshal(self._node.put(request)))


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
    print("p100", percentile(100, sorted_in_list))


def create_node(transport, ring_size, join_node_id=None):
    node_id = uuid4().hex
    node = LocalChordHandler(node_id, transport, DictChordStorage(), ring_size)
    nodes[node_id] = node
    joined_list.append(node_id)
    print("Added %s" % node_id)

    if join_node_id:
        RemoteChordNode(transport, node_id).join(
                JoinRequest(RemoteChordNode(transport, join_node_id))
        )
    else:
        RemoteChordNode(transport, node_id).create(CreateRequest())

    return node_id


if __name__ == '__main__':
    main()
