from typing import Dict


def marshal(node: "ChordNode") -> str:
    """ Returns a dict representation of a Chord node. """
    if node is None:
        return {}
    return {
        "node_id": node.node_id,
        "ring_size": node.ring_size
    }


def unmarshal(node: Dict[str, str]) -> str:
    """ Returns args to creates a RemoteChordNode. """
    # Can't actually instantiate the RemoteChordNode since it creates a circular import dependency
    if node is None or "node_id" not in node:
        return None
    return node["node_id"]
