# TODO: Re-consider whether a marshalling layer is needed.
from typing import Any, Dict, Optional
from chord.node import ChordNode


def _marshal_node(node: ChordNode):
    return node.node_id if node is not None else None


def marshal(node: ChordNode) -> Dict[str, Any]:
    """ Returns a dict representation of a Chord node. """
    if node is None:
        return {}
    return {
        "node_id": node.node_id,
        "fingers": [_marshal_node(finger) for finger in node.fingers],
        "repr": repr(node),
        "ring_size": node.ring_size
    }


def unmarshal(node: Dict[str, str]) -> Optional[str]:
    """ Returns the node_id for instantiating a RemoteChordNode. """
    # Can't actually instantiate the RemoteChordNode since it creates a circular import dependency
    if node is None or "node_id" not in node:
        return None
    return node["node_id"]
