from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, TYPE_CHECKING, Union

import chord

if TYPE_CHECKING:
    import chord.node


@dataclass
class BaseRequest:
    """Request super-class."""

@dataclass
class BaseResponse:
    """Response super-class."""

@dataclass
class NodeRequest(BaseRequest):
    """Request for Node operation."""

@dataclass
class NodeResponse(BaseResponse):
    """Response for Node operation."""
    node: chord.node.ChordNode
    is_alive: bool

@dataclass
class CreateRequest(BaseRequest):
    """Request for Create operation."""

@dataclass
class CreateResponse(BaseResponse):
    """Response for Create operation."""

@dataclass
class FindSuccessorRequest(BaseRequest):
    """Request for FindSuccessorRequest operation."""
    key: Union[int, str]

@dataclass
class FindSuccessorResponse(BaseResponse):
    """Response for FindSuccessorRequest operation."""
    node: chord.node.ChordNode
    hops: int

@dataclass
class JoinRequest(BaseRequest):
    """Request for Join operation."""
    remote_node: chord.node.ChordNode

@dataclass
class JoinResponse(BaseResponse):
    """Response for Join operation."""

@dataclass
class NotifyRequest(BaseRequest):
    """Request for Notify operation."""
    remote_node: Optional[chord.node.ChordNode]

@dataclass
class NotifyResponse(BaseResponse):
    """Response for Notify operation."""

@dataclass
class GetPredecessorRequest(BaseRequest):
    """Request for GetPredecessor operation."""

@dataclass
class GetPredecessorResponse(BaseResponse):
    """Response for GetPredecessor operation."""
    node: Optional[chord.node.ChordNode]

@dataclass
class GetSuccessorListRequest(BaseRequest):
    """Request for GetSuccessorList operation."""

@dataclass
class GetSuccessorListResponse(BaseResponse):
    """Response for GetSuccessorList operation."""
    successor_list: list[chord.node.ChordNode]

@dataclass
class ShutdownRequest(BaseRequest):
    """Request for Shutdown operation."""

@dataclass
class ShutdownResponse(BaseResponse):
    """Response for Shutdown operation."""

@dataclass
class GetKeyRequest(BaseRequest):
    """Request for GetKey operation."""
    key: str

@dataclass
class GetKeyResponse(BaseResponse):
    """Response for GetKey operation."""
    storage_node: chord.node.ChordNode
    hops: int
    value: Optional[str]
    found: bool

@dataclass
class PutKeyRequest(BaseRequest):
    """Request for PutKey operation."""
    key: str
    value: str
    no_redirect: bool = False

@dataclass
class PutKeyResponse(BaseResponse):
    """Response for PutKey operation."""
    storage_node: chord.node.ChordNode
    hops: int
