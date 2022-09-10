from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, TYPE_CHECKING, Union

import chord

if TYPE_CHECKING:
    import chord.node


@dataclass
class BaseRequest:
    pass

@dataclass
class BaseResponse:
    pass

@dataclass
class NodeRequest(BaseRequest):
    pass

@dataclass
class NodeResponse(BaseResponse):
    node: chord.node.ChordNode

@dataclass
class CreateRequest(BaseRequest):
    pass

@dataclass
class CreateResponse(BaseResponse):
    pass

@dataclass
class FindSuccessorRequest(BaseRequest):
    key: Union[int, str]

@dataclass
class FindSuccessorResponse(BaseResponse):
    node: chord.node.ChordNode
    hops: int

@dataclass
class JoinRequest(BaseRequest):
    remote_node: chord.node.ChordNode

@dataclass
class JoinResponse(BaseResponse):
    pass

@dataclass
class NotifyRequest(BaseRequest):
    remote_node: Optional[chord.node.ChordNode]

@dataclass
class NotifyResponse(BaseResponse):
    pass

@dataclass
class GetPredecessorRequest(BaseRequest):
    pass

@dataclass
class GetPredecessorResponse(BaseResponse):
    node: Optional[chord.node.ChordNode]

@dataclass
class GetSuccessorListRequest(BaseRequest):
    pass

@dataclass
class GetSuccessorListResponse(BaseResponse):
    successor_list: list[chord.node.ChordNode]

@dataclass
class ShutdownRequest(BaseRequest):
    pass

@dataclass
class ShutdownResponse(BaseResponse):
    pass

@dataclass
class GetKeyRequest(BaseRequest):
    key: str

@dataclass
class GetKeyResponse(BaseResponse):
    storage_node: chord.node.ChordNode
    hops: int
    value: Optional[str]
    found: bool

@dataclass
class PutKeyRequest(BaseRequest):
    key: str
    value: str
    no_redirect: bool = False

@dataclass
class PutKeyResponse(BaseResponse):
    storage_node: chord.node.ChordNode
    hops: int
