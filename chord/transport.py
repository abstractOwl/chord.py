import abc
from typing import Type

from chord.model import BaseRequest, BaseResponse


class ChordConnection(abc.ABC):
    def make_request(
            self, command: str, request: BaseRequest, response_cls: Type[BaseResponse]
    ) -> BaseResponse:
        pass

class ChordTransport(abc.ABC):
    def create_connection(self, node_id: str) -> ChordConnection:
        pass
