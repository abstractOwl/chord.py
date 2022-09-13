import abc
from typing import Type

from chord.model import BaseRequest, BaseResponse


class ChordConnection(abc.ABC):
    """Represents a connection to a remote node."""
    def make_request(
            self, command: str, request: BaseRequest, response_cls: Type[BaseResponse]
    ) -> BaseResponse:
        """Sends a request to the remote node.

        :param command: the command to send
        :param request: the request object
        :param response_cls: the expected response class
        :return: the response object
        """

class ChordTransport(abc.ABC):
    """Facilitates Chord RPC over a defined protocol."""
    def create_connection(self, node_id: str) -> ChordConnection:
        """Creates a connection to a remote node.

        :param node_id: the node address of the remote node
        :return: a :class:`ChordConnection` to the remote node
        """
