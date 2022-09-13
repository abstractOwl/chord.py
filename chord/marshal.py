from __future__ import annotations
import abc
import json
from typing import (
        Any, get_args, get_origin, get_type_hints, Type, TypeVar, TYPE_CHECKING, Union
)

from chord.model import BaseRequest, BaseResponse
from chord.node import ChordNode, RemoteChordNode

if TYPE_CHECKING:
    from chord.transport import ChordTransport


RequestResponse = TypeVar("RequestResponse")


class UnmarshalError(ValueError):
    """Failed to unmarshal the payload into the specified class."""


class ChordMarshaller(abc.ABC):
    """Abstract class that handles marshalling requests and responses to and from the expected
    payload format.
    """
    def marshal(self, obj: Union[BaseRequest, BaseResponse]) -> str:
        """Marshals a request or response into a string payload.

        :param obj: a sub-class of :class:`BaseRequest` or :class:`BaseResponse` to marshal
        :return: a `str` payload
        """

    def unmarshal(self, payload: dict, cls: Type[RequestResponse]) -> RequestResponse:
        """Unmarshals a :class:`dict` payload into the expected request/response object.

        :param payload: the :class:`dict` payload
        :param cls: the :class:`BaseRequest` or :class:`BaseResponse` sub-class to unmarshal the
                    payload into
        :return: an instance of the `cls`
        """


class JsonChordMarshaller(ChordMarshaller):
    """Marshals requests/responses to and from JSON."""
    def __init__(self, transport: ChordTransport):
        self._transport = transport

    def marshal(self, obj: Union[BaseRequest, BaseResponse]):
        return json.dumps(obj, cls=JsonChordEncoder)

    @staticmethod
    def _is_optional(value_cls) -> bool:
        """Returns `True` if a type is an :class:`Optional`."""
        return (
                get_origin(value_cls) is Union
                and len(get_args(value_cls)) == 2
                and get_args(value_cls)[1] == type(None)
        )

    def _parse_value(self, value: Any, cls: Type) -> Any:
        """Recursively unpacks and parses :class:`ChordNode` fields."""
        if cls == ChordNode:
            value = RemoteChordNode(self._transport, value)
        elif self._is_optional(cls):
            if value is not None:
                value = self._parse_value(value, get_args(cls)[0])
        elif get_origin(cls) is list:
            value = [self._parse_value(v, get_args(cls)[0]) for v in value]
        return value

    def unmarshal(self, payload: dict, cls: Type[RequestResponse]) -> RequestResponse:
        hints = get_type_hints(cls)

        # Check that all params are populated
        if set(hints.keys()) != set(payload.keys()):
            raise UnmarshalError("Invalid parameters for type {}".format(cls.__name__))

        # Pass arguments to Request/Response class constructor
        params = [self._parse_value(payload[param], hint_cls) for param, hint_cls in hints.items()]
        return cls(*params)


class JsonChordEncoder(json.JSONEncoder):
    """An extended :class:`JSONEncoder` which converts :class:`ChordNode`s to a `str` node id."""
    def default(self, o):
        if isinstance(o, (BaseRequest, BaseResponse)):
            return o.__dict__
        if isinstance(o, ChordNode):
            return o.node_id
        return super().default(o)
