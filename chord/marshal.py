import json
from typing import Any, get_args, get_origin, get_type_hints, List, Optional, Type, Union

from chord.model import BaseRequest, BaseResponse
from chord.node import ChordNode, RemoteChordNode


class JsonChordMarshaller:
    def marshal(self, request: Union[BaseRequest, BaseResponse]):
        return json.dumps(request, cls=JsonChordEncoder)


class JsonChordEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, BaseRequest) or isinstance(obj, BaseResponse):
            return obj.__dict__
        elif isinstance(obj, ChordNode):
            return obj.node_id
        return super().default(obj)


class UnmarshalError(BaseException):
    pass


class JsonChordUnmarshaller:
    def __init__(self, transport_factory):
        self._transport_factory = transport_factory

    @staticmethod
    def _is_optional(cls):
        return (
                get_origin(cls) is Union
                and len(get_args(cls)) == 2
                and get_args(cls)[1] == type(None)
        )

    def _parse_value(self, value: Any, cls: Type):
        if cls == ChordNode:
            value = RemoteChordNode(self._transport_factory, value)
        elif self._is_optional(cls):
            if value is not None:
                value = self._parse_value(value, get_args(cls)[0])
        elif get_origin(cls) is list:
            value = [self._parse_value(v, get_args(cls)[0]) for v in value]
        return value

    def unmarshal(self, response: dict, cls: Union[Type[BaseRequest], Type[BaseResponse]]):
        hints = get_type_hints(cls)

        # Check that all params are populated
        if set(hints.keys()) != set(response.keys()):
            raise UnmarshalError("Invalid parameters for response type {}".format(cls.__name__))

        # Pass arguments to Request/Response class constructor
        params = []
        for param, hint_cls in hints.items():
            value = response[param]
            params.append(self._parse_value(response[param], hint_cls))

        return cls(*params)
