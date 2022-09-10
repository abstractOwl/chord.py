from __future__ import annotations
from typing import Type

import requests

from chord.exceptions import NodeFailureException
from chord.marshal import (
        ChordMarshaller, ChordUnmarshaller, JsonChordMarshaller, JsonChordUnmarshaller
)
from chord.model import BaseRequest, BaseResponse
from chord.transport import ChordConnection, ChordTransport


class HttpChordConnection(ChordConnection):
    """
    HTTP connection implementation for Chord. node_id is {hostname}:{port}.
    """
    def __init__(
            self,
            marshaller: ChordMarshaller,
            unmarshaller: ChordUnmarshaller,
            session: requests.Session,
            node_id: str
    ):
        self._marshaller = marshaller
        self._unmarshaller = unmarshaller
        self._node_id = node_id
        self._session = session

    def make_request(
            self, command: str, request: BaseRequest, response_cls: Type[BaseResponse]
    ) -> BaseResponse:
        try:
            params = self._marshaller.marshal(request)
            response = self._session.post(
                    f"http://{self._node_id}/{command}",
                    headers={"Content-Type": "application/json"},
                    data=params,
            ).json()
            return self._unmarshaller.unmarshal(response, response_cls)

        except requests.exceptions.RequestException as ex:
            raise NodeFailureException(f"Failed: {self._node_id}/{command}") from ex


class HttpChordTransport(ChordTransport):
    def __init__(self, ring_size: int):
        self._marshaller = JsonChordMarshaller()
        self._unmarshaller = JsonChordUnmarshaller(self)
        self._ring_size = ring_size

        self._session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
                max_retries=3,
                pool_connections=self._ring_size + 5,
        )
        self._session.mount("http://", adapter)

    def create_connection(self, node_id: str) -> ChordConnection:
        return HttpChordConnection(self._marshaller, self._unmarshaller, self._session, node_id)
