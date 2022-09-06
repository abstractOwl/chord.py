from __future__ import annotations
import json
from typing import Type

import requests

from chord.exceptions import NodeFailureException
from chord.marshal import JsonChordMarshaller, JsonChordUnmarshaller
from chord.model import BaseRequest, BaseResponse
from chord.node import ChordNode


class HttpChordTransport:
    """
    HTTP transport implementation for Chord. node_id is {hostname}:{port}.
    """
    def __init__(self, transport_factory: HttpChordTransportFactory, session: requests.Session, node_id: str):
        self.node_id = node_id
        self.marshaller = JsonChordMarshaller()
        self.session = session
        self.unmarshaller = JsonChordUnmarshaller(transport_factory)

    def make_request(self, command: str, request: BaseRequest, response_cls: Type[BaseResponse]):
        try:
            params = self.marshaller.marshal(request)
            json = self.session.post(
                    f"http://{self.node_id}/{command}",
                    headers={"Content-Type": "application/json"},
                    data=params,
            ).json()
            return self.unmarshaller.unmarshal(json, response_cls)

        except requests.exceptions.RequestException as ex:
            raise NodeFailureException(f"Failed: {self.node_id}/{command}") from ex


class HttpChordTransportFactory:
    def __init__(self, successor_list_size):
        self.successor_list_size = successor_list_size

    def new_transport(self, node_id: str):
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
                max_retries=3,
                pool_connections=self.successor_list_size + 5,
        )
        session.mount("http://", adapter)
        return HttpChordTransport(self, session, node_id)
