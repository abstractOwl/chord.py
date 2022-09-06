from typing import List

class DictChordStorage:
    def __init__(self):
        self.storage = {}

    def get(self, key: str) -> str:
        return self.storage[key]

    def has(self, key: str) -> bool:
        return key in self.storage

    def list(self) -> List[str]:
        return list(self.storage.keys())

    def put(self, key: str, value: str):
        self.storage[key] = value
