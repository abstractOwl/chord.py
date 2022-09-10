import abc


class ChordStorage(abc.ABC):
    def get(self, key: str) -> str:
        pass

    def has(self, key: str) -> bool:
        pass

    def list(self) -> list[str]:
        pass

    def put(self, key: str, value: str) -> None:
        pass


class NullChordStorage(ChordStorage):
    def __init__(self):
        pass

    def get(self, key: str) -> str:
        pass

    def has(self, key: str) -> bool:
        pass

    def list(self) -> list[str]:
        pass

    def put(self, key: str, value: str) -> None:
        pass


class DictChordStorage(ChordStorage):
    def __init__(self):
        self.storage = {}

    def get(self, key: str) -> str:
        return self.storage[key]

    def has(self, key: str) -> bool:
        return key in self.storage

    def list(self) -> list[str]:
        return list(self.storage.keys())

    def put(self, key: str, value: str) -> None:
        self.storage[key] = value
