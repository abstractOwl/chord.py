import abc


class ChordStorage(abc.ABC):
    """Interface for a Chord node to store and retrieve data locally."""
    def get(self, key: str) -> str:
        """Retrieve the stored value for a key.

        :param key: the `str` key
        :return: the stored `str` value for the key
        """

    def has(self, key: str) -> bool:
        """Checks if a value is stored for the specified key.

        :param key: the `str` key to check
        :return: `True` if the key is stored
        """

    def list(self) -> list[str]:
        """Returns a list of keys stored locally.

        :return: a :class:`list` of `str` keys
        """

    def put(self, key: str, value: str) -> None:
        """Stores a key and value.

        :param key: the `str` key to store
        :param value: the `str` value to store
        """


class NullChordStorage(ChordStorage):
    """A no-op :class:`ChordStorage` implementation."""
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
    """A :class:`ChordStorage` implementation that stores values in a :class:`dict`."""
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
