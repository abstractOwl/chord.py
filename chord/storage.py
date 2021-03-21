class DictChordStorage:
    def __init__(self):
        self.storage = {}

    def get(self, key):
        return self.storage[key]

    def put(self, key, value):
        self.storage[key] = value
