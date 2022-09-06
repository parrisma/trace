import uuid


class UniqueRef:
    def __init__(self):
        # UUID type 4 is the 'most-unique'
        self._unique_ref = str(uuid.uuid4()).replace('-', '')

    @property
    def ref(self) -> str:
        return self._unique_ref

    def __str__(self):
        return self._unique_ref

    def __repr__(self):
        return str(self)
