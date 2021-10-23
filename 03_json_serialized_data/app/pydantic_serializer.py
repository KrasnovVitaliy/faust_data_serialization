from faust.serializers import codecs
from typing import Any


class PydanticSerializer(codecs.Codec):
    def __init__(self, cls_type: Any):
        self.cls_type = cls_type
        super(self.__class__, self).__init__()

    def _dumps(self, cls: Any) -> bytes:
        return cls.json().encode()

    def _loads(self, s: bytes) -> Any:
        cls_impl = self.cls_type.parse_raw(s)
        return cls_impl
