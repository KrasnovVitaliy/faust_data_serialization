from faust.serializers import codecs
from typing import Any


class ProtobufSerializer(codecs.Codec):
    def __init__(self, pb_type: Any):
        self.pb_type = pb_type
        super(self.__class__, self).__init__()

    def _dumps(self, pb: Any) -> bytes:
        return pb.SerializeToString()

    def _loads(self, s: bytes) -> Any:
        pb = self.pb_type()
        pb.ParseFromString(s)
        return pb
