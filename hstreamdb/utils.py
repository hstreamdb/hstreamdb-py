import hashlib
import logging
from typing import Optional, List, Iterator
import HStream.Server.HStreamApi_pb2 as ApiPb
from google.protobuf.struct_pb2 import Struct
from google.protobuf import json_format, message

from hstreamdb.types import (
    Shard,
    Record,
    record_id_from,
    RecordHeader,
    TimeStamp,
)

logger = logging.getLogger(__name__)


def cons_record(payload, key):
    if isinstance(payload, bytes):
        return ApiPb.HStreamRecord(
            header=ApiPb.HStreamRecordHeader(
                flag=ApiPb.HStreamRecordHeader.Flag.RAW,
                attributes=None,
                key=key,
            ),
            payload=payload,
        )
    elif isinstance(payload, dict):
        payload_struct = Struct()
        payload_struct.update(payload)
        return ApiPb.HStreamRecord(
            header=ApiPb.HStreamRecordHeader(
                flag=ApiPb.HStreamRecordHeader.Flag.JSON,
                attributes=None,
                key=key,
            ),
            payload=payload_struct.SerializeToString(),
        )
    elif isinstance(payload, str):
        return cons_record(payload.encode("utf-8"), key)
    else:
        raise ValueError("Invalid payload type!")


def parse_recived_records(rs: List[ApiPb.ReceivedRecord]) -> Iterator[Record]:
    for r in rs:
        record_id = record_id_from(r.recordId)

        hstream_record = ApiPb.HStreamRecord()
        hstream_record.ParseFromString(r.record)

        record_header = RecordHeader(
            publish_time=TimeStamp(
                seconds=hstream_record.header.publish_time.seconds,
                nanos=hstream_record.header.publish_time.nanos,
            ),
            key=(
                hstream_record.header.key if hstream_record.header.key else None
            ),
            attributes=hstream_record.header.attributes,
        )

        record_type = hstream_record.header.flag
        record_payload = None
        if record_type == ApiPb.HStreamRecordHeader.Flag.RAW:
            record_payload = hstream_record.payload
        elif record_type == ApiPb.HStreamRecordHeader.Flag.JSON:
            try:
                payload_struct = Struct()
                payload_struct.ParseFromString(hstream_record.payload)
                record_payload = json_format.MessageToDict(payload_struct)
            except message.DecodeError:
                logger.error("Can not decode this payload!")
        else:
            raise NotImplementedError("Unsupported record type!")

        if record_payload:
            yield Record(
                id=record_id, header=record_header, payload=record_payload
            )


# TODO: cache
#
# class find_shard_id:
#   def __call__(): ...
def find_shard_id(shards: List[Shard], key: Optional[str] = None) -> int:
    bs_key = (key or "").encode("utf-8")
    key_hash = int(hashlib.md5(bs_key).hexdigest(), 16)
    for s in shards:
        if key_hash >= s.start and key_hash <= s.end:
            return s.id

    raise KeyError(f"Impossible happened! No such shard for key {key}")
