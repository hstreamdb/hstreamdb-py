import hashlib
import logging
import gzip
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


def encode_payload(payload):
    raw_type = ApiPb.HStreamRecordHeader.Flag.RAW
    hrecord_type = ApiPb.HStreamRecordHeader.Flag.JSON

    # NOTE: if payload is a tuple, its contents must be
    # (encoded_bytes, HStreamRecordHeader_Flag). We do not check it but it requires.

    if isinstance(payload, tuple):  # alreay encoded, ignore
        return payload
    elif isinstance(payload, bytes):
        return payload, raw_type
    elif isinstance(payload, dict):
        payload_struct = Struct()
        payload_struct.update(payload)
        return payload_struct.SerializeToString(), hrecord_type
    elif isinstance(payload, str):
        return payload.encode("utf-8"), raw_type
    else:
        raise ValueError("Invalid payload type!")


def _cons_record(payload, key):
    _payload, _payload_type = encode_payload(payload)
    return ApiPb.HStreamRecord(
        header=ApiPb.HStreamRecordHeader(
            flag=_payload_type,
            attributes=None,
            key=key,
        ),
        payload=_payload,
    )


def _encode_records(
    records: List[ApiPb.HStreamRecord], compresstype=None, compresslevel=9
) -> ApiPb.BatchedRecord:
    records_bs = ApiPb.BatchHStreamRecords(records=records).SerializeToString()
    if not compresstype:
        _compresstype = ApiPb.CompressionType.Value("None")
    elif compresstype == "gzip":
        _compresstype = ApiPb.CompressionType.Value("Gzip")
        records_bs = gzip.compress(records_bs, compresslevel=compresslevel)
    else:
        raise KeyError(f"Unsupported CompressionType: {compresstype}")

    return ApiPb.BatchedRecord(
        compressionType=_compresstype,
        batchSize=len(records),
        payload=records_bs,
    )


def encode_records(
    payloads, key=None, compresstype=None, compresslevel=9
) -> ApiPb.BatchedRecord:
    return _encode_records(
        [_cons_record(p, key) for p in payloads],
        compresstype=compresstype,
        compresslevel=compresslevel,
    )


def encode_records_from_append_payload(
    payloads, compresstype=None, compresslevel=9
) -> ApiPb.BatchedRecord:
    return _encode_records(
        [
            _cons_record((p._payload_bin, p._payload_type), p.key)
            for p in payloads
        ],
        compresstype=compresstype,
        compresslevel=compresslevel,
    )


def decode_records(record_: ApiPb.ReceivedRecord) -> Iterator[Record]:
    record_ids = record_.recordIds
    batched_record = record_.record

    if batched_record.compressionType == ApiPb.CompressionType.Value("None"):
        payload_bytes = batched_record.payload
    elif batched_record.compressionType == ApiPb.CompressionType.Value("Gzip"):
        payload_bytes = gzip.decompress(batched_record.payload)
    else:
        raise NotImplementedError(
            f"Unsupported CompressionType: {batched_record.compressionType}"
        )

    try:
        batched_hstream_record = ApiPb.BatchHStreamRecords()
        batched_hstream_record.ParseFromString(payload_bytes)
        hstream_records = batched_hstream_record.records
    except message.DecodeError:
        logger.error("Can not decode this BatchHStreamRecords!")

    if len(record_ids) != len(hstream_records):
        raise KeyError("Invalid BatchHStreamRecords: mismatch of size")

    for rid, hstream_record in zip(record_ids, hstream_records):
        record_id = record_id_from(rid)

        record_header = RecordHeader(
            publish_time=TimeStamp(
                seconds=batched_record.publishTime.seconds,
                nanos=batched_record.publishTime.nanos,
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
