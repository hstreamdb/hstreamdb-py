from typing import NamedTuple, Optional, Dict, Union

import HStream.Server.HStreamApi_pb2 as ApiPb


class TimeStamp(NamedTuple):
    seconds: int
    nanos: int


class Stream(NamedTuple):
    name: str
    replication_factor: int


def stream_type_from(stream: ApiPb.Stream) -> Stream:
    return Stream(
        name=stream.streamName, replication_factor=stream.replicationFactor
    )


class Subscription(NamedTuple):
    subscription_id: str
    stream_name: str
    ack_timeout: int
    max_unacks: int


def subscription_type_from(sub: ApiPb.Subscription) -> Subscription:
    return Subscription(
        subscription_id=sub.subscriptionId,
        stream_name=sub.streamName,
        ack_timeout=sub.ackTimeoutSeconds,
        max_unacks=sub.maxUnackedRecords,
    )


class RecordId(NamedTuple):
    shard_id: int
    batch_id: int
    batch_index: int


def record_id_to(record_id: RecordId) -> ApiPb.RecordId:
    return ApiPb.RecordId(
        shardId=record_id.shard_id,
        batchId=record_id.batch_id,
        batchIndex=record_id.batch_index,
    )


def record_id_from(record_id: ApiPb.RecordId) -> RecordId:
    return RecordId(
        shard_id=record_id.shardId,
        batch_id=record_id.batchId,
        batch_index=record_id.batchIndex,
    )


class RecordHeader(NamedTuple):
    publish_time: TimeStamp
    key: Optional[str]
    attributes: Dict[str, str]


class Record(NamedTuple):
    id: RecordId
    header: RecordHeader
    payload: Union[bytes, dict]
