import asyncio
import logging
import json
from typing import (
    Any,
    NamedTuple,
    Union,
    Optional,
    Dict,
    Iterator,
)

import HStream.HStreamApi_pb2 as ApiPb
import HStream.HStreamApi_pb2_grpc as ApiGrpc

logger = logging.getLogger(__name__)


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


class TimeStamp(NamedTuple):
    seconds: int
    nanos: int


class RecordHeader(NamedTuple):
    publish_time: TimeStamp
    key: Optional[str]
    attributes: Dict[str, str]


class Record(NamedTuple):
    id: RecordId
    header: RecordHeader
    payload: Union[bytes, dict]


class Consumer:
    ResponseTy = Any

    _stub: ApiGrpc.HStreamApiStub
    _requests: asyncio.Queue
    # _call:

    def __init__(
        self,
        name: str,
        subscription: str,
        find_stub_coro,
        processing_func,
    ):
        self._name = name
        self._subscription = subscription
        self._requests = asyncio.Queue()
        self._find_stub_coro = find_stub_coro
        self._processing_func = processing_func

    async def start(self):
        self._stub = await self._find_stub_coro()
        await self._requests.put(self._fetch_request)
        self._call = self._stub.StreamingFetch(self._request_gen())
        try:
            async for r in self._call:
                await self._processing_func(
                    self._ack,
                    self._stop,
                    self._parse_recived_record(r.receivedRecords),
                )
        except asyncio.exceptions.CancelledError:
            logger.info("Consumer is Cancelled")

    async def _stop(self):
        if self._call:
            self._call.cancel()
        else:
            logger.error("Make sure you have started the consumer!")

    async def _ack(self, record_ids: [RecordId]):
        await self._requests.put(
            ApiPb.StreamingFetchRequest(
                subscriptionId=self._subscription,
                consumerName=self._name,
                ackIds=[record_id_to(r) for r in record_ids],
            )
        )

    async def _request_gen(self):
        while True:
            r = await self._requests.get()
            self._requests.task_done()
            if not r:
                break
            else:
                yield r

    @property
    def _fetch_request(self):
        return ApiPb.StreamingFetchRequest(
            subscriptionId=self._subscription,
            consumerName=self._name,
            ackIds=[],
        )

    @staticmethod
    def _parse_recived_record(rs: [ApiPb.ReceivedRecord]) -> Iterator[Record]:
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
                    hstream_record.header.key
                    if hstream_record.header.key
                    else None
                ),
                attributes=hstream_record.header.attributes,
            )

            record_type = hstream_record.header.flag
            record_payload = None
            if record_type == ApiPb.HStreamRecordHeader.Flag.RAW:
                record_payload = hstream_record.payload
            elif record_type == ApiPb.HStreamRecordHeader.Flag.JSON:
                try:
                    record_payload = json.loads(hstream_record.payload)
                except json.decoder.JSONDecodeError:
                    logger.error("Can not decode this JSON payload!")
            else:
                raise NotImplementedError("Unsupported record type!")

            if record_payload:
                yield Record(
                    id=record_id, header=record_header, payload=record_payload
                )
