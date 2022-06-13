import functools
from typing import Optional, Any, Iterable, Type
import grpc
import logging

import HStream.Server.HStreamApi_pb2 as ApiPb
import HStream.Server.HStreamApi_pb2_grpc as ApiGrpc
from hstreamdb.aio.producer import BufferedProducer
from hstreamdb.aio.consumer import Consumer

__all__ = ["insecure_client", "HStreamClient", "BufferedProducer"]

logger = logging.getLogger(__name__)


def dec_api(f):
    @functools.wraps(f)
    async def wrapper(client, *args, **kargs):
        try:
            return await f(client, *args, **kargs)
        except grpc.aio.AioRpcError as e:
            # The service is currently unavailable, so we choose another
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                logger.warning("Service unavailable, switching to another...")
                client._switch_channel()
            else:
                raise e

    return wrapper


class HStreamClient:
    DEFAULT_STREAM_KEY = "__default__"
    TargetTy = str

    # TODO: improvements
    _channels: {TargetTy: Optional[grpc.aio.Channel]} = {}
    # TODO
    # _unvaliadble_channels: {TargetTy: Optional[grpc.aio.Channel]} = {}
    _append_channels: {(str, str): TargetTy} = {}
    _subscription_channels: {str: TargetTy} = {}

    _stub: ApiGrpc.HStreamApiStub

    def __init__(self, target: str):
        # TODO: secure_channel
        _channel = grpc.aio.insecure_channel(target)
        self._channels[target] = _channel
        self._stub = ApiGrpc.HStreamApiStub(_channel)

    async def init(self):
        cluster_info = await self._stub.DescribeCluster(None)
        # TODO: check protocolVersion, serverVersion
        for node in cluster_info.serverNodes:
            target = f"{node.host}:{node.port}"
            if target not in self._channels:
                self._channels[target] = None

    # -------------------------------------------------------------------------

    @dec_api
    async def create_stream(self, name, replication_factor):
        await self._stub.CreateStream(
            ApiPb.Stream(streamName=name, replicationFactor=replication_factor)
        )

    @dec_api
    async def delete_stream(self, name, ignore_non_exist=False, force=False):
        await self._stub.DeleteStream(
            ApiPb.DeleteStreamRequest(
                streamName=name, ignoreNonExist=ignore_non_exist, force=force
            )
        )

    @dec_api
    async def list_streams(self):
        """List all streams

        >>> import asyncio
        >>>
        >>> async def main():
        >>>     async with await insecure_client("127.0.0.1:6570") as client:
        >>>         streams = await client.list_streams()
        >>>         print(streams)
        >>>
        >>> asyncio.run(main())
        [{'name': 'stream1', 'replication_factor': 3}]
        """
        r = await self._stub.ListStreams(ApiPb.ListStreamsRequest())
        return [
            {"name": s.streamName, "replication_factor": s.replicationFactor}
            for s in r.streams
        ]

    async def append(
        self,
        name: str,
        payloads: Iterable[Any],
        key: Optional[str] = None,
    ):
        def cons_record(payload):
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
                return ApiPb.HStreamRecord(
                    header=ApiPb.HStreamRecordHeader(
                        flag=ApiPb.HStreamRecordHeader.Flag.JSON,
                        attributes=None,
                        key=key,
                    ),
                    payload=payload,
                )
            elif isinstance(payload, str):
                return cons_record(payload.encode("utf-8"))
            else:
                raise ValueError("Invalid payload type!")

        channel = await self._lookup_stream(name, key=key)
        stub = ApiGrpc.HStreamApiStub(channel)
        r = await stub.Append(
            ApiPb.AppendRequest(
                streamName=name, records=map(cons_record, payloads)
            )
        )
        return (
            {
                "shard_id": x.shardId,
                "batch_id": x.batchId,
                "batch_index": x.batchIndex,
            }
            for x in r.recordIds
        )

    def new_producer(
        self,
        append_callback: Optional[Type[BufferedProducer.AppendCallback]] = None,
        size_trigger=0,
        time_trigger=0,
        workers=1,
        retry_count=0,
        retry_max_delay=60,
    ):
        return BufferedProducer(
            self.append,
            append_callback=append_callback,
            size_trigger=size_trigger,
            time_trigger=time_trigger,
            workers=workers,
            retry_count=retry_count,
            retry_max_delay=retry_max_delay,
        )

    @dec_api
    async def create_subscription(
        self,
        subscription: str,
        stream_name: str,
        ack_timeout: int = 600,  # 10min
        max_unacks: int = 10000,
    ):
        await self._stub.CreateSubscription(
            ApiPb.Subscription(
                subscriptionId=subscription,
                streamName=stream_name,
                ackTimeoutSeconds=ack_timeout,
                maxUnackedRecords=max_unacks,
            )
        )

    @dec_api
    async def list_subscriptions(self):
        r = await self._stub.ListSubscriptions(None)
        return [
            {
                "subscription": s.subscriptionId,
                "stream_name": s.streamName,
                "ack_timeout": s.ackTimeoutSeconds,
                "max_unacks": s.maxUnackedRecords,
            }
            for s in r.subscription
        ]

    @dec_api
    async def does_subscription_exist(self, subscription: str):
        r = await self._stub.CheckSubscriptionExist(
            ApiPb.CheckSubscriptionExistRequest(subscriptionId=subscription)
        )
        return r.exists

    @dec_api
    async def delete_subscription(self, subscription: str, force=False):
        await self._stub.DeleteSubscription(
            ApiPb.DeleteSubscriptionRequest(
                subscriptionId=subscription, force=force
            )
        )

    def new_consumer(self, name: str, subscription: str, processing_func):
        async def find_stub():
            channel = await self._lookup_subscription(subscription)
            return ApiGrpc.HStreamApiStub(channel)

        return Consumer(
            name,
            subscription,
            find_stub,
            processing_func,
        )

    # -------------------------------------------------------------------------

    # TODO
    def _switch_channel(self):
        pass

    async def _lookup_stream(self, name, key=None):
        key = key or self.DEFAULT_STREAM_KEY
        target = self._append_channels.get((name, key))
        if not target:
            node = await self._lookup_stream_api(name, key)
            target = self._cons_target(node)
            self._append_channels[(name, key)] = target

        logger.debug(f"Find target for stream <{name},{key}>: {target}")

        channel = self._channels.get(target)
        if channel:
            return channel
        else:
            # new channel
            channel = grpc.aio.insecure_channel(target)
            self._channels[target] = channel
            return channel

    @dec_api
    async def _lookup_stream_api(self, name, key):
        r = await self._stub.LookupStream(
            ApiPb.LookupStreamRequest(streamName=name, orderingKey=key)
        )
        # there is no reason that returned value does not equal to requested.
        assert r.streamName == name and r.orderingKey == key
        return r.serverNode

    async def _lookup_subscription(self, subscription: str):
        target = self._subscription_channels.get(subscription)
        if not target:
            node = await self._lookup_subscription_api(subscription)
            target = self._cons_target(node)
            self._subscription_channels[subscription] = target

        logger.debug(f"Find target for subscription <{subscription}>: {target}")

        channel = self._channels.get(target)
        if channel:
            return channel
        else:
            # new channel
            channel = grpc.aio.insecure_channel(target)
            self._channels[target] = channel
            return channel

    @dec_api
    async def _lookup_subscription_api(self, subscription: str):
        r = await self._stub.LookupSubscription(
            ApiPb.LookupSubscriptionRequest(subscriptionId=subscription)
        )
        assert r.subscriptionId == subscription
        return r.serverNode

    @staticmethod
    def _cons_target(node):
        return f"{node.host}:{node.port}"

    # -------------------------------------------------------------------------

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        for target, channel in self._channels.items():
            if channel:
                await channel.close(grace=None)


async def insecure_client(target):
    client = HStreamClient(target)
    await client.init()
    return client
