import functools
from typing import Optional, Any, Iterable, Type, Iterator
import grpc
import logging

import HStream.Server.HStreamApi_pb2 as ApiPb
import HStream.Server.HStreamApi_pb2_grpc as ApiGrpc
from hstreamdb.aio.producer import BufferedProducer
from hstreamdb.aio.consumer import Consumer
from hstreamdb.types import (
    RecordId,
    record_id_from,
    Stream,
    stream_type_from,
    Subscription,
    subscription_type_from,
)

__all__ = ["insecure_client", "HStreamDBClient"]

logger = logging.getLogger(__name__)


def dec_api(f):
    @functools.wraps(f)
    async def wrapper(client, *args, **kargs):
        try:
            return await f(client, *args, **kargs)
        except grpc.aio.AioRpcError as e:
            # The service is currently unavailable, so we choose another
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                await client._switch_channel()
                return await f(client, *args, **kargs)
            else:
                raise e

    return wrapper


class HStreamDBClient:
    _stub: ApiGrpc.HStreamApiStub

    _DEFAULT_STREAM_KEY = "__default__"
    _TargetTy = str

    _channels: {_TargetTy: Optional[grpc.aio.Channel]} = {}

    _current_target: _TargetTy
    _append_channels: {(str, str): _TargetTy} = {}
    _subscription_channels: {str: _TargetTy} = {}

    _cons_target = staticmethod(lambda host, port: f"{host}:{port}")

    def __init__(self, host: str = "127.0.0.1", port: int = 6570):
        self._current_target = self._cons_target(host, port)
        # TODO: secure_channel
        _channel = grpc.aio.insecure_channel(self._current_target)
        self._channels[self._current_target] = _channel
        self._stub = ApiGrpc.HStreamApiStub(_channel)

    async def init_cluster_info(self):
        cluster_info = await self._stub.DescribeCluster(None)
        # TODO: check protocolVersion, serverVersion
        for node in cluster_info.serverNodes:
            target = self._cons_target(node.host, node.port)
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
    async def list_streams(self) -> Iterator[Stream]:
        """List all streams"""
        r = await self._stub.ListStreams(ApiPb.ListStreamsRequest())
        return (stream_type_from(s) for s in r.streams)

    async def append(
        self,
        name: str,
        payloads: Iterable[Any],
        key: Optional[str] = None,
    ) -> Iterator[RecordId]:
        """Append payloads to a stream.

        Args:
            name: stream name
            payloads: a list of string, bytes or dict(json).
            key: Optional stream key.

        Returns:
            Appended RecordIds generator
        """

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

        return (record_id_from(x) for x in r.recordIds)

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
        subscription_id: str,
        stream_name: str,
        ack_timeout: int = 600,  # 10min
        max_unacks: int = 10000,
    ):
        await self._stub.CreateSubscription(
            ApiPb.Subscription(
                subscriptionId=subscription_id,
                streamName=stream_name,
                ackTimeoutSeconds=ack_timeout,
                maxUnackedRecords=max_unacks,
            )
        )

    @dec_api
    async def list_subscriptions(self) -> Iterator[Subscription]:
        r = await self._stub.ListSubscriptions(None)
        return (subscription_type_from(s) for s in r.subscription)

    @dec_api
    async def does_subscription_exist(self, subscription_id: str):
        r = await self._stub.CheckSubscriptionExist(
            ApiPb.CheckSubscriptionExistRequest(subscriptionId=subscription_id)
        )
        return r.exists

    @dec_api
    async def delete_subscription(self, subscription_id: str, force=False):
        await self._stub.DeleteSubscription(
            ApiPb.DeleteSubscriptionRequest(
                subscriptionId=subscription_id, force=force
            )
        )

    def new_consumer(self, name: str, subscription_id: str, processing_func):
        async def find_stub():
            channel = await self._lookup_subscription(subscription_id)
            return ApiGrpc.HStreamApiStub(channel)

        return Consumer(
            name,
            subscription_id,
            find_stub,
            processing_func,
        )

    # -------------------------------------------------------------------------

    async def _switch_channel(self):
        while True:
            logger.warning(
                f"Target {self._current_target} unavailable, switching to another..."
            )
            # remove unavailable target
            self._channels.pop(self._current_target)

            if not self._channels:
                raise RuntimeError("No unavailable targets!")

            # Now, self._channels should not be empty.
            self._current_target = list(self._channels.keys())[0]
            channel = self._get_channel(self._current_target)
            self._stub = ApiGrpc.HStreamApiStub(channel)

            try:
                return await self.init_cluster_info()
            except grpc.aio.AioRpcError as e:
                # The service is currently unavailable, so we choose another
                logger.warning(
                    f"Fetch cluster info from {self._current_target} failed! \n {e}"
                )
                continue

    async def _lookup_stream(self, name, key=None):
        key = key or self._DEFAULT_STREAM_KEY
        target = self._append_channels.get((name, key))
        if not target:
            node = await self._lookup_stream_api(name, key)
            target = self._cons_target(node.host, node.port)
            self._append_channels[(name, key)] = target

        logger.debug(f"Find target for stream <{name},{key}>: {target}")

        return self._get_channel(target)

    @dec_api
    async def _lookup_stream_api(self, name, key):
        r = await self._stub.LookupStream(
            ApiPb.LookupStreamRequest(streamName=name, orderingKey=key)
        )
        # there is no reason that returned value does not equal to requested.
        assert r.streamName == name and r.orderingKey == key
        return r.serverNode

    async def _lookup_subscription(self, subscription_id: str):
        target = self._subscription_channels.get(subscription_id)
        if not target:
            node = await self._lookup_subscription_api(subscription_id)
            target = self._cons_target(node.host, node.port)
            self._subscription_channels[subscription_id] = target

        logger.debug(
            f"Find target for subscription <{subscription_id}>: {target}"
        )

        return self._get_channel(target)

    @dec_api
    async def _lookup_subscription_api(self, subscription_id: str):
        r = await self._stub.LookupSubscription(
            ApiPb.LookupSubscriptionRequest(subscriptionId=subscription_id)
        )
        assert r.subscriptionId == subscription_id
        return r.serverNode

    def _get_channel(self, target):
        channel = self._channels.get(target)
        if channel:
            return channel
        else:
            # new channel
            channel = grpc.aio.insecure_channel(target)
            self._channels[target] = channel
            return channel

    # -------------------------------------------------------------------------

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        for target, channel in self._channels.items():
            if channel:
                await channel.close(grace=None)


async def insecure_client(host="127.0.0.1", port=6570):
    """Creates an insecure client to a cluster.

    Args:
        host: hostname to connect to HStreamDB, defaults to '127.0.0.1'
        port: port to connect to HStreanDB, defaults to 6570

    Returns:
        A :class:`HStreamDBClient`
    """
    client = HStreamDBClient(host=host, port=port)
    await client.init_cluster_info()
    return client
