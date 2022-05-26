import functools
from typing import Optional
import grpc
import logging

import HStream.HStreamApi_pb2 as ApiPb
import HStream.HStreamApi_pb2_grpc as ApiGrpc

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
                client.switch_channel()
            else:
                raise e

    return wrapper


class HStreamClient:
    _channels: {str: Optional[grpc.aio.Channel]} = {}
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

    # -------------------------------------------------------------------------

    # TODO
    def switch_channel(self):
        pass

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
