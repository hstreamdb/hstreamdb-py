"""
Simple tool for writing to hstreamdb, demonstrating usage of the append API.
"""
import asyncio
from hstreamdb import insecure_client


async def create_stream_if_not_exist(client, name):
    ss = await client.list_streams()
    if name not in {s.name for s in ss}:
        await client.create_stream(name, 1)


async def main(host, port, stream_name):
    async with await insecure_client(host=host, port=port) as client:
        await create_stream_if_not_exist(client, stream_name)
        for i in range(10):
            print(f"Append {i}")
            await client.append(stream_name, ["x"])


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Append Example")
    parser.add_argument(
        "--host", type=str, help="server host", default="127.0.0.1"
    )
    parser.add_argument("--port", type=int, help="server port", default=6570)
    parser.add_argument(
        "--stream-name",
        type=str,
        help="name of the stream, default is 'test_stream'",
        default="test_stream",
    )

    args = parser.parse_args()
    asyncio.run(main(args.host, args.port, args.stream_name))
