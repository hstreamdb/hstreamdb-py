# hstreamdb-py

Python client for [HStreamDB](https://github.com/hstreamdb/hstream)

## Installation

Create a new virtual environment if you prefer to use isolated environments:

```sh
virtualenv -p python3 ./venv
source ./venv/bin/activate
```

Install hstreamdb:

```sh
pip install hstreamdb
```

## Examples

Here's a basic example (For more examples and api documentation, see:
<https://hstreamdb.github.io/hstreamdb-py/>) :

```python
$ python

>>> from hstreamdb import insecure_client
>>> async def main():
...     async with await insecure_client(host="127.0.0.1", port=6570) as client:
...         streams = await client.list_streams()
...         print(streams)
...
>>> import asyncio
>>> asyncio.run(main())
```
