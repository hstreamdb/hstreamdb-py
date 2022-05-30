import asyncio
from typing import Optional, Any, Callable, Awaitable, Iterable
import logging

__all__ = ["PayloadsFull", "PayloadTooBig", "PayloadGroup", "BufferedProducer"]

logger = logging.getLogger(__name__)


class Timer:
    _enable: asyncio.Event = asyncio.Event()
    _task: asyncio.Task

    def __init__(self, delay, coro):
        self._delay = delay
        self._coro = coro
        self._task = asyncio.create_task(self._loop())

    def start(self):
        self._enable.set()

    def stop(self):
        self._enable.clear()
        self._task.cancel()

    async def _loop(self):
        while True:
            try:
                await self._enable.wait()
                await asyncio.sleep(self._delay)
                await asyncio.shield(self._coro())
            except asyncio.CancelledError:
                # do nothing
                logger.debug("Timer: receive CancelledError")


class PayloadsFull(Exception):
    pass


class PayloadTooBig(Exception):
    pass


class PayloadGroup:
    _payloads: [bytes] = []
    _size: int = 0
    _lock: asyncio.Lock

    _maxsize: int
    _timer: Optional[asyncio.Task] = None

    _flushing_payloads: [bytes] = []
    _flushing_size: int = 0
    _flush_done: asyncio.Event = asyncio.Event()
    _key: Any
    _notify_queue: asyncio.Queue

    def __init__(self, queue, key, maxsize=0, maxtime=0):
        self._key = key
        self._notify_queue = queue
        self._lock = asyncio.Lock()
        self._maxsize = maxsize
        if maxtime > 0:
            self._timer = Timer(maxtime, self.flush)

    async def append(self, payload: bytes):
        if not self._payloads and self._timer:
            # The first payload comes, set timer
            self._timer.start()

        payload_size = len(payload)

        if self._maxsize > 0 and payload_size > self._maxsize:
            raise PayloadTooBig

        # reach maxsize
        if self._upper(payload_size):
            await self.flush()

        await self._append_nowait(payload)

    async def flush(self):
        if self._flushing_payloads:
            # block until last flushing done
            logger.debug("waiting last flush done...")
            await self._flush_done.wait()

        if self._timer:
            self._timer.stop()

        # no current flushing, trigger it
        self._flush_nowait()

    def pop(self):
        return self._flushing_payloads, self._flushing_size

    async def post_flush(self):
        self._flushing_payloads = []
        self._flushing_size = 0
        self._flush_done.set()

    def _flush_nowait(self):
        self._flushing_payloads = self._payloads
        self._flushing_size = self._size
        self._payloads = []
        self._size = 0
        self._flush_done.clear()
        self._notify_queue.put_nowait(self._key)

    async def _append_nowait(self, payload: bytes):
        """Put a payload into the payloads without blocking.

        If no free bytes is immediately available, raise PayloadsFull.
        """
        payload_size = len(payload)
        if self._upper(payload_size):
            raise PayloadsFull

        # FIXME: does this lock really needed?
        async with self._lock:
            self._payloads.append(payload)
            self._size += payload_size
            logger.debug(f"append size: {self._size}")

    async def _set_timer(self, delay):
        async def loop():
            asyncio.sleep(delay)
            pass

        asyncio.create_task(loop)

    def _upper(self, size):
        """Return True if there are not exceed maxsize bytes.

        Note: if the Payloads was initialized with maxsize=0 (the default),
        then _upper() is never True.
        """
        if self._maxsize <= 0:
            return False
        else:
            return (self._size + size) > self._maxsize


class BufferedProducer:
    GroupKeyTy = (str, Optional[str])  # (stream_name, optional key)
    _cons_group_key = staticmethod(lambda name, key=None: (name, key))
    _uncons_group_key = staticmethod(
        lambda group_key: (group_key[0], group_key[1])
    )

    _group: {GroupKeyTy: PayloadGroup} = {}

    def __init__(
        self,
        flush_coro: Callable[
            [str, Iterable[Any], Optional[str]], Awaitable[None]
        ],
        size_trigger=0,
        time_trigger=0,
        workers=1,
    ):
        if workers < 1:
            raise ValueError("workers must be no less than 1")
        self._size_trigger = size_trigger
        self._time_trigger = time_trigger
        self._flush_coro = flush_coro
        self._queues = [asyncio.Queue() for _ in range(workers)]
        self._workers = [
            asyncio.create_task(self._loop_queue(self._queues[i]))
            for i in range(workers)
        ]

    async def append(
        self, stream_name: str, payload: bytes, key: Optional[str] = None
    ):
        group_key = self._cons_group_key(stream_name, key)

        payloads: PayloadGroup
        if group_key not in self._group:
            payloads = PayloadGroup(
                self._find_queue(group_key),
                group_key,
                maxsize=self._size_trigger,
                maxtime=self._time_trigger,
            )
            self._group[group_key] = payloads
        else:
            payloads = self._group[group_key]

        await payloads.append(payload)

    async def flush(self, stream_name: str, key: Optional[str] = None):
        group_key = self._cons_group_key(stream_name, key)
        payloads = self._group.get(group_key)
        if not payloads:
            raise ValueError("No such payloads!")
        await payloads.flush()

    # TODO
    def close(self):
        pass

    async def _loop_queue(self, queue):
        while True:
            group_key = await queue.get()
            payloads = self._group[group_key]
            bs, _size = payloads.pop()
            stream_name, stream_key = self._uncons_group_key(group_key)
            await self._flush_coro(stream_name, bs, stream_key)
            await payloads.post_flush()
            queue.task_done()

    def _find_queue(self, group_key):
        return self._queues[abs(hash(group_key)) % len(self._queues)]
