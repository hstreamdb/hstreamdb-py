import abc
import asyncio
from typing import Optional, Any, Callable, Awaitable, Iterable, Type
import logging

__all__ = ["PayloadsFull", "PayloadTooBig", "PayloadGroup", "BufferedProducer"]

logger = logging.getLogger(__name__)


class Timer:
    _continue: asyncio.Event = asyncio.Event()
    _enable: bool = True
    _task: asyncio.Task

    def __init__(self, delay, coro):
        self._delay = delay
        self._coro = coro
        self._task = asyncio.create_task(self._loop())

    def start(self):
        self._continue.set()

    def stop(self):
        self._continue.clear()
        self._task.cancel()

    def exit(self):
        self._enable = False

    async def _loop(self):
        while True:
            if not self._enable:
                break
            try:
                await self._continue.wait()
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
    _timer: Optional[Timer] = None

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

    def exit(self):
        if self._timer:
            self._timer.exit()

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

    class AppendCallback(abc.ABC):
        @abc.abstractmethod
        def on_success(
            self, stream_name: str, payloads: [bytes], stream_key: Optional[str]
        ):
            ...

        @abc.abstractmethod
        def on_fail(
            self,
            stream_name: str,
            payloads: [bytes],
            stream_key: Optional[str],
            e: Exception,
        ):
            ...

    def __init__(
        self,
        flush_coro: Callable[
            [str, Iterable[Any], Optional[str]], Awaitable[None]
        ],
        append_callback: Optional[Type[AppendCallback]] = None,
        size_trigger=0,
        time_trigger=0,
        workers=1,
        retry_count=0,
        retry_max_delay=60,  # seconds
    ):
        if workers < 1:
            raise ValueError("workers must be no less than 1")
        self._size_trigger = size_trigger
        self._time_trigger = time_trigger
        self._retry_count = retry_count
        self._retry_max_delay = retry_max_delay
        self._flush_coro = flush_coro
        self._append_callback = append_callback
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

    async def flushall(self):
        for _, payloads in self._group.items():
            await payloads.flush()

    async def close(self):
        for _, pg in self._group.items():
            pg.exit()

        await self.flushall()

        for q in self._queues:
            await q.put(None)

    async def wait(self):
        try:
            await asyncio.gather(
                *[pg._timer._task for _, pg in self._group.items() if pg._timer]
            )
        except asyncio.CancelledError:
            pass
        await asyncio.gather(*self._workers)

    async def wait_and_close(self):
        await self.close()
        await self.wait()

    async def _loop_queue(self, queue):
        while True:
            group_key = await queue.get()
            if group_key is None:
                break

            payload_group = self._group[group_key]
            stream_name, stream_key = self._uncons_group_key(group_key)

            await self._flusing_worker(stream_name, payload_group, stream_key)
            queue.task_done()

    async def _flusing_worker(self, stream_name, payload_group, stream_key):
        payloads, _size = payload_group.pop()
        logger.debug(
            f"Flushing stream <{stream_name},{stream_key}> "
            f"with {len(payloads)} batches..."
        )
        retries = 0
        while True:
            try:
                await self._flush_coro(stream_name, payloads, stream_key)
                await payload_group.post_flush()
            except Exception as e:  # TODO: should be a specific append exception
                if self._retry_count < 0 or retries < self._retry_count:
                    logger.debug(
                        f"Retrying {retries} with max deley {self._retry_max_delay}s..."
                    )
                    await asyncio.sleep(
                        min(2**retries, self._retry_max_delay)
                        if self._retry_max_delay >= 0
                        else 2**retries
                    )
                    retries += 1
                    continue
                else:
                    if self._append_callback:
                        return self._append_callback.on_fail(
                            stream_name, payloads, stream_key, e
                        )
                    else:
                        raise e
            break

        if self._append_callback:
            return self._append_callback.on_success(
                stream_name, payloads, stream_key
            )

    def _find_queue(self, group_key):
        return self._queues[abs(hash(group_key)) % len(self._queues)]
