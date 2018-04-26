import asyncio
import logging
import traceback

from asyncpg.exceptions import DuplicateObjectError
from gino.loader import ColumnLoader

from .task import db, Task


class _WorkerExit(BaseException):
    pass


class AintQConsumer:
    delay = db.func.extract(
        'EPOCH',
        Task.schedule - db.func.timezone('UTC', db.func.now())).label('delay')
    get_query = db.select([
        Task,
        Task.ctid,
        delay,
    ]).order_by(
        Task.schedule.nullsfirst(),
    ).limit(
        1,
    ).with_for_update(
        skip_locked=True,
    ).execution_options(
        loader=Task.load(
            ctid=ColumnLoader(Task.ctid),
            delay=ColumnLoader(delay),
        )
    )

    def __init__(self, size=8, loop=None):
        if loop is None:
            loop = asyncio.get_event_loop()
        self._loop = loop
        self._semaphore = asyncio.Semaphore(loop=loop)
        self._maxsize = size
        self._free = self._size = 0
        self._running = True
        self._ticker = None
        self._next_tick = 0
        self._break = True
        self._events = asyncio.Queue(loop=loop)

    async def worker(self):
        self._size += 1
        self._free += 1
        logger = logging.getLogger('aintq.worker-{}'.format(self._size))
        logger.info('Worker started.')
        try:
            while True:
                await self._semaphore.acquire()
                if not self._running or self._size > self._maxsize:
                    raise _WorkerExit()
                logger.debug('awaken')
                self._free -= 1
                try:
                    while True:
                        async with db.transaction(reuse=False):
                            if await self._work(logger):
                                break
                        if not self._running or self._size > self._maxsize:
                            raise _WorkerExit()
                        if (await db.scalar(Task.seq.next_value())) % 256 == 0:
                            await db.status('VACUUM ANALYZE aintq.tasks')
                except Exception:
                    traceback.print_exc()
                    # TODO: robust worker doesn't die, retry
                    pass
                finally:
                    self._free += 1
                    logger.debug('sleep')
        except _WorkerExit:
            pass
        finally:
            self._size -= 1
            self._free -= 1

    async def _work(self, logger):
        self._break = True
        task = await self.get_query.gino.first()
        if task is None:
            logger.debug('end of queue')
            return self._break
        if task.delay and task.delay > 0:
            logger.debug('no due')
            next_tick = self._loop.time() + task.delay
            if next_tick < self._next_tick:
                self._clear_tick()
            if self._ticker is None:
                logger.debug('set tick %r', task.delay)
                self._next_tick = next_tick
                self._ticker = self._loop.call_at(next_tick, self._tick)
            return self._break
        logger.debug('got a task %r', task.ctid)
        if self._free:
            self._semaphore.release()
        async with db.transaction() as tx:
            try:
                result = await task.run()
            except Exception:
                traceback.print_exc()
                tx.raise_rollback()
            else:
                logger.critical('got result: %r', result)
        await task.delete()

    def _clear_tick(self):
        if self._ticker is not None:
            ticker, self._ticker = self._ticker, None
            ticker.cancel()

    def _tick(self):
        self._ticker = None
        if self._free:
            self._semaphore.release()

    async def run(self):
        await db.gino.create_all()
        for sql in Task.trigger:
            try:
                await db.status(sql)
            except DuplicateObjectError:
                pass
            except Exception:
                traceback.print_exc()
                pass
        for _ in range(self._maxsize - self._size):
            asyncio.ensure_future(self.worker(), loop=self._loop)
        async with db.acquire() as conn:
            await conn.raw_connection.add_listener(
                'aintq_enqueue', lambda *x: self._events.put_nowait(x[3]))
            while True:
                await self._events.get()
                if not self._running:
                    break
                if self._free == self._size:
                    self._semaphore.release()
                else:
                    self._break = False

    # @property
    # def maxsize(self):
    #     return self._maxsize
    #
    # @maxsize.setter
    # def maxsize(self, val):
    #     self._maxsize, diff = val, val - self._maxsize
    #     if val < 0:
    #         self._size - self._free
