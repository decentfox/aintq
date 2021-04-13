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
    """
    The dequeue query, using ctid to identify the task within the same
    transaction, and central-database-calculated delay time to schedule
    deferred tasks.
    
    """

    def __init__(self, aintq, size=8, loop=None):
        if loop is None:
            loop = asyncio.get_event_loop()
        self.aintq = aintq
        self._loop = loop

        self._semaphore = asyncio.Semaphore(loop=loop)
        """
        This semaphore is used to wake consumer worker coroutines.
        
        """

        self._maxsize = size
        """
        Control concurrent consumer workers.
        
        """

        self._free = self._size = 0
        """
        Internal counters presenting idle and alive workers in real time.
        
        """

        self._running = True
        """
        Used for graceful shutdown.
        
        """

        self._ticker = None
        """
        A asyncio timer handle for scheduled deferred tasks. All workers share
        one ticker.
        
        """

        self._next_tick = 0
        """
        The ticker only fires once for the most recent task.
        
        """

        self._break = True
        """
        Used for a racing scenario when an event of new task arrives while the
        last busy worker is retrieving new tasks right after the last task was
        completed.
        
        """

        self._events = asyncio.Queue(loop=loop)
        """
        Used to turn new task event callbacks into coroutine.
        
        """

    async def worker(self):
        """The outer worker coroutine.

        A worker is always in idle unless explicitly triggered by
        ``semaphore.release()``. Then the worker will run until there is no
        further tasks to immediately execute in the queue.

        """
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
        """The inner worker code within a transaction.

        Dequeue and locking the task must be step one to avoid deadlocks.

        Returning ``True`` will put the worker into sleep, indicating the end
        of queue is reached, or next task is scheduled to run in the future.

        Before executing a task, another worker is always awaken if there is
        any sleeping, so that the second task in the queue could be executed in
        time.

        The actual task code is executed within a savepoint subtransaction,
        thus it may fail and cause the subtransaction rolled back. The outer
        transaction is always committed with the task row deleted for
        performance.

        """

        # Always break, unless stopped by a racing new task event
        self._break = True

        # The dequeue
        task = await self.get_query.gino.first()

        # End of queue?
        if task is None:
            logger.debug('end of queue')
            return self._break

        # Task is not due?
        if task.delay and task.delay > 0:
            logger.debug('no due')

            # Calculate next_tick in loop time
            next_tick = self._loop.time() + task.delay

            # Keep only the most recent tick
            if next_tick < self._next_tick and self._ticker is not None:
                ticker, self._ticker = self._ticker, None
                ticker.cancel()

            # Schedule the ticker
            if self._ticker is None:
                logger.debug('set tick %r', task.delay)
                self._next_tick = next_tick
                self._ticker = self._loop.call_at(next_tick, self._tick)

            return self._break

        # Got a task, wake up another sleeping worker if any
        logger.debug('got a task %r', task.ctid)
        if self._free:
            self._semaphore.release()

        # Create a savepoint for actual task
        async with db.transaction() as tx:
            try:
                result = await task.run(self.aintq)
            except Exception:
                traceback.print_exc()
                tx.raise_rollback()
            else:
                logger.critical('got result: %r', result)

        # Always delete the task row after execution regardless of success or
        # failure, in order to avoid duplicate execution
        await Task.delete.where(Task.ctid == task.ctid).gino.status()

    def _tick(self):
        """Scheduled trigger."""
        self._ticker = None
        self._wake_up_one()

    def _wake_up_one(self):
        """Wake up one worker, or stop racing workers from falling asleep."""
        if self._free:
            self._semaphore.release()
        else:
            self._break = False

    async def run(self):
        """Main coroutine."""

        # Make sure the schema is present
        await db.gino.create_all()
        for sql in Task.trigger:
            try:
                await db.status(sql)
            except DuplicateObjectError:
                pass
            except Exception:
                traceback.print_exc()
                pass

        # Start workers
        for _ in range(self._maxsize - self._size):
            asyncio.create_task(self.worker())

        # Listen to new task events
        async with db.acquire() as conn:
            await conn.raw_connection.add_listener(
                'aintq_enqueue', lambda *x: self._events.put_nowait(x[3]))
            while True:
                await self._events.get()
                if not self._running:
                    break
                self._wake_up_one()

    # @property
    # def maxsize(self):
    #     return self._maxsize
    #
    # @maxsize.setter
    # def maxsize(self, val):
    #     self._maxsize, diff = val, val - self._maxsize
    #     if val < 0:
    #         self._size - self._free
