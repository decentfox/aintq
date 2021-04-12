import asyncio
import threading
from datetime import datetime, timedelta


from aintq.worker import AintQConsumer
from aintq.task import db, Task


t1 = dict(name='t1')
t2 = dict(schedule=datetime.utcnow() + timedelta(seconds=10), name='t2')
t3 = dict(name='t3')

mock_data = [t1, t2, t3]


def thread_run():
    loop = asyncio.new_event_loop()
    loop.run_until_complete(create_task())
    loop.close()


async def create_task():
    await db.set_bind('postgresql://localhost/aintq')
    while mock_data:
        timer = asyncio.create_task(inner())
        await timer


async def inner():
    async with db.transaction():
        await Task.create(**mock_data.pop(0))

thread = threading.Thread(name='Thread-1', target=thread_run())
thread.start()


async def main():
    await db.set_bind('postgresql://localhost/aintq')
    consumer = AintQConsumer()
    await consumer.run()

asyncio.get_event_loop().run_until_complete(main())
