import asyncio
import time

from config import aintq
from tasks import add, mul, slow


async def main():
    await aintq.init()
    await add(10, b=5)
    time.sleep(1)
    await mul(a=2, b=7)
    time.sleep(1)
    await slow(5)


if __name__ == '__main__':
    asyncio.run(main())
