import time

from config import aintq


def tprint(s, c=32):
    # Helper to print messages from within tasks using color, to make them
    # stand out in examples.
    print('\x1b[1;%sm%s\x1b[0m' % (c, s))


# Tasks used in examples.

@aintq.task()
def add(a, b):
    res = a + b
    tprint(res)
    return res


@aintq.task()
def mul(a, b):
    res = a * b
    tprint(res)
    return res


@aintq.task()
async def slow(n):
    # generate a coroutine function
    tprint('going to sleep for %s seconds' % n)
    time.sleep(n)
    tprint('finished sleeping for %s seconds' % n)
    return n

