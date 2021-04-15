import asyncio
import sys
import os


def load_aintq(path):
    try:
        _path, _klass = path.rsplit('.', 1)
        __import__(_path)
        mod = sys.modules[_path]
        return getattr(mod, _klass)
    except:
        cur_dir = os.getcwd()
        if cur_dir not in sys.path:
            sys.path.insert(0, cur_dir)
            return load_aintq(path)
        raise


async def main():
    aintq = load_aintq(sys.argv[1])
    await aintq.init()
    consumer = aintq.create_consumer()
    await consumer.run()


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
