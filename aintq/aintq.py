# -*- coding: utf-8 -*-

"""Main module."""
from aintq.db import db
from aintq.task import Task
from aintq.utils import pickle_data
from aintq.worker import AintQConsumer


class Aintq(object):
    bind = None
    registry = {}

    def __init__(self, bind=None):
        self.bind = bind

    async def init(self):
        if self.bind:
            await db.set_bind(self.bind)

    def create_consumer(self, **options):
        return AintQConsumer(self, **options)

    def task(self, **kwargs):
        def decorator(func):
            return TaskWrapper(
                self,
                func.func if isinstance(func, TaskWrapper) else func,
                **kwargs)
        return decorator

    def register(self, func):
        self.registry[func.__name__] = func

    async def execute(self, func, *args, **kwargs):
        async with db.transaction():
            await Task.create(name=func.__name__, params=pickle_data(*args, **kwargs))


class TaskWrapper(object):
    def __init__(self, aintq, func, **settings):
        self.aintq = aintq
        self.func = func
        self.settings = settings
        self.aintq.register(func)

    async def __call__(self, *args, **kwargs):
        await self.aintq.execute(self.func, *args, **kwargs)
