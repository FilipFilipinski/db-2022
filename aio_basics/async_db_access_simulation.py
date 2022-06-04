import asyncio
from asyncio import sleep
from random import randint

from aio_basics.profile import log


async def call_db(i):
    log(f'task{i} calling db')
    await sleep(0.1 * randint(1, 6))
    log(f'task{i} db answer ready')


async def foo(i):
    log(f'hello {i}')
    await call_db(i)
    log(f'end of task {i}')


async def run_it():
    log('app starts')
    # await foo(1)
    asyncio.create_task(foo(1))
    asyncio.create_task(foo(2))
    asyncio.create_task(foo(3))
    await sleep(0.5)
    log('app ends')


if __name__ == '__main__':
    asyncio.run(run_it())  # "engine" asynchroniczny uruchamiany (even loop)
