import asyncio
from dataclasses import dataclass

import asyncpg
from asyncache import cached
from cachetools import TTLCache

from aio_basics.profile import log

DB_HOST = '10.10.0.33'
DB_DB = 'student'
DB_USER = 'student'
DB_PASS = 'wsiz#1234'


def dicts(rows):
    """
    Convert DB-rows to dictionaries.
    Note: use only for DB-rows, not for collections of objects.
    :param rows:
    :return:
    """
    return [dict(r) for r in rows]


@dataclass
class Person:
    # przykład klasy danych odpowiadającej tabeli na bazie
    id: int
    name: str

@dataclass
class GPU:
    id: int
    producent: str
    model: str
    cena: float

@dataclass
class Country:
    id: int
    name: str
    population: int


async def create_pool():
    log(f'creating pool for db:{DB_HOST}:5432, db={DB_DB}')
    pool = await asyncpg.create_pool(host=DB_HOST, port=5432, database=DB_DB, user=DB_USER, password=DB_PASS)
    log(f'pool created')
    return pool


class DbService:
    pool: asyncpg.pool.Pool

    async def initalize(self):
        self.pool = await create_pool()

    # @cached(TTLCache(10, ttl=2))
    async def get_all_persons(self) -> list[Person]:
        log('launching db request')
        async with self.pool.acquire() as c:
            log('connection obtained')
            rows = await c.fetch('select * from s1.person order by id') # -> list[Record] -- wynik zapytania
        log('db access finished')
        return [Person(**d) for d in dicts(rows)]

    async def get_person_count(self) -> int:
        async with self.pool.acquire() as c:
            n_persons = await c.fetchval("select count(*) from s1.person")
        return n_persons

    async def get_all_gpus(self) -> list[GPU]:
        async with self.pool.acquire() as c:
            rows = await c.fetch('select * from s1.karty_graficzne order by id')
        return [GPU(**d) for d in dicts(rows)]

    async def get_country_by_name(self, country_name: str) -> Country:
        async with self.pool.acquire() as c:
            row = await c.fetch('select * from s1.country where name=$1', country_name)
        return Country(**dict(row[0]))

    async def update_person(self, person: Person):
        p = person  # alias

        async with self.pool.acquire() as c:
            res = await c.fetch('''
                            UPDATE s1.person
                            SET name=$2
                            WHERE id = $1
                            RETURNING *''', p.id, p.name)
            d = dict(res[0])
            return Person(**d)

    async def delete_person(self, id: int):
        async with self.pool.acquire() as c:
            await c.execute('''
                            DELETE FROM s1.person
                            WHERE id = $1
                            ''', id)

    async def create_person(self, person: Person) -> Person:
        p = person
        async with self.pool.acquire() as c:
            res = await c.fetch('''
                        INSERT INTO s1.person(name) 
                        VALUES ($1) RETURNING *''',
                                p.name)
            d = dict(res[0])
            return Person(**d)


async def run_it():
    db = DbService()
    await db.initalize()

    # persons = await db.get_all_persons()
    # for p in persons:
    #     print(p)

    print(await db.get_person_count())

    for g in await db.get_all_gpus():
        print(g.model)

    print(await db.get_country_by_name('Ukraina'))

    # print(await db.create_person(Person(0, 'xiaoli')))
    # p = Person(3, 'xiaoli11')
    # print(await db.update_person(p))
    log('--')


if __name__ == '__main__':
    asyncio.run(run_it())
