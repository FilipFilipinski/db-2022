import asyncio
from dataclasses import dataclass
from datetime import datetime

import asyncpg
from asyncache import cached
from cachetools import TTLCache

from aio_basics.profile import log
from db_service.utils import get_random_lastname

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
class User:
    id: int
    name: str


@dataclass
class Villa:
    id: int
    rate: int
    city: str


class DataError(RuntimeError):
    """Raised when problems with save/update of dato to db occur (constraints etc)"""


async def create_pool():
    log(f'creating pool for db:{DB_HOST}:5432, db={DB_DB}')
    pool = await asyncpg.create_pool(host=DB_HOST, port=5432, database=DB_DB, user=DB_USER, password=DB_PASS)
    log(f'pool created')
    return pool


class AirbnbDbService:
    """ Prototype of DAO, data access object"""
    pool: asyncpg.pool.Pool

    async def initalize(self):
        self.pool = await create_pool()

    @cached(TTLCache(maxsize=10, ttl=2))
    async def get_all_users(self) -> list[User]:
        # fixme: list of selected id's?  await c.fetch('select * from airbnb.users where id = ANY ($1)', ids)
        log('getting users from db')
        async with self.pool.acquire() as c:
            rows = await c.fetch('select * from airbnb.users order by name')  # -> list[Record] -- wynik zapytania
        return [User(**d) for d in dicts(rows)]

    async def create_user(self, u: User) -> User:
        async with self.pool.acquire() as c:
            res = await c.fetch('''
                        INSERT INTO airbnb.users(name)
                        VALUES ($1) RETURNING *''',
                                u.name)
            d = dict(res[0])
            return User(**d)

    async def update_user(self, u: User):
        async with self.pool.acquire() as c:
            try:
                res = await c.fetch('''
                                UPDATE airbnb.users
                                SET name=$2
                                WHERE id = $1
                                RETURNING *''', u.id, u.name)
                d = dict(res[0])
                return User(**d)
            except IndexError as e:
                raise DataError(f'User with id {u.id} does not exist; cannot update')

    async def delete_user(self, id: int):
        async with self.pool.acquire() as c:
            await c.execute('''
                            DELETE FROM airbnb.users
                            WHERE id = $1
                            ''', id)
            log(f'Removed user {id}')

    async def add_book_villa(self, uid: int, villaid: int):
        async with self.pool.acquire() as c:
            # ~~ sposoby reakcji na błędy związane z ograniczeniami na tabelach
            res = await c.fetch('''
                        INSERT INTO airbnb.uservillas(userid,villaid)
                        VALUES ($1,$2)''',
                                uid, villaid)
            # ON CONFLICT(userid) DO UPDATE set userid=$1
        pass  # fixme: catch all errors

    async def get_all_villas(self) -> list[Villa]:
        log('getting villas from db')
        async with self.pool.acquire() as c:
            rows = await c.fetch('select * from airbnb.villas')  # -> list[Record] -- wynik zapytania
        return [Villa(**d) for d in dicts(rows)]

    async def get_villa_by_id(self, villaid: int):
        async with self.pool.acquire() as c:
            rows = await c.fetch('select * from airbnb.villas where id = $1', villaid)
            return Villa(**rows[0])

    async def create_villa(self, u: Villa) -> Villa:
        async with self.pool.acquire() as c:
            try:
                res = await c.fetch('''
                               INSERT INTO airbnb.villas(rate, city)
                               VALUES ($1,$2) RETURNING *''',
                                    u.rate, u.city)
                return res
            except asyncpg.exceptions.DataError:
                print("Data error", c)

    async def remove_booking(self, uid: int, vid: int):
        async with self.pool.acquire() as c:
            await c.execute('''
                            DELETE FROM airbnb.uservillas
                            WHERE userid = $1 and villaid = $2
                            ''', uid, vid)
            log(f'Removed user {uid}, {vid}')

    async def get_all_booked_villas(self, userid: int) -> list[Villa]:
        async with self.pool.acquire() as c:
            rows = await c.fetch('select * from airbnb.uservillas where userid = $1', userid)
            villalist = []
            for i in rows:
                villalist.append([Villa(**d) for d in dicts(await c.fetch("""select * from airbnb.villas 
                where id = $1""", i[1]))][0])
            return villalist


async def run_it():
    db = AirbnbDbService()
    await db.initalize()

    # u = await db.create_user(User(0, get_random_lastname()))

    """users = await db.get_all_Villas()
    for u in users:
        print(u)"""

    # await db.create_villa(Villa(1, 12, 'test1'))
    # await db.remove_booking(2, 10)
    print(await db.get_all_booked_villas(2))

    # await db.get_all_booked_villas(2) <------
    # await db.add_book_villa(3, 2)
    # await db.delete_user(1)


if __name__ == '__main__':
    asyncio.run(run_it())
