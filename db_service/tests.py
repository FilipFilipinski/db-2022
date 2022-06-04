import unittest

from db_service.airbnb_db_service import AirbnbDbService, User


class AirbnbDbTests(unittest.IsolatedAsyncioTestCase):

    # @unittest.skip('--')
    async def test_create_user(self):
        db = AirbnbDbService()
        await db.initalize()
        u = await db.create_user(User(0, 'Wu'))
        assert u.id > 0

    async def test_create_remove_user(self):
        db = AirbnbDbService()
        await db.initalize()
        u = await db.create_user(User(0, 'Wu'))
        await db.delete_user(u.id)
        users = await db.get_all_users()
        ids = [us.id for us in users]
        assert u.id not in ids

    async def test_create_two_users_with_same_names_is_possible(self):
        db = AirbnbDbService()
        await db.initalize()
        u1 = await db.create_user(User(0, 'Wu'))
        u2 = await db.create_user(User(0, 'Wu'))
        assert u1.id != u2.id




if __name__ == '__main__':
    unittest.main()
