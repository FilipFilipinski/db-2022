from faker import Faker


def get_random_lastname():
    return Faker().name().split()[1]