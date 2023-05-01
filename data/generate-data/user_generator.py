import datetime
import os
import random
from time import sleep

import mysql.connector
import pandas as pd
from faker import Faker
from faker.providers import DynamicProvider
from util.logger import logger


class UserGenerator:
    def __init__(self):
        self.local_time = datetime.datetime.now()
        self.time_days = 86400
        self.fake = Faker()

        self.cnxpool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name="mypool",
            pool_size=5,
            pool_reset_session=True,
            host="mysql",
            user="root",
            password="debezium",
            database="myshop",
        )

    def get_connection(self):
        return self.cnxpool.get_connection()

    def users_dummy(self):
        address = pd.read_csv(f"{os.getcwd()}/data/address.csv")["address"].tolist()
        address_provider = DynamicProvider(provider_name="my_address", elements=address)
        phone_numbers_provider = DynamicProvider(
            provider_name="my_phone_number",
            elements=pd.read_csv(f"{os.getcwd()}/data/phone_numbers.csv")[
                "phone_number"
            ].tolist(),
        )
        self.fake.add_provider(address_provider)
        self.fake.add_provider(phone_numbers_provider)

        user = (
            self.fake.user_name(),
            self.fake.first_name() + " " + self.fake.last_name(),
            self.fake.email(),
            self.fake.my_address(),
            self.fake.my_phone_number(),
            int(self.local_time.timestamp()) - random.randint(0, 100) * self.time_days,
        )
        return user

    def insert_users(self, conn):
        try:
            cursor = conn.cursor()
            sql = """
                INSERT INTO users
                (username, fullname, email, address, phone_number, created_at)
                VALUES (%s, %s, %s, %s, %s, %s)
            """

            users_item = self.users_dummy()
            print(users_item)
            cursor.execute(sql, users_item)
            conn.commit()
        except Exception as e:
            logger.error(e)

    def run(self):
        with self.get_connection() as conn:
            self.insert_users(conn)


if __name__ == "__main__":
    u = UserGenerator()

    while True:
        u.run()
        sleep(10)
