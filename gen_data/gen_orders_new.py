import mysql.connector
import random
import datetime
from time import sleep
from faker import Faker
from faker.providers import DynamicProvider
from util.logger import logger

local_time = datetime.datetime.now()

# gen data payment_type_provider
payment_type_provider = DynamicProvider(
    provider_name="payment_type", elements=["instalment", "credit_card", "cash"]
)

fake = Faker()
fake.add_provider(payment_type_provider)


class mysql_connector():
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database

    def __enter__(self):
        self.conn = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database
        )
        self.cursor = self.conn.cursor()
        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.conn.rollback()
        else:
            self.conn.commit()
        self.cursor.close()
        self.conn.close()


if __name__ == "__main__":
    with mysql_connector("mysql", "root", "debezium", "myshop") as cursor:
      while True:
        logger.info(20 * '-' + "inserting to orders table" + '-' * 20)
        sql = "INSERT INTO orders (user_id, payment, status) value (%s, %s, %s)"
        val = (random.randint(0, 10000), fake.payment_type(), "confirmed")
        cursor.execute(sql, val)

        # random products of a order
        order_number = random.randint(0, 5)
        logger.info(20 * '-' + "inserting to order_detail table" + '-' * 20)
        order_id = cursor.lastrowid

        order_details = []
        for i in range(0, order_number):

          product_id = random.randint(1, 695)
          quantity = random.randint(1, 10)

          logger.info(20 * '-' + "get price from products" + '-' * 20)
          cursor.execute(f"select price from products where id = {product_id}")
          price = cursor.fetchone()[0]

          order_details.append((order_id, product_id, quantity, price * quantity, int(local_time.timestamp())))

        try:
          sql = "INSERT INTO order_detail (order_id, product_id, quantity, total_price, create_at) VALUES (%s, %s, %s, %s, %s)"
          cursor.executemany(sql, order_details)
        except Exception as e:
          logger.error(e)
        sleep(5)
