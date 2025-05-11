import datetime
import random
from time import sleep

import mysql.connector
from faker import Faker
from faker.providers import DynamicProvider
from util.logger import logger
import os  # Add this import


class OrderGenerator:
    def __init__(self, host, user, password, database) -> None:
        self.local_time = datetime.datetime.now()
        self.time_days = 86400

        # gen data payment_type_provider
        self.payment_type_provider = DynamicProvider(
            provider_name="payment_type", elements=["instalment", "credit_card", "cash"]
        )
        self.fake = Faker()
        self.fake.add_provider(self.payment_type_provider)

        self.cnxpool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name="mypool",
            pool_size=5,
            pool_reset_session=True,
            host=host,
            user=user,
            password=password,
            database=database,
        )

    def _get_connection(self):
        return self.cnxpool.get_connection()

    def insert_order_detail(self, conn, order_details):
        try:
            cursor = conn.cursor()
            sql = """
                INSERT INTO order_detail
                (
                    order_id,
                    product_id,
                    quantity,
                    item_price,
                    created_at
                )
                VALUES (%s, %s, %s, %s, %s)
            """
            [print(i) for i in order_details]
            cursor.executemany(sql, order_details)
            conn.commit()
        except Exception as e:
            logger.error(e)

    def update_inventory(self, conn, product_id, quantity):
        cursor = conn.cursor()
        cursor.execute(
            f"""
                update inventory
                set quantity = quantity - {quantity}
                where product_id = {product_id}
            """
        )
        conn.commit()

    def generate_order(self):
        logger.info(20 * "-" + "inserting to orders table" + "-" * 20)

        with self._get_connection() as conn:
            cursor = conn.cursor()
            time_order = int(
                self.local_time.timestamp()
            ) - self.time_days * random.randint(0, 50)

            # Lấy danh sách user_id hợp lệ
            cursor.execute("SELECT id FROM users")
            user_ids = [row[0] for row in cursor.fetchall()]
            if not user_ids:
                logger.error("No users found in users table.")
                return
            user_id = random.choice(user_ids)

            # Lấy danh sách product_id hợp lệ
            cursor.execute("SELECT id, price FROM products")
            products = cursor.fetchall()
            if not products:
                logger.error("No products found in products table.")
                return
            product_list = [(row[0], row[1]) for row in products]

            # INSERT ORDER TABLE
            sql = """
                INSERT INTO orders
                (
                    user_id,
                    payment,
                    status_id,
                    created_at
                )
                value (%s, %s, %s, %s)
            """
            val = (
                user_id,
                self.fake.payment_type(),
                random.randint(1, 4),
                time_order,
            )
            cursor.execute(sql, val)
            conn.commit()
            print("insert order done ...")

            # INSERT ORDER_DETAIL
            order_number = random.randint(1, 5)
            order_id = cursor.lastrowid

            logger.info(20 * "-" + "inserting to order_detail table" + "-" * 20)
            logger.info(f"=============== order_id: {order_id} ==============")

            order_details = []
            for i in range(order_number):
                product_id, price = random.choice(product_list)
                quantity = random.randint(1, 4)
                item_price = price * quantity

                order_details.append(
                    (order_id, product_id, quantity, item_price, time_order)
                )

                # update quantity of product in inventory table
                self.update_inventory(conn, product_id, quantity)

            self.insert_order_detail(conn, order_details)
            print("insert order detail done ...")

    def _run(self) -> None:
        while True:
            self.generate_order()
            sleep(1)


if __name__ == "__main__":
    MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
    MYSQL_USER = os.getenv("MYSQL_USER", "root")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "debezium")
    MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "myshop")

    OrderGenerator(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
    )._run()
