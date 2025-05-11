import datetime
import random
import os
from time import sleep

import mysql.connector
from faker import Faker
from faker.providers import DynamicProvider
from util.logger import logger


class ProductGenerator:
    def __init__(self, host, user, password, database):
        self.local_time = datetime.datetime.now()
        self.time_days = [0, 86400, 172800, 345600, 432000]

        self.cnxpool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name="mypool",
            pool_size=5,
            pool_reset_session=True,
            host=host,
            user=user,
            password=password,
            database=database,
        )

    def get_connection(self):
        return self.cnxpool.get_connection()

    def product_dummy(self):
        fake = Faker()
        product_name = fake.word().capitalize()
        price = round(random.uniform(10, 1000), 2)
        category_id = random.randint(1, 5)
        brand_id = random.randint(1, 5)
        created_at = int(self.local_time.timestamp()) - random.choice(self.time_days)
        description = fake.sentence()
        return (product_name, price, category_id, brand_id, created_at, description)

    def insert_products(
        self,
        conn: str,
    ):
        try:
            cursor = conn.cursor()
            sql = """
                INSERT INTO products
                (product_name, price, category_id, brand_id, created_at, description)
                VALUES (%s, %s, %s, %s, %s, %s)
            """

            # Đọc dữ liệu từ file products.csv nếu có
            products_csv_path = f"{os.getcwd()}/data/products.csv"
            if os.path.exists(products_csv_path):
                import pandas as pd

                df = pd.read_csv(products_csv_path)
                # Giả sử file products.csv có các cột: product_name, price, category_id, brand_id, created_at, description
                products = df[
                    [
                        "products_name",
                        "price",
                        "catagory_id",
                        "brand_id",
                        "created_at",
                        "description",
                    ]
                ].values.tolist()
                cursor.executemany(sql, products)
                print(f"Inserted {len(products)} products from products.csv")
                conn.commit()
                return
            else:
                product_item = self.product_dummy()
                print(product_item)
                cursor.execute(sql, product_item)
                conn.commit()
        except Exception as e:
            logger.error(e)


if __name__ == "__main__":
    MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
    MYSQL_USER = os.getenv("MYSQL_USER", "root")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "debezium")
    MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "myshop")

    p = ProductGenerator(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
    )

    with p.get_connection() as conn:
        p.insert_products(conn)
