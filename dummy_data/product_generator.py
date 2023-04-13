import mysql.connector
import random
import datetime
from time import sleep
from faker import Faker
from faker.providers import DynamicProvider
from util.logger import logger

class ProductGenerator:
    def __init__(self):
        self.local_time = datetime.datetime.now()
        self.time_days = [0, 86400, 172800, 345600, 432000]

        self.cnxpool = mysql.connector.pooling.MySQLConnectionPool (
            pool_name='mypool',
            pool_size=5,
            pool_reset_session=True,
            host='mysql',
            user='root',
            password='debezium',
            database='myshop'
        )
    
    def get_connection(self):
        return self.cnxpool.get_connection()
    
    @staticmethod
    def product_dummy(self):
        product_item = ()
        return product_item
    
    def insert_products(
            self, 
            conn: str,
    ):
        try:
            cursor = conn.cursor()
            sql = """
                INSERT INTO PRODUCTS
                (product_name, price, category_id, brand_id, created_at, description)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            
            product_item = self.product_dummy()

            cursor.execute(sql, product_item)
            conn.commit()
        except Exception as e:
            logger.error(e)

    