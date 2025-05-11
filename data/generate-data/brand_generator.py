import os
import pandas as pd
import mysql.connector
from util.logger import logger


class BrandGenerator:
    def __init__(self, host, user, password, database):
        self.cnxpool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name="brandpool",
            pool_size=5,
            pool_reset_session=True,
            host=host,
            user=user,
            password=password,
            database=database,
        )

    def get_connection(self):
        return self.cnxpool.get_connection()

    def insert_brands(self, conn, brands):
        try:
            cursor = conn.cursor()
            sql = """
                INSERT INTO brands (brand_name, created_at)
                VALUES (%s, UNIX_TIMESTAMP())
            """
            cursor.executemany(sql, [(brand,) for brand in brands])
            conn.commit()
        except Exception as e:
            logger.error(e)

    def run(self, csv_path):
        brands = pd.read_csv(csv_path)["brand_name"].tolist()
        with self.get_connection() as conn:
            self.insert_brands(conn, brands)


if __name__ == "__main__":
    MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
    MYSQL_USER = os.getenv("MYSQL_USER", "root")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "debezium")
    MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "myshop")
    CSV_PATH = f"{os.getcwd()}/data/brand.csv"

    generator = BrandGenerator(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
    )
    generator.run(CSV_PATH)
