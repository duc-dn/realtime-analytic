import os
import pandas as pd
import mysql.connector
from util.logger import logger


class CategoryGenerator:
    def __init__(self, host, user, password, database):
        self.cnxpool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name="categorypool",
            pool_size=5,
            pool_reset_session=True,
            host=host,
            user=user,
            password=password,
            database=database,
        )

    def get_connection(self):
        return self.cnxpool.get_connection()

    def insert_categories(self, conn, categories):
        try:
            cursor = conn.cursor()
            sql = """
                INSERT INTO categories (category_name, created_at)
                VALUES (%s, UNIX_TIMESTAMP())
            """
            cursor.executemany(sql, [(cat,) for cat in categories])
            conn.commit()
        except Exception as e:
            logger.error(e)

    def run(self, csv_path):
        categories = pd.read_csv(csv_path)["category_name"].tolist()
        with self.get_connection() as conn:
            self.insert_categories(conn, categories)


if __name__ == "__main__":
    MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
    MYSQL_USER = os.getenv("MYSQL_USER", "root")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "debezium")
    MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "myshop")
    CSV_PATH = f"{os.getcwd()}/data/categories.csv"

    generator = CategoryGenerator(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
    )
    generator.run(CSV_PATH)
