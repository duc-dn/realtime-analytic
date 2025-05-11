import os
import pandas as pd
import mysql.connector
from util.logger import logger


class OrderStatusGenerator:
    def __init__(self, host, user, password, database):
        self.cnxpool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name="orderstatuspool",
            pool_size=5,
            pool_reset_session=True,
            host=host,
            user=user,
            password=password,
            database=database,
        )

    def get_connection(self):
        return self.cnxpool.get_connection()

    def insert_order_status(self, conn, statuses):
        try:
            cursor = conn.cursor()
            sql = """
                INSERT INTO order_status (id, status_name)
                VALUES (%s, %s)
            """
            cursor.executemany(sql, statuses)
            conn.commit()
        except Exception as e:
            logger.error(e)

    def run(self, csv_path):
        df = pd.read_csv(csv_path)
        statuses = df[["id", "status_name"]].values.tolist()
        with self.get_connection() as conn:
            self.insert_order_status(conn, statuses)


if __name__ == "__main__":
    MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
    MYSQL_USER = os.getenv("MYSQL_USER", "root")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "debezium")
    MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "myshop")
    CSV_PATH = f"{os.getcwd()}/data/order_status.csv"

    generator = OrderStatusGenerator(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
    )
    generator.run(CSV_PATH)
