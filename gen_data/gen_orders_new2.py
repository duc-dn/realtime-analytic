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
    
  def connect_to_myql(self):
    mydb = mysql.connector.connect(
      host=self.host,
      user=self.user,
      password=self.password,
      database=self.database
    )
    return mydb

if __name__ == "__main__":
  
  mydb = mysql_connector("mysql", "root", "debezium", "myshop")
  
  conn = mydb.connect_to_myql()
  mycursor = conn.cursor()

  while True:
    logger.info(20*'-' + "inserting to orders table" + '-'*20)
    sql = "INSERT INTO orders (user_id, payment, status) value (%s, %s, %s)"
    val = (random.randint(0, 10000), fake.payment_type(), "confirmed")
    mycursor.execute(sql, val)  
    conn.commit()

    # random products of a order
    order_number = random.randint(0, 5)
    order_id = mycursor.lastrowid
    logger.info(20*'-' + "inserting to order_detail table" + '-'*20)
    
    logger.info(f"=============== ORDER ID {order_id} ==============")
    order_details = []
    for i in range(0, order_number):

      product_id = random.randint(1, 800)
      quantity = random.randint(1, 6)

      logger.info(20*'-' + "get price from products" + '-'*20)
      mycursor.execute(f"select price from products where id = {product_id}")
      price = mycursor.fetchone()[0]
      
      order_details.append((order_id, product_id, quantity, price * quantity, int(local_time.timestamp())))

      logger.info(20*"-" + "UPDATE QUANTITY OF PRODUCT ID = " + str(product_id) + ", QUANTITY = " + str(quantity) + "-"*20)
      mycursor.execute(f"update products set quantity = quantity - {quantity} where id = {product_id}")


    try:
      sql = "INSERT INTO order_detail (order_id, product_id, quantity, total_price, create_at) VALUES (%s, %s, %s, %s, %s)"
      mycursor.executemany(sql, order_details) 
      conn.commit()
    except Exception as e:
      logger.error(e)

    sleep(10)