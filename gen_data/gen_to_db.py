import mysql.connector
import random
from time import sleep

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
  
  mydb = mysql_connector("mysql", "mysqluser", "mysqlpw", "inventory")
  
  conn = mydb.connect_to_myql()
  mycursor = conn.cursor()

  while True:
    sql = "INSERT INTO orders (order_date, purchaser, quantity, product_id) VALUES (%s, %s, %s, %s)"
    val = ("2022-02-14", random.randint(1001, 1004), random.randint(10, 50), random.randint(101, 109))
    mycursor.execute(sql, val)  

    conn.commit()

    print(val)
    sleep(5)