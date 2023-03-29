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

    products_name = ["bread", "noodles", "Iphone", "SamSung", "candy"]

    while True:
        sql = "INSERT INTO products (name, description, weight) VALUES (%s, %s, %s)"
        val = (products_name[random.randint(0, len(products_name) - 1)], "OK", random.randint(10, 50))
        mycursor.execute(sql, val)  

        conn.commit()

        print(val)
        sleep(5)