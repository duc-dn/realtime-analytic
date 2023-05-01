import os
import datetime
import pandas as pd
import numpy as np
import random

from faker import Faker
from faker.providers import DynamicProvider
from tqdm import tqdm
from faker_vehicle import VehicleProvider

local_time = datetime.datetime.now()
time_days = 86400


# gen data payment_type_provider
payment_type_provider = DynamicProvider(
    provider_name="payment_type", elements=["instalment", "credit_card", "cash"]
)

# gen data address
address = pd.read_csv(f'{os.getcwd()}/data_csv/address.csv')["address"].tolist()
address_provider = DynamicProvider(
    provider_name="my_address", elements=address
)

phone_numbers_provider = DynamicProvider(
    provider_name="my_phone_number", elements=pd.read_csv(f'{os.getcwd()}/data_csv/phone_numbers.csv')["phone_number"].tolist()
)

fake = Faker()
fake.add_provider(VehicleProvider)
fake.add_provider(payment_type_provider)
fake.add_provider(address_provider)
fake.add_provider(phone_numbers_provider)


def gen_user(num=1):
    users = []
    
    for i in tqdm(range(1, num)):
        user = {}

        user["id"] = i + 1
        user["username"] = fake.user_name()
        user["fullname"] = fake.first_name() + " " + fake.last_name()
        user["email"] = fake.email()

        user["address"] = fake.my_address()
        user["phone_number"] = fake.my_phone_number()
        user["created_at"] = int(local_time.timestamp()) - random.randint(0, 100) * time_days

        users.append(user)
    
    dfu = pd.DataFrame(users)
    dfu.to_csv(f"{os.getcwd()}/data_mysql/users.csv", index=False)

def gen_brand(num=1):
    brand_name = pd.read_csv((f'{os.getcwd()}/data_csv/brand_names.csv'))["brand_name"].to_list()

    brands = []

    for i in range(len(brand_name)):
        brand = {}

        brand["id"] = i + 1
        brand["brand_name"] = brand_name[i]
        brand["description"] = "Designed by " + brand_name[i]
        brand["created_at"] = int(local_time.timestamp()) - random.randint(0, 100) * time_days

        brands.append(brand)

    dfb = pd.DataFrame(brands)
    dfb.to_csv(f"{os.getcwd()}/data_mysql/brand.csv", index=False)


def gen_categories():
    categories_name = ["smartphone", "phone accessories", "headphone", "mouse", "laptop", "keyboard", "barttery", "chager"]

    categories = []

    for i in range(len(categories_name)):
        category = {}

        category["id"] = i + 1
        category["category_name"] = categories_name[i]
        category["description"] = categories_name[i] + " category"
        category["created_at"] = int(local_time.timestamp()) - random.randint(0, 100) * time_days

        categories.append(category)

    dfc = pd.DataFrame(categories)
    dfc.to_csv(f"{os.getcwd()}/data_mysql/categories.csv", index=False)

def gen_inventory():
    df = pd.read_csv(f"{os.getcwd()}/data_mysql/products.csv")
    pro_length = len(df)

    inventory_lst = []

    for i in range(0, pro_length):
        inventory = {}
        
        inventory["id"] = i + 1
        inventory["product_id"] = i + 1
        inventory["quantity"] = random.randint(100, 300)
        inventory["updated_at"] = int(local_time.timestamp()) - random.randint(0, 100) * time_days

        inventory_lst.append(inventory)

    dfin = pd.DataFrame(inventory_lst)
    dfin.to_csv(f"{os.getcwd()}/data_mysql/inventory.csv", index=False)

def gen_products():
    df = pd.read_csv(f"{os.getcwd()}/data_csv/products.csv")

    # filter brand id, and category_id
    category_filter = [
        df['products_name'].str.contains('smartphone'),
        df['products_name'].str.contains('laptop'),
        df['products_name'].str.contains('headphone'),
        df['products_name'].str.contains('mouse') |
        df['products_name'].str.contains('Mouse'),
        df['products_name'].str.contains('Keyboard')
    ]

    brand_filter = [
        df['products_name'].str.contains('Samsung'),
        df['products_name'].str.contains('Apple') |
        df['products_name'].str.contains('AirPods') |
        df['products_name'].str.contains('iPhone'),
        df['products_name'].str.contains('Xiaomi'),
        df['products_name'].str.contains('HTC'),
        df['products_name'].str.contains('LG'),
        df['products_name'].str.contains('Realme'),
        df['products_name'].str.contains('Oppo'),
        df['products_name'].str.contains('Asus'),
        df['products_name'].str.contains('Huawei'),
        df['products_name'].str.contains('Lenovo'),
        df['products_name'].str.contains('BlackBerry'),
        df['products_name'].str.contains('Google'),
        df['products_name'].str.contains('Dell'),
    ]

    # ===================== Products ======================
    # change order of id column into first
    df['id'] = range(len(df))
    id_column = df.pop('id')
    df.insert(0, 'id', id_column)
    df['id'] = df['id'] + 1

    # random for price of products
    df["price"] = np.random.randint(100, 2000, size=len(df))

    # choices_brand = [13, 0, 17, 4, 7, 12, 11, 1, 5, 6, 2, 3] + 1
    choices_brand = [14, 1, 18, 5, 8, 13, 12, 2, 6, 7, 3, 4, 20]
    choices_categories = [1, 5, 3, 4, 6]

    df['category_id'] = np.select(category_filter, choices_categories, default=-10000000000)
    df['brand_id'] = np.select(brand_filter, choices_brand, default=-200000000000)

    # create timestamp which products were created
    df["created_at"] = df.apply(lambda row: generate_created_at(), axis=1)

    # description for product
    df["description"] = "designed by " + df["products_name"].str.extract(r'(\w+)', expand=False)
    # df["description"] = df["description"].str.split(" ")[0]

    df.to_csv(f"{os.getcwd()}/data_mysql/products.csv", index=False)

def generate_created_at():
    return (
        int(local_time.timestamp()) 
        - random.randint(0, 100) * time_days
    )

if __name__ == '__main__':
    while True:
        print(20 * "=" + " Welcome to my project " + "=" * 20)
        print("What is the table that you want to generate??\n")

        print("1. Brands")
        print("2. Products")
        print("3. Users")
        print("4. Inventory")
        print("5. categories")
        print("6. Exit\n")


        choose = int(input("Your choose: "))

        if choose == 1:
            gen_brand()
            print("GENERATE BRAND SUCESSFULLY")
        elif choose == 2:
            gen_products()
            print("GENERATE PRODUCT SUCESSFULLY")
        elif choose == 3:
            num = int(input("How many users do you want to generate?"))
            gen_user(num=num)
        elif choose == 4:
            gen_inventory()
        elif choose == 5:
            gen_categories()
        elif choose == 6:
            print("see you again!!")
            break
        else:
            print("Your choose is invalid!!")
            break
