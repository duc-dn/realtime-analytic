import datetime
import os
import random

import pandas as pd
from faker.providers import DynamicProvider


def generate_phone_numbers(num):
    phone_numbers = []
    for i in range(num):
        prefix = random.choice(["03", "05", "07", "08", "09"])
        suffix = "".join([str(random.randint(0, 9)) for _ in range(8)])
        phone_number = f"{prefix}-{suffix[:4]}-{suffix[4:]}"
        phone_numbers.append(phone_number)

    pd.DataFrame(phone_numbers, columns=["phone_number"]).to_csv(
        f"{os.getcwd()}/data_csv/phone_numbers.csv", index=False
    )


def generate_brand_name():
    brand = [
        "Apple",
        " Asus",
        " BlackBerry",
        " Google",
        " HTC",
        " Huawei",
        " Lenovo",
        " LG",
        " Motorola",
        " Nokia",
        " OnePlus",
        " Oppo",
        " Realme",
        " Samsung",
        " Sony",
        " TCL",
        " Vivo",
        " Xiaomi ",
        "ZTE",
        " Infinix",
    ]

    brand = [name.strip() for name in brand]
    pd.DataFrame(brand, columns=["brand_name"]).to_csv(
        f"{os.getcwd()}/data_csv/brand_names.csv", index=False
    )


def generate_products():
    products = []


if __name__ == "__main__":
    # generate_phone_numbers(10000)
    generate_brand_name()
