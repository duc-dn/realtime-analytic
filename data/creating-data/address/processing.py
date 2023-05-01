import os

import pandas as pd

cwd = os.getcwd()
products_path = os.path.join(cwd, "products")
frames = []


def concat_file():
    for f in os.listdir(products_path):
        if f.endswith("csv"):
            filepath = os.path.join(products_path, f)

            df = pd.read_csv(filepath)
            frames.append(df)

    result = pd.concat(frames).dropna()
    return result


def get_distinct_value(df, field: str):
    return df[field].unique()


df = concat_file()
address = get_distinct_value(df, "Purchase Address")

df = pd.DataFrame(address, columns=["address"])
df.to_csv("address.csv", index=False)
