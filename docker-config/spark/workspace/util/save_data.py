from pyspark.sql.dataframe import DataFrame


def save_data_to_minio(
    df: DataFrame, path: str, 
    partitionField: str,
    mode='append', fmat='delta',
):
    """
    save data to minio
    Args:
        df (DataFrame): dataframe 
        path (str): path to s3
        mode (str, optional): mode when writting data
        fmat (str, optional): format of data
    """

    try:
        df.write \
        .mode(mode) \
        .format(fmat) \
        .partitionBy(partitionField) \
        .save(path=path)
    except Exception as e:
        print(f'Error: {e}')

def save_to_database(
    df: DataFrame, table: str, username: str, 
    password: str, url: str, mode: str = 'append',
    fmat: str = 'jdbc', driver= 'org.postgresql.Driver'
):
    """
    save data to database
    Args:
        df (DataFrame): dataframe
        table (str): table name in database
        username (str): username
        password (str): password 
        url (str): url to database
        mode (str, optional): Defaults to 'append'.
        fmat (str, optional): Defaults to 'jdbc'.
        driver (str, optional): Defaults to 'org.postgresql.Driver'.
    """

    try:
        df.write.format(fmat)\
        .mode(mode) \
        .option("url", url) \
        .option("driver", driver) \
        .option("dbtable", table) \
        .option("user", username) \
        .option("password", password) \
        .save()
    except Exception as e:
        print(f'Error: {e}')