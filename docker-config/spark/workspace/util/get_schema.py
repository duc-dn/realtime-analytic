from confluent_kafka.schema_registry import SchemaRegistryClient
from util.logger import logger


def get_schema(schema_id: str):
    """_summary_

    Args:
        schema_id (str): id of schema on schema-registry

    Returns:
        schema compatible with id
    """
    try:
        sr = SchemaRegistryClient({"url": "http://schema-registry:8081"})
        schema = sr.get_schema(schema_id=schema_id).schema_str
    except Exception as e:
        logger.error(f"Error: {e}")

    return schema
