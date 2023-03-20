from confluent_kafka.schema_registry import SchemaRegistryClient
import json

sr = SchemaRegistryClient({"url": 'http://localhost:8081'})
SCHEMA_REGISTRY_CLIENT = SchemaRegistryClient({"url": "http://localhost:8081"})

def get_schema(schema_id):
    schema = sr.get_schema(schema_id=schema_id).schema_str
    return schema



# def get_schema(schema_id: int):
#     """
#     Get schema from schema registry by schema id
#     :param schema_id: schema id
#     :return: schema
#     """
#     for i in range(3):
#         try:
#             return str(SCHEMA_REGISTRY_CLIENT.get_schema(schema_id).schema_str)
#         except Exception as e:
#             print(e)
#     # return None
# schema = json.loads(get_schema(5))

# for f in schema["fields"]:
#     print(f)
#     print("="*20)
