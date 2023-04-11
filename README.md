### DO AN: XAY DUNG MO HINH PHAN TICH VA XU LY DU LIEU REALTIME
---
- Create connector between kafka connector and mysql
```
docker exec connect curl -X POST -H "Content-Type:application/json" -d @/connect/config_connector_myshop.json http://localhost:8083/connectors

docker exec connect curl -X DELETE http://localhost:8083/connectors/myshop-connector
```
---
- Copy jars file into spark master and spark worker
```
docker cp ./spark/lib/. master:/opt/spark/jars
docker cp ./spark/lib/. worker-a:/opt/spark/jars
docker cp ./spark/lib/. worker-b:/opt/spark/jars

docker exec -it master spark-submit --master spark://master:7077 /opt/workspace/main.py
```

#### Superset
---
- Config superset
```
docker-compose -f docker-compose-non-dev.yml up -d

docker network connect superset_default trino

docker exec -it b89  superset fab create-admin \
              --username admin \
              --firstname Superset \
              --lastname Admin \
              --email admin@superset.com \
              --password admin

docker exec -it 'superset_id' bash

pip install trino

trino://admin@trino:8080/delta
```
### Trino
---
```
call system.register_table(schema_name => 'default', table_name => 'customers', table_location => 's3a://datalake/brozen/cdc.inventory.customers');

call system.register_table(schema_name => 'default', table_name => 'orders', table_location => 's3a://datalake/brozen/cdc.inventory.orders');

call system.register_table(schema_name => 'default', table_name => 'products', table_location => 's3a://datalake/brozen/cdc.inventory.products');
```