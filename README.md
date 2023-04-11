### DO AN: XAY DUNG MO HINH PHAN TICH VA XU LY DU LIEU REALTIME
---
#### Kafka Connector
- Create connector between kafka connector and mysql
```
docker exec connect curl -X POST -H "Content-Type:application/json" -d @/connect/config_connector_myshop.json http://localhost:8083/connectors

docker exec connect curl -X DELETE http://localhost:8083/connectors/myshop-connector
```
---
#### Spark
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
#### Trino
---
```
call system.register_table(schema_name => 'default', table_name => 'users', table_location => 's3a://datalake/brozen/cdc.myshop.users');

call system.register_table(schema_name => 'default', table_name => 'brands', table_location => 's3a://datalake/brozen/cdc.myshop.brands');

call system.register_table(schema_name => 'default', table_name => 'products', table_location => 's3a://datalake/brozen/cdc.myshop.products');

call system.register_table(schema_name => 'default', table_name => 'order_detail', table_location => 's3a://datalake/brozen/cdc.myshop.order_detail');

call system.register_table(schema_name => 'default', table_name => 'orders', table_location => 's3a://datalake/brozen/cdc.myshop.orders');

call system.register_table(schema_name => 'default', table_name => 'catagories', table_location => 's3a://datalake/brozen/cdc.myshop.categories');
```