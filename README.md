### DO AN: XAY DUNG MO HINH PHAN TICH VA XU LY DU LIEU REALTIME
---
```
docker exec connect curl -X POST -H "Content-Type:application/json" -d @/connect/config.json http://localhost:8083/connectors
```
    
```
docker exec connect curl -X DELETE http://localhost:8083/connectors/inventory-connector
```

```
docker cp ./spark/lib/. master:/opt/spark/jars

docker cp ./spark/lib/. worker-a:/opt/spark/jars
```

```
docker exec -it master spark-submit --master spark://master:7077 /opt/workspace/spark-demo.py
```