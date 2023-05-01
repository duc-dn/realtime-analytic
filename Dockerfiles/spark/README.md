#### Build dockerfiles
- Move to folder dockerfile of spark
```
    docker build -t cluster-apache-spark:3.2.0 .
```
#### Pushing docker image to docker responsitory
- First, login into docker hub
```
    docker login
```
- Enter username and password
- Second, creating responsitory in docker hub
- Third, push to docker hub
```
    docker push cluster-apache-spark:3.2.0
```
