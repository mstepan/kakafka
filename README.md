# KaKafka (Kinda Almost Kafka)

Use the following:
* java 17
* [maven wrapper](https://maven.apache.org/wrapper/index.html) with maven 3.9.3 
`mvn wrapper:wrapper -Dmaven=3.9.3`
* [Netty 4.x](https://netty.io/wiki/user-guide-for-4.x.html)

## Build
```
./mvnw clean package
```

## Unit tests.

```
./mvnw test
```

## Local run

To start a single broker or set of broker execute:

```bash
./run.sh
```

Inside mentioned above file you can find cluster configuration:
```
cluster_mode=true
NODES_CNT=5
```

To stop all running brokers execute:
```bash
./stop-all.sh
```

## Kakafka Protocol Description

//todo:  

