# KaKafka (Kinda Almost Kafka)

Use the following:
* java 17
* [maven wrapper](https://maven.apache.org/wrapper/index.html) with maven 3.9.3 
`mvn wrapper:wrapper -Dmaven=3.9.3`
* [Netty 4.x](https://netty.io/wiki/user-guide-for-4.x.html)

## Build
```
./mvnw clean package -DskipTests
```

## Local run

After the build is completed you can start a single broker or set of broker using script:

```bash
./run.sh
```

Inside `run.sh` script you can find cluster configuration:
```
cluster_mode=true # run application in cluster mode
NODES_CNT=5 # number of broker to start
```

You can find all running brokers by executing:
```bash
jps | grep kakafka-*
```

To stop all running brokers execute:
```bash
./stop-all.sh
```

## Unit/Integration tests.

Right now we only have integration tests, so to run tests suites you should build application first and 
then run brokers locally using `run.sh` script.

To run tests just execute:
```
./mvnw test
```


## Kakafka Protocol Description

//todo:  

