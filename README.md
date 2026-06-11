### Commands

**HSQL DB**:
Run from `db` folder
```
sh start.sh
```

**Spark**:
Run from `spark` folder
```
MAVEN_OPTS="-javaagent:../monitoring/jmx_exporter/jmx_prometheus_javaagent.jar=9091:../monitoring/jmx_exporter/config.yml" \
 mvn clean compile exec:java -Dexec.mainClass="com.taboola.spark.SparkApp"
```

**API**:
Run from `api` folder
```
mvn clean compile spring-boot:run
```

**Dashboard**:
Run from `monitoring` folder
```
docker compose up -d
```

Then connect to [http://localhost:3000/d/spark-pipeline/spark-pipeline?orgId=1&refresh=5s](http://localhost:3000/d/spark-pipeline/spark-pipeline?orgId=1&refresh=5s)
