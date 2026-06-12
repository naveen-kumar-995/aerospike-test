FROM eclipse-temurin:21-jdk

ENV TZ="Asia/Kolkata"

RUN apt-get update && apt-get install -y curl vim && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/apps/aerospike-load-tester/conf
RUN mkdir -p /logs

COPY logback.xml /opt/apps/aerospike-load-tester/conf/logback.xml

COPY target/aerospike-test-1.0-SNAPSHOT.jar /aerospike-load-tester.jar

ENTRYPOINT ["java", "-Dlogback.configurationFile=/opt/apps/aerospike-load-tester/conf/logback.xml", "-jar", "/aerospike-load-tester.jar"]