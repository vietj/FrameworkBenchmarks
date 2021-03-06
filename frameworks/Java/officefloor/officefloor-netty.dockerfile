FROM maven:3.5.4-jdk-10 as maven
WORKDIR /officefloor
COPY src src
WORKDIR /officefloor/src
RUN mvn -q -N clean install
WORKDIR /officefloor/src/woof_benchmark
RUN mvn -q clean install
WORKDIR /officefloor/src/woof_netty
RUN mvn -q clean package

FROM openjdk:10
WORKDIR /officefloor
COPY --from=maven /officefloor/src/woof_netty/target/woof_netty-1.0.0.jar server.jar
CMD ["java", "-server", "-Xms2g", "-Xmx2g", "-XX:+UseNUMA", "-Dhttp.port=8080", "-Dhttp.server.name=OF", "-Dhttp.date.header=true", "-jar", "server.jar"]
