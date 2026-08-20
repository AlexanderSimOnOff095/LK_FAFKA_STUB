FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY pom.xml .
COPY src ./src
RUN mvn -B package

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /workspace/target/lk-fafka-stub.jar /app/lk-fafka-stub.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/lk-fafka-stub.jar"]
