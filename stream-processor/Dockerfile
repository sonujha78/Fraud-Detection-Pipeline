FROM eclipse-temurin:21-jre-jammy

WORKDIR /app

COPY build/libs/fraud-detection-streams-1.0.0-all.jar app.jar

ENV KAFKA_BOOTSTRAP_SERVERS=kafka.kafka.svc.cluster.local:9092

ENTRYPOINT ["java", "-jar", "app.jar"]
