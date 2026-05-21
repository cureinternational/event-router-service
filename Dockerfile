FROM amazoncorretto:17-alpine AS builder
ARG JAR_FILE=target/*.jar
COPY ${JAR_FILE} event-router-service.jar
RUN java -Djarmode=tools -jar event-router-service.jar extract --layers --destination extracted

FROM amazoncorretto:17-alpine
COPY --from=builder extracted/dependencies/ ./
COPY --from=builder extracted/spring-boot-loader/ ./
COPY --from=builder extracted/snapshot-dependencies/ ./
COPY --from=builder extracted/application/ ./
ENTRYPOINT ["java", "org.springframework.boot.loader.launch.JarLauncher"]
