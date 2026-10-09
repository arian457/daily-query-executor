FROM eclipse-temurin:21-jdk AS build
WORKDIR /app
COPY gradlew build.gradle.kts settings.gradle.kts ./
COPY gradle gradle
RUN ./gradlew --no-daemon dependencies -q > /dev/null 2>&1 || true
COPY src src
RUN ./gradlew --no-daemon bootJar -x test

FROM eclipse-temurin:21-jre
COPY --from=build /app/build/libs/*.jar /app.jar
ENTRYPOINT ["java", "-jar", "/app.jar"]
