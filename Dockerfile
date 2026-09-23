# Build and run the Market Data Service.
#
# One image serves both roles the Compose file needs. The simulated exchange and the consumer are
# the same application with different properties, so shipping two images would mean building the
# same code twice and keeping the two in step by hand.

FROM eclipse-temurin:21-jdk AS build
WORKDIR /src

# Dependencies first, so editing a source file does not re-resolve them.
COPY gradle gradle
COPY gradlew settings.gradle build.gradle ./
RUN chmod +x gradlew && ./gradlew --no-daemon dependencies --configuration runtimeClasspath

COPY src src
# Tests are not run here: they need a Docker daemon of their own for Testcontainers, which is not
# available inside a build. CI runs them instead.
RUN ./gradlew --no-daemon bootJar -x test


FROM eclipse-temurin:21-jre
WORKDIR /app

# curl is here for the Compose health checks, which gate start-up ordering: the consumer must not
# begin migrating before the database and the exchange are actually answering.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /src/build/libs/*.jar /app/app.jar

RUN useradd --system --uid 10001 marketdata && chown -R marketdata:marketdata /app
USER marketdata

EXPOSE 8099

ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/app.jar"]
