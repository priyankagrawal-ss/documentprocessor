# ============================
# Stage 1: Build with Gradle
# ============================
FROM gradle:8.7-jdk21 AS build
WORKDIR /app

COPY gradlew ./
COPY gradle gradle
COPY build.gradle* settings.gradle* ./

RUN ./gradlew --no-daemon dependencies || true

COPY src src

RUN ./gradlew build --no-daemon -x test

# ============================
# Stage 2: Runtime Image
# ============================
FROM eclipse-temurin:21-jre-jammy AS runtime
WORKDIR /app

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        fonts-liberation2 \
        ghostscript \
        libreoffice \
        qpdf \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*


COPY --from=build /app/build/libs/*.jar app.jar

RUN useradd -ms /bin/bash appuser
USER appuser

EXPOSE 8080
ENV SOFFICE_PATH=/usr/bin/soffice

ENTRYPOINT ["java", "-jar", "app.jar"]
