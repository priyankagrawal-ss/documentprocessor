# Stage 1: Build the Java application using Gradle
FROM gradle:8.7-jdk21 AS build
WORKDIR /home/gradle/src
COPY --chown=gradle:gradle . .
RUN gradle build --no-daemon -x test

# Stage 2: Create the final runtime image
FROM eclipse-temurin:21-jre-jammy
WORKDIR /app

# Install runtime dependencies including LibreOffice, Ghostscript, AND qpdf
RUN apt-get update && \
    apt-get install -y \
        libreoffice \
        libreoffice-java-common \
        fonts-liberation2 \
        ghostscript \
        qpdf \
        --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

# Copy the application JAR from the 'build' stage
COPY --from=build /home/gradle/src/build/libs/*.jar app.jar

EXPOSE 8080
ENV SOFFICE_PATH=/usr/bin/soffice

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar app.jar"]