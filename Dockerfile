# Stage 1: Build pdfcpu from source using the official Go image
FROM golang:1.23-alpine AS pdfcpu-builder
WORKDIR /src
ARG PDFCPU_VERSION=v0.11.0
RUN go install github.com/pdfcpu/pdfcpu/cmd/pdfcpu@${PDFCPU_VERSION}


# Stage 2: Build the Java application using Gradle
FROM gradle:8.7-jdk21 AS build
WORKDIR /home/gradle/src
COPY --chown=gradle:gradle . .
RUN gradle build --no-daemon -x test


# Stage 3: Create the final runtime image
FROM eclipse-temurin:21-jre-jammy
WORKDIR /app

# Install runtime dependencies including LibreOffice AND Ghostscript
RUN apt-get update && \
    apt-get install -y \
        libreoffice \
        libreoffice-java-common \
        fonts-liberation2 \
        ghostscript \
        --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

# Copy the application JAR from the 'build' stage
COPY --from=build /home/gradle/src/build/libs/*.jar app.jar

# Copy the compiled pdfcpu binary from the 'pdfcpu-builder' stage
COPY --from=pdfcpu-builder /go/bin/pdfcpu /usr/local/bin/pdfcpu
RUN chmod +x /usr/local/bin/pdfcpu

EXPOSE 8080
ENV SOFFICE_PATH=/usr/bin/soffice

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar app.jar"]