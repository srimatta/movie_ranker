# First stage: Build the application
FROM openjdk:8-jdk AS builder

# Set the working directory
WORKDIR /app

# Install necessary dependencies
RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Install SBT manually
ENV SBT_VERSION=1.3.13
RUN curl -L -o sbt.zip https://github.com/sbt/sbt/releases/download/v${SBT_VERSION}/sbt-${SBT_VERSION}.zip && \
    unzip sbt.zip -d /opt && \
    rm sbt.zip

ENV PATH=$PATH:/opt/sbt/bin

# Verify SBT installation
RUN sbt sbtVersion

# Copy the entire project into the container
COPY . /app

# Build the application using SBT
RUN sbt clean assembly

# Second stage: Create the final image
FROM openjdk:8-jre

# Set environment variables for Spark
ENV SPARK_VERSION=3.5.3
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Install wget and curl
RUN apt-get update && apt-get install -y wget curl && \
    rm -rf /var/lib/apt/lists/*

# Download and extract Spark
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar xvf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Set the working directory
WORKDIR /app

# Copy the built JAR from the builder stage
COPY --from=builder /app/target/scala-2.12/movie_ranker-assembly-1.0.jar /app/movie_ranker-assembly-1.0.jar

# Copy the IMDb data files
COPY imdb_data /app/imdb_data

# Define the default command to run the application
CMD ["spark-submit", "--class", "com.imdb.MovieRankerMain", "--master", "local[*]", "/app/movie_ranker-assembly-1.0.jar"]
