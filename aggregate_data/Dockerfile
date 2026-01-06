# Use official Spark image
FROM apache/spark:3.5.7

# ----------------------------
# Set environment variables
# ----------------------------
ENV SPARK_MODE=client
ENV PYTHONUNBUFFERED=1
ENV HOME=/opt/spark
ENV SPARK_USER_HOME=/opt/spark
ENV IVY_HOME=/opt/spark/.ivy2
ENV HADOOP_USER_NAME=spark
ENV JAVA_TOOL_OPTIONS="-Dcom.sun.security.auth.useFirstPass=true -Dhadoop.security.authentication=simple"

# ----------------------------
# Install Python dependencies
# ----------------------------
USER root
RUN pip install --no-cache-dir pyspark kafka-python

# ----------------------------
# Create spark user and directories
# ----------------------------
RUN useradd -m -u 1001 -s /bin/bash spark \
    && mkdir -p /opt/spark/{app,checkpoints,.ivy2} \
    && chown -R spark:spark /opt/spark

# Switch to spark user
USER spark
WORKDIR /opt/spark/app

# ----------------------------
# Copy application
# ----------------------------
COPY aggregate_data.py /opt/spark/app/

# ----------------------------
# Spark-submit command
# ----------------------------
CMD ["spark-submit",\
     "--master", "spark://spark-master:7077",\
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7",\
     "--conf", "spark.sql.streaming.checkpointLocation=/opt/spark/checkpoints",\
     "--conf", "spark.sql.shuffle.partitions=4",\
     "--conf", "spark.streaming.backpressure.enabled=true",\
     "--conf", "spark.jars.ivy=/opt/spark/.ivy2",\
     "--conf", "spark.hadoop.security.authentication=simple",\
     "aggregate_data.py"
]
