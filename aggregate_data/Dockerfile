FROM apache/spark:3.5.7

# Switch to root to install dependencies
USER root

# Install Python dependencies
RUN pip install --no-cache-dir kafka-python

# Create working directories
RUN mkdir -p /opt/bitnami/spark/app \
    && mkdir -p /opt/bitnami/spark/checkpoints \
    && chown -R 1001:1001 /opt/bitnami/spark

# Copy your application
COPY aggregate_data.py /opt/bitnami/spark/app/

# Switch to non-root user
USER 1001

WORKDIR /opt/bitnami/spark/app

# Command to run Spark with simplified configuration
CMD [ \
  "spark-submit", \
  "--master", "spark://spark-master:7077", \
  "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7", \
  "--conf", "spark.sql.streaming.checkpointLocation=/opt/bitnami/spark/checkpoints", \
  "--conf", "spark.sql.shuffle.partitions=4", \
  "--conf", "spark.streaming.backpressure.enabled=true", \
  "--conf", "spark.jars.ivy=/tmp/.ivy2", \
  "--conf", "spark.hadoop.security.authentication=simple", \
  "--conf", "spark.hadoop.security.authorization=false", \
  "--conf", "spark.driver.extraJavaOptions=-Djava.security.krb5.realm= -Djava.security.krb5.kdc=none -Dhadoop.security.authentication=simple -Dhadoop.security.authorization=false", \
  "--conf", "spark.executor.extraJavaOptions=-Djava.security.krb5.realm= -Djava.security.krb5.kdc=none -Dhadoop.security.authentication=simple -Dhadoop.security.authorization=false", \
  "--conf", "spark.hadoop.fs.defaultFS=file:///", \
  "--conf", "spark.yarn.appMasterEnv.HADOOP_USER_NAME=spark", \
  "--conf", "spark.executorEnv.HADOOP_USER_NAME=spark", \
  "--conf", "spark.driverEnv.HADOOP_USER_NAME=spark", \
  "aggregate_data.py" \
]