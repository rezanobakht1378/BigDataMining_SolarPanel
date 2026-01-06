FROM apache/spark:3.5.7


ENV SPARK_MODE=client
ENV PYTHONUNBUFFERED=1
ENV HOME=/opt/spark
ENV SPARK_USER_HOME=/opt/spark
ENV IVY_HOME=/opt/spark/.ivy2
ENV HOME=/opt/spark
ENV USER=spark

USER root

RUN pip install --no-cache-dir pyspark kafka-python

RUN mkdir -p /opt/spark/app \
    && mkdir -p /opt/spark/checkpoints \
    && chown -R 1001:1001 /opt/spark \
    && mkdir -p /opt/spark/.ivy2 \
    && chown -R 1001:1001 /opt/spark

USER 1001

COPY aggregate_data.py /opt/spark/app/aggregate_data.py

WORKDIR /opt/spark/app

CMD [ \
  "spark-submit", \
  "--master", "spark://spark-master:7077", \
  "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7", \
  "--conf", "spark.sql.streaming.checkpointLocation=/opt/spark/checkpoints", \
  "--conf", "spark.sql.shuffle.partitions=4", \
  "--conf", "spark.streaming.backpressure.enabled=true", \
  "--conf", "spark.jars.ivy=/opt/spark/.ivy2", \
  "--conf", "spark.hadoop.security.authentication=simple", \
  "--conf", "spark.hadoop.security.authorization=false", \
  "aggregate_data.py" \
]
