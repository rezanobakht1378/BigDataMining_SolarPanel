FROM apache/spark:3.5.7

# Install Python dependencies
USER root
RUN pip install --no-cache-dir kafka-python

# Create directories and set permissions
RUN mkdir -p /opt/spark/app /opt/spark/checkpoints \
    && chown -R 185:185 /opt/spark

# Copy your application
COPY aggregate_data.py /opt/spark/app/

USER 185

WORKDIR /opt/spark/app

# Use shell form for CMD to ensure PATH is set
CMD /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 \
    --conf spark.sql.streaming.checkpointLocation=/opt/spark/checkpoints \
    --conf spark.sql.shuffle.partitions=4 \
    --conf spark.streaming.backpressure.enabled=true \
    --conf spark.jars.ivy=/opt/spark/.ivy2 \
    --conf spark.hadoop.security.authentication=simple \
    --conf spark.hadoop.security.authorization=false \
    --conf spark.driver.extraJavaOptions="-Djava.security.krb5.realm= -Djava.security.krb5.kdc=none -Dhadoop.security.authentication=simple" \
    --conf spark.executor.extraJavaOptions="-Djava.security.krb5.realm= -Djava.security.krb5.kdc=none -Dhadoop.security.authentication=simple" \
    aggregate_data.py