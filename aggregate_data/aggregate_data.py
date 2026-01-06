from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, expr, avg

# ۱. ایجاد جلسه اسپارک با پکیج‌های مورد نیاز کافکا
spark = SparkSession.builder \
    .appName("SolarPanelStreamingProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ۲. تعریف اسکیما برای داده‌های JSON (مطابق با خروجی producer.py)
# توجه: فیلد timestamp برای پردازش Event-time حیاتی است
base_schema = "panel_id STRING, timestamp TIMESTAMP"
irradiance_schema = base_schema + ", irradiance_w_m2 DOUBLE"
power_ac_schema = base_schema + ", power_ac_w DOUBLE"
power_dc_schema = base_schema + ", power_dc_w DOUBLE"
temp_schema = base_schema + ", temperature_c DOUBLE"
orient_schema = base_schema + ", tilt_deg DOUBLE, azimuth_deg DOUBLE"

def read_from_kafka(topic, schema):
    """تابع کمکی برای خواندن از هر تاپیک و اعمال اسکیما"""
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load() \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .withWatermark("timestamp", "10 seconds") # اجازه ۱۰ ثانیه تاخیر برای همگام‌سازی

# ۳. ایجاد استریم برای هر ۵ تاپیک [cite: 32, 33, 34]
df_irr   = read_from_kafka("kafka_solar_irradiance", irradiance_schema)
df_ac    = read_from_kafka("kafka_solar_power_ac", power_ac_schema)
df_dc    = read_from_kafka("kafka_solar_power_dc", power_dc_schema)
df_temp  = read_from_kafka("kafka_solar_temperature", temp_schema)
df_orient = read_from_kafka("kafka_solar_orientation", orient_schema)

# ۴. انجام Join‌های متوالی (Stream-to-Stream Join)
# ما تمام استریم‌ها را بر اساس panel_id و timestamp با هم یکی می‌کنیم
joined_df = df_irr \
    .join(df_ac, ["panel_id", "timestamp"]) \
    .join(df_dc, ["panel_id", "timestamp"]) \
    .join(df_temp, ["panel_id", "timestamp"]) \
    .join(df_orient, ["panel_id", "timestamp"])

# ۵. مهندسی ویژگی (Feature Engineering) [cite: 45]
# محاسبه راندمان و سایر ویژگی‌های مورد نیاز مدل [cite: 47, 48]
enriched_df = joined_df \
    .withColumn(
        "efficiency",
        expr("CASE WHEN power_dc_w > 0 THEN (power_ac_w / power_dc_w) * 100 ELSE NULL END")
    ) \
    .withColumn(
        "temp_per_power",
        expr("CASE WHEN power_dc_w > 0 THEN temperature_c / power_dc_w ELSE NULL END")
    )

enriched_query = enriched_df \
    .selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "172.17.0.1:9092") \
    .option("topic", "kafka_solar_enriched_features") \
    .option("checkpointLocation", "/opt/spark/checkpoints/enriched") \
    .outputMode("append") \
    .start()


# ۶. تحلیل در پنجره‌های زمانی ۱ دقیقه‌ای (Windowing) [cite: 79, 80]
# این بخش میانگین ویژگی‌ها را در هر دقیقه برای هر پنل محاسبه می‌کند
windowed_stats = enriched_df \
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("panel_id")
    ) \
    .agg(
        avg("efficiency").alias("avg_efficiency"),
        avg("temperature_c").alias("avg_temp"),
        avg("irradiance_w_m2").alias("avg_irradiance")
    )

# ۷. خروجی موقت در کنسول برای تست
query = windowed_stats \
    .selectExpr(
        "to_json(struct(" +
        "panel_id, " +
        "window.start as window_start, " +
        "window.end as window_end, " +
        "avg_efficiency, avg_temp, avg_irradiance" +
        ")) AS value"
    ) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "172.17.0.1:9092") \
    .option("topic", "kafka_solar_window_stats") \
    .option("checkpointLocation", "/opt/spark/checkpoints/window_stats") \
    .outputMode("update") \
    .start()

spark.streams.awaitAnyTermination()