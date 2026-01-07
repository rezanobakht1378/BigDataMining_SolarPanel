# ============================================================
# نام فایل: fault_detector_service.py
# وظیفه: تشخیص پنل‌های معیوب بر اساس انحراف از میانگین و ارسال به کافکا
# ============================================================

import json
import time
import socket
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, avg, abs as spark_abs, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from confluent_kafka import Producer

# --- تنظیمات ---
THRESHOLD_WATTS = 50.0  # اگر پنلی ۵۰ وات کمتر/بیشتر از میانگین بقیه تولید کرد، خراب است
KAFKA_TOPIC_FAULT = "kafka_fault_events"  # نام تاپیک دقیق استخراج شده از PDF
BOOTSTRAP_SERVERS = "localhost:9092"

# تنظیمات پرودیوسر کافکا
producer_conf = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'client.id': socket.gethostname()
}

# تلاش برای اتصال (برای جلوگیری از خطا در کولب)
KAFKA_AVAILABLE = False
try:
    producer = Producer(producer_conf)
    KAFKA_AVAILABLE = True
except Exception:
    print("⚠️ هشدار: کافکا پیدا نشد (حالت شبیه‌سازی).")


def delivery_report(err, msg):
    if err is not None:
        print(f"❌ ارسال خطا شکست خورد: {err}")
    else:
        print(f"🚨 هشدار خرابی ارسال شد به: {msg.topic()}")


# --- راه‌اندازی اسپارک ---
spark = SparkSession.builder \
    .appName("SolarFaultDetector") \
    .master("local[*]") \
    .getOrCreate()

# اسکیما (ورودی)
schema = StructType([
    StructField("panel_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("irradiance_w_m2", DoubleType(), True),
    StructField("power_dc_w", DoubleType(), True),
    # سایر فیلدها...
])


def detect_and_report_faults(df_batch):
    """
    این تابع یک دسته داده (Batch) را می‌گیرد، میانگین‌گیری می‌کند
    و پنل‌های پرت (Outlier) را پیدا می‌کند.
    """
    # ۱. تعریف پنجره زمانی: میانگین‌گیری بر اساس Timestamp
    # (یعنی تمام پنل‌هایی که در یک لحظه گزارش داده‌اند را با هم مقایسه کن)
    window_spec = Window.partitionBy("timestamp")

    # ۲. محاسبه میانگین و اختلاف
    analyzed_df = df_batch \
        .withColumn("avg_power", avg("power_dc_w").over(window_spec)) \
        .withColumn("deviation", spark_abs(col("power_dc_w") - col("avg_power")))

    # ۳. فیلتر کردن پنل‌های خراب (انحراف > آستانه)
    faulty_panels_df = analyzed_df.filter(col("deviation") > THRESHOLD_WATTS)

    # اگر داده‌ای پیدا شد، پرینت کن و بفرست به کافکا
    count = faulty_panels_df.count()
    if count > 0:
        print(f"\n⚠️ تشخیص {count} پنل مشکوک به خرابی!")
        faulty_panels_df.select("panel_id", "power_dc_w", "avg_power", "deviation").show()

        # تبدیل به لیست پایتون برای ارسال به کافکا
        rows = faulty_panels_df.collect()
        for row in rows:
            alert_payload = {
                "event_type": "FAULT_DETECTED",
                "panel_id": row["panel_id"],
                "timestamp": row["timestamp"].isoformat(),
                "details": {
                    "current_power": row["power_dc_w"],
                    "average_others": row["avg_power"],
                    "deviation": row["deviation"],
                    "threshold": THRESHOLD_WATTS
                },
                "action_required": "Inspect Panel Immediately"
            }

            # ارسال به کافکا
            msg_val = json.dumps(alert_payload)
            if KAFKA_AVAILABLE:
                producer.produce(
                    KAFKA_TOPIC_FAULT,
                    key=row["panel_id"],
                    value=msg_val.encode('utf-8'),
                    callback=delivery_report
                )
                producer.poll(0)
            else:
                print(f"📡 [شبیه‌سازی ارسال به {KAFKA_TOPIC_FAULT}]: {msg_val}")

    else:
        print("✅ وضعیت همه پنل‌ها نرمال است.")

    if KAFKA_AVAILABLE:
        producer.flush()


# ============================================================
# بخش تست (اجرا در گوگل کولب)
# ============================================================
if __name__ == "__main__":
    from datetime import datetime

    # تولید داده تستی: ۳ پنل، یکی سالم، یکی سالم، یکی خراب
    print("🧪 شروع تست تشخیص خرابی...")
    mock_data = [
        # پنل ۱ و ۲ نزدیک به هم (سالم)
        ("panel-1", datetime(2024, 1, 1, 12, 0), 900.0, 250.0),
        ("panel-2", datetime(2024, 1, 1, 12, 0), 900.0, 255.0),
        # پنل ۳ خیلی کم تولید می‌کند (خراب - مثلاً سایه افتاده یا سوخته)
        ("panel-3", datetime(2024, 1, 1, 12, 0), 900.0, 50.0),
    ]

    df_test = spark.createDataFrame(mock_data, ["panel_id", "timestamp", "irradiance_w_m2", "power_dc_w"])

    print("📊 داده‌های ورودی:")
    df_test.show()

    # اجرای تابع تشخیص
    detect_and_report_faults(df_test)