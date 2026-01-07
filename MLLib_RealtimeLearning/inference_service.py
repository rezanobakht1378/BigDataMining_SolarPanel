# ============================================================
# نام فایل: inference_service.py
# وظیفه: دریافت داده، محاسبه زاویه بهینه و ارسال فرمان به کافکا
# ============================================================

# ۱. نصب کتابخانه‌ها (اگر در کولب هستید خط زیر را اجرا کنید)
# !pip install pyspark confluent_kafka

import time
import json
import random
import math
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import hour, dayofyear, col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from confluent_kafka import Producer
import socket

# ------------------------------------------------------------
# تنظیمات کافکا (Kafka Config)
# ------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"  # در سیستم اصلی به kafka:9092 تغییر دهید
CONTROL_TOPIC = "kafka_solar_control"  # تاپیک فرمان‌های کنترلی

# تنظیمات پرودیوسر
producer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'client.id': socket.gethostname()
}

# تلاش برای اتصال به کافکا (با مدیریت خطا برای اجرا در کولب)
try:
    producer = Producer(producer_conf)
    KAFKA_AVAILABLE = True
except Exception as e:
    print(f"⚠️ هشدار: کافکا در دسترس نیست (حالت شبیه‌سازی لوکال). خطا: {e}")
    KAFKA_AVAILABLE = False


def delivery_report(err, msg):
    """کال‌بک برای اطمینان از ارسال پیام به کافکا"""
    if err is not None:
        print(f"❌ ارسال ناموفق به کافکا: {err}")
    else:
        print(f"✅ فرمان ارسال شد به: {msg.topic()} [{msg.partition()}]")


# ------------------------------------------------------------
# راه‌اندازی اسپارک و مدل‌سازی
# ------------------------------------------------------------
spark = SparkSession.builder \
    .appName("SolarInferenceService") \
    .master("local[*]") \
    .getOrCreate()

# تعریف اسکیما
schema = StructType([
    StructField("panel_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("irradiance_w_m2", DoubleType(), True),
    StructField("power_ac_w", DoubleType(), True),
    StructField("power_dc_w", DoubleType(), True),
    StructField("temperature_c", DoubleType(), True),
    StructField("tilt_deg", DoubleType(), True),
    StructField("azimuth_deg", DoubleType(), True)
])


# --- فاز ۱: آموزش مدل (در سیستم واقعی مدل از قبل ذخیره و لود می‌شود) ---
def train_mock_model():
    """یک مدل موقت آموزش می‌دهد تا بتوانیم استنتاج کنیم"""
    print("⏳ در حال آموزش مدل هوشمند...")
    data = []
    start = datetime(2024, 1, 1, 8, 0)
    for i in range(500):
        irr = random.uniform(200, 1000)
        tilt = random.uniform(0, 90)
        # فرمول فرضی: زاویه ۳۰ درجه بهترین است
        factor = math.cos(math.radians(tilt - 30))
        power = (irr * 0.2 * factor) if factor > 0 else 0
        data.append(("p1", start, irr, power * 0.9, power, 25.0, tilt, 180.0))

    df = spark.createDataFrame(data, schema=schema)
    df = df.withColumn("hour", hour("timestamp"))

    assembler = VectorAssembler(
        inputCols=["irradiance_w_m2", "temperature_c", "hour", "tilt_deg", "azimuth_deg"],
        outputCol="features"
    )
    train_data = assembler.transform(df)

    gbt = GBTRegressor(featuresCol="features", labelCol="power_dc_w", maxIter=10)
    return gbt.fit(train_data), assembler


# آموزش مدل و دریافت آن
model, assembler = train_mock_model()
print("✅ مدل آماده است.")


# ------------------------------------------------------------
# فاز ۲: توابع استنتاج (Inference Logic)
# ------------------------------------------------------------
def find_optimal_tilt(irradiance, temp, current_time):
    """پیدا کردن بهترین زاویه برای شرایط فعلی"""
    candidates = []
    current_hour = current_time.hour

    # تست زوایا از ۰ تا ۶۰ درجه با قدم ۵ تایی
    for t in range(0, 65, 5):
        candidates.append((float(irradiance), float(temp), int(current_hour), float(t), 180.0))

    schema_cand = ["irradiance_w_m2", "temperature_c", "hour", "tilt_deg", "azimuth_deg"]
    df_cand = spark.createDataFrame(candidates, schema=schema_cand)

    # پیش‌بینی
    vec_df = assembler.transform(df_cand)
    preds = model.transform(vec_df)

    # انتخاب بهترین زاویه (بیشترین prediction)
    best_row = preds.orderBy(col("prediction").desc()).first()
    return best_row['tilt_deg'], best_row['prediction']


# ------------------------------------------------------------
# فاز ۳: حلقه اجرایی (Main Loop)
# ------------------------------------------------------------
def run_inference_service():
    print("\n🚀 سرویس استنتاج و کنترل فعال شد (Press Ctrl+C to stop)...")

    try:
        while True:
            # ۱. شبیه‌سازی دریافت داده جدید از سنسورها
            # (در پروژه واقعی، این بخش داده را از تاپیک سنسورها می‌خواند)
            current_time = datetime.now()
            live_irr = random.uniform(800, 1000)  # فرض: روز آفتابی
            live_temp = 30.0
            panel_id = "panel-1"

            print(f"\n📥 دریافت داده سنسور: {panel_id} | تابش: {live_irr:.1f}")

            # ۲. محاسبه زاویه بهینه
            optimal_tilt, predicted_power = find_optimal_tilt(live_irr, live_temp, current_time)

            print(f"🧠 تحلیل هوش مصنوعی: زاویه بهینه {optimal_tilt} درجه است.")

            # ۳. ساخت پیام فرمان (Command Payload)
            control_message = {
                "message_id": str(time.time()),
                "panel_id": panel_id,
                "command_type": "SET_ANGLE",
                "target_tilt": optimal_tilt,
                "target_azimuth": 180.0,  # فعلاً ثابت
                "reason": "Maximized power output",
                "timestamp": current_time.isoformat()
            }

            # ۴. ارسال به کافکا
            json_payload = json.dumps(control_message)

            if KAFKA_AVAILABLE:
                producer.produce(
                    TOPIC=CONTROL_TOPIC,
                    key=panel_id,
                    value=json_payload.encode('utf-8'),
                    callback=delivery_report
                )
                producer.poll(0)  # تریگر کردن کال‌بک‌ها
            else:
                # فقط چاپ در کنسول (برای حالت تست کولب)
                print(f"📡 [شبیه‌سازی ارسال کافکا]: {json_payload}")

            # صبر برای سیکل بعدی (مثلاً هر ۵ ثانیه)
            time.sleep(5)

    except KeyboardInterrupt:
        print("\n🛑 سرویس متوقف شد.")
    finally:
        if KAFKA_AVAILABLE:
            producer.flush()


if __name__ == "__main__":
    run_inference_service()