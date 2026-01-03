#!/usr/bin/env python3
import json
import uuid
import time
import math
import random
from datetime import datetime, timezone
from confluent_kafka import Producer

# ---------- تنظیمات ----------
BOOTSTRAP_SERVERS = "localhost:9092"  # مطابق سیستم شما
NUM_PANELS = 3                        # تعداد پنل‌هایی که می‌خواهیم شبیه‌سازی کنیم
INTERVAL_SEC = 2.0                    # فاصله زمانی بین هر چرخه تولید داده (ثانیه)
# نام تاپیک‌ها
TOPICS = {
    "irradiance": "kafka_solar_irradiance",
    "power_ac":   "kafka_solar_power_ac",
    "power_dc":   "kafka_solar_power_dc",
    "temp":       "kafka_solar_temperature",
    "orientation":"kafka_solar_orientation"
}

producer_conf = {"bootstrap.servers": BOOTSTRAP_SERVERS}
producer = Producer(producer_conf)

# ---------- callback برای گزارش تحویل ----------
def delivery_report(err, msg):
    """Callback وقتی پیام ارسال شد یا خطا رخ داد."""
    if err is not None:
        print(f"❌ delivery failed for {msg.topic()}: {err}")
    else:
        # فقط برای دیباگ کوتاه چاپ می‌کنیم (می‌توانید غیرفعال کنید)
        try:
            print(f"✅ delivered to {msg.topic()}: {msg.value().decode('utf-8')}")
        except Exception:
            print(f"✅ delivered to {msg.topic()} (binary payload)")

# ---------- توابع کمکی برای تولید مقادیر فیک ----------
def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

def simulate_irradiance(panel_id, t_seconds):
    """
    مقدار شدت تابش را بر اساس زمانِ روز شبیه‌سازی می‌کند (W/m^2).
    الگوی ساده: سینوس روزانه بین 0 و 1000.
    """
    seconds_per_day = 24*3600
    phase = (t_seconds % seconds_per_day) / seconds_per_day  # 0..1
    # اوج در وسط روز (phase ~ 0.5). amplitude ~1000 W/m2, پایه کمی نویز
    base = max(0, math.sin(math.pi * phase))  # 0..1 (نصف دور سینوسی)
    noise = random.uniform(-50, 50)
    value = max(0.0, base * 1000 + noise)
    return round(value, 2)

def simulate_temperature(irradiance, panel_id):
    """
    دما را از روی شدت تابش و نرخ محیط فرضی محاسبه می‌کنیم.
    """
    ambient = random.uniform(10, 30)  # دمای محیط پایه
    # پنل گرم می‌شود با افزایش irradiance؛ رابطه ساده
    temp = ambient + (irradiance / 1000.0) * random.uniform(20, 40)
    # نویز
    temp += random.uniform(-2, 2)
    return round(temp, 2)

def simulate_power_dc(irradiance, tilt_factor=1.0):
    """
    توان DC به‌صورت خطی با irradiance و یک بهرهٔ فرضی (panel area * efficiency).
    این مقدار وابسته به tilt/azimuth نیست در این نمونه ساده، اما می‌توان آن را گسترش داد.
    """
    # فرض: پنل معادل 1.6 m^2 و راندمان 18% => در 1000 W/m2 حدود 288 W
    panel_effective = 1.6 * 0.18 * tilt_factor
    power = irradiance * panel_effective
    # محدودیت و نویز
    power = max(0.0, power + random.uniform(-20, 20))
    return round(power, 2)

def simulate_power_ac(power_dc):
    """
    مبدل (inverter) کارایی دارد؛ فرض می‌کنیم بین 95% و 98%.
    """
    eff = random.uniform(0.95, 0.98)
    power_ac = power_dc * eff
    # مقداری افت و نویز
    power_ac = max(0.0, power_ac + random.uniform(-5, 5))
    return round(power_ac, 2)

def simulate_orientation(panel_id):
    """
    زاویه tilt (0-90) و azimuth (0-360).
    برای هر پنل مقدار ثابتی در بازه‌ای دارد ولی کمی نوسان دارد (مثلاً برای تنظیم دینامیکی).
    """
    # پایه‌های فرضی برای هر پنل
    base_tilt = 20 + 10 * (panel_id % 3)    # مثال: 20,30,40
    base_azimuth = 180 + 15 * (panel_id % 4)# مثال: 180,195,210,...
    tilt = max(0, min(90, base_tilt + random.uniform(-2, 2)))
    azimuth = (base_azimuth + random.uniform(-5, 5)) % 360
    return {"tilt_deg": round(tilt,2), "azimuth_deg": round(azimuth,2)}

# ---------- حلقهٔ اصلی تولید و ارسال پیام ----------
def run_simulation(num_panels=NUM_PANELS, interval_sec=INTERVAL_SEC):
    print(f"Starting solar-sensor simulator: {num_panels} panels, interval {interval_sec}s")
    try:
        start_time = time.time()
        while True:
            t = time.time()
            elapsed = t - start_time
            # برای هر پنل پیام‌های پنج تاپیک را تولید می‌کنیم
            for panel_idx in range(1, num_panels+1):
                panel_id = f"panel-{panel_idx}"
                ts = utc_now_iso()
                # irradiance شبیه‌سازی شده
                irr = simulate_irradiance(panel_idx, t)
                # orientation
                orient = simulate_orientation(panel_idx)
                # temperature
                temp = simulate_temperature(irr, panel_idx)
                # power dc
                # در tilt پایین/بالا می‌توانیم tilt_factor بسازیم (مثال: tilt به تابش موثر مربوط است)
                tilt_factor = max(0.5, math.cos(math.radians(orient["tilt_deg"])))  # ساده‌شده
                power_dc = simulate_power_dc(irr, tilt_factor)
                # power ac
                power_ac = simulate_power_ac(power_dc)

                # فرم پیام‌ها (هر تاپیک فقط اطلاعات مرتبط را خواهد گرفت)
                msgs = [
                    (TOPICS["irradiance"], {
                        "measurement_id": str(uuid.uuid4()),
                        "panel_id": panel_id,
                        "timestamp": ts,
                        "irradiance_w_m2": irr
                    }),
                    (TOPICS["power_dc"], {
                        "measurement_id": str(uuid.uuid4()),
                        "panel_id": panel_id,
                        "timestamp": ts,
                        "power_dc_w": power_dc
                    }),
                    (TOPICS["power_ac"], {
                        "measurement_id": str(uuid.uuid4()),
                        "panel_id": panel_id,
                        "timestamp": ts,
                        "power_ac_w": power_ac
                    }),
                    (TOPICS["temp"], {
                        "measurement_id": str(uuid.uuid4()),
                        "panel_id": panel_id,
                        "timestamp": ts,
                        "temperature_c": temp
                    }),
                    (TOPICS["orientation"], {
                        "measurement_id": str(uuid.uuid4()),
                        "panel_id": panel_id,
                        "timestamp": ts,
                        "tilt_deg": orient["tilt_deg"],
                        "azimuth_deg": orient["azimuth_deg"]
                    })
                ]

                # ارسال هر پیام به تاپیک مربوطه
                for topic, payload in msgs:
                    value = json.dumps(payload).encode("utf-8")
                    # key می‌تونه panel_id باشه تا پیام‌های یک پنل به پارتیشن مشابه برن (اختیاری)
                    try:
                        producer.produce(topic=topic, key=panel_id, value=value, callback=delivery_report)
                    except BufferError as e:
                        # buffer پر شده؛ بهتره poll کنیم تا فضای آزاد شود
                        print("Local producer queue is full ({}) — calling poll(1)".format(e))
                        producer.poll(1)

            # مهم: اجرا کردن poll تا callbackها فراخوانی شوند
            producer.poll(0)
            # خواب بین چرخه‌ها
            time.sleep(interval_sec)

    except KeyboardInterrupt:
        print("\nInterrupted by user — flushing messages...")
    finally:
        producer.flush()
        print("Producer flushed. Exiting.")

if __name__ == "__main__":
    run_simulation()
