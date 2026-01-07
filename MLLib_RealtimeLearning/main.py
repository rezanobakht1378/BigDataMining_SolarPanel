from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime
import os
import time
import json
import socket
import random
import math
import threading

from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor

try:
    from confluent_kafka import Producer
    _HAS_KAFKA = True
except Exception:
    Producer = None
    _HAS_KAFKA = False


# ---- Configuration ----
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
CONTROL_TOPIC = "kafka_solar_control"


class PredictRequest(BaseModel):
    panel_id: Optional[str] = Field(default="panel-1")
    irradiance_w_m2: float = Field(..., gt=0)
    temperature_c: float = Field(...)
    timestamp: Optional[datetime] = None
    send_to_kafka: Optional[bool] = False


class PredictResponse(BaseModel):
    panel_id: str
    optimal_tilt: float
    predicted_power: float
    timestamp: str
    kafka_sent: bool = False


app = FastAPI(title="Solar Inference Service (FastAPI)")


# Globals to hold Spark/model objects
_spark = None
_assembler = None
_model = None
_producer = None
_retrain_lock = threading.Lock()


def create_spark_session():
    global _spark
    if _spark is None:
        # Allow overriding the spark master via environment variable (useful in docker-compose)
        master = os.environ.get("SPARK_MASTER", "local[*]")
        print(f"Creating SparkSession with master={master}")
        _spark = SparkSession.builder.appName("SolarInferenceAPI").master(master).getOrCreate()
    return _spark


def train_mock_model(spark):
    """Train a small mock GBT model for demo purposes (same logic as previous script)."""
    data = []
    start = datetime(2024, 1, 1, 8, 0)
    for i in range(500):
        irr = random.uniform(200, 1000)
        tilt = random.uniform(0, 90)
        factor = math.cos(math.radians(tilt - 30))
        power = (irr * 0.2 * factor) if factor > 0 else 0
        data.append(("p1", start, irr, power * 0.9, power, 25.0, tilt, 180.0))

    schema = ["panel_id", "timestamp", "irradiance_w_m2", "power_ac_w", "power_dc_w", "temperature_c", "tilt_deg", "azimuth_deg"]
    df = spark.createDataFrame(data, schema=schema)
    df = df.withColumn("hour", hour("timestamp"))

    assembler = VectorAssembler(
        inputCols=["irradiance_w_m2", "temperature_c", "hour", "tilt_deg", "azimuth_deg"],
        outputCol="features"
    )
    train_data = assembler.transform(df)

    gbt = GBTRegressor(featuresCol="features", labelCol="power_dc_w", maxIter=10)
    model = gbt.fit(train_data)
    return model, assembler


def init_kafka_producer():
    global _producer
    if not _HAS_KAFKA:
        return None
    if _producer is None:
        conf = {"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS, "client.id": socket.gethostname()}
        try:
            _producer = Producer(conf)
        except Exception:
            _producer = None
    return _producer


def delivery_report(err, msg):
    if err is not None:
        app.logger = getattr(app, "logger", None)
        print(f"Kafka delivery failed: {err}")
    else:
        print(f"Kafka message delivered to {msg.topic()} [{msg.partition()}]")


def find_optimal_tilt(irradiance, temp, current_time):
    global _spark, _assembler, _model
    candidates = []
    current_hour = current_time.hour
    for t in range(0, 65, 5):
        candidates.append((float(irradiance), float(temp), int(current_hour), float(t), 180.0))

    schema_cand = ["irradiance_w_m2", "temperature_c", "hour", "tilt_deg", "azimuth_deg"]
    df_cand = _spark.createDataFrame(candidates, schema=schema_cand)
    vec_df = _assembler.transform(df_cand)
    preds = _model.transform(vec_df)
    best_row = preds.orderBy(col("prediction").desc()).first()
    return float(best_row['tilt_deg']), float(best_row['prediction'])


@app.on_event("startup")
def startup_event():
    global _spark, _model, _assembler, _producer
    _spark = create_spark_session()
    _model, _assembler = train_mock_model(_spark)
    if _HAS_KAFKA:
        _producer = init_kafka_producer()
    print("Startup complete: model trained and ready.")


@app.get("/health")
def health():
    return {"status": "ok", "model_loaded": _model is not None}


def _send_control_to_kafka(payload: dict):
    if not _HAS_KAFKA or _producer is None:
        print("Kafka not available, skipping send.")
        return False
    try:
        _producer.produce(topic=CONTROL_TOPIC, key=payload.get("panel_id"), value=json.dumps(payload).encode("utf-8"), callback=delivery_report)
        _producer.poll(0)
        return True
    except Exception as e:
        print(f"Failed to send to Kafka: {e}")
        return False


def _consume_kafka_messages(topic: str, max_messages: int = 500, timeout: int = 15):
    """Consume up to max_messages from Kafka topic and return a list of parsed records.
    Each record is expected to be a JSON object matching the training schema.
    """
    if not _HAS_KAFKA:
        raise RuntimeError("confluent_kafka not available")

    try:
        from confluent_kafka import Consumer
    except Exception as e:
        raise RuntimeError(f"Failed to import Consumer: {e}")

    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": f"retrainer-{int(time.time())}",
        "auto.offset.reset": "earliest",
    }
    consumer = Consumer(conf)
    consumer.subscribe([topic])

    start = time.time()
    records = []
    try:
        while len(records) < max_messages and (time.time() - start) < timeout:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Kafka consumer error: {msg.error()}")
                continue
            try:
                payload = json.loads(msg.value().decode("utf-8"))
            except Exception as e:
                print(f"Failed to parse message: {e}")
                continue

            # map payload to expected schema fields (best-effort)
            try:
                panel_id = payload.get("panel_id", "p1")
                ts = payload.get("timestamp")
                if isinstance(ts, str):
                    try:
                        ts_val = datetime.fromisoformat(ts)
                    except Exception:
                        ts_val = datetime.now()
                else:
                    ts_val = datetime.now()
                irr = float(payload.get("irradiance_w_m2", payload.get("irradiance", 0)))
                power_dc = float(payload.get("power_dc_w", payload.get("power", 0)))
                power_ac = float(payload.get("power_ac_w", power_dc * 0.9 if power_dc else 0))
                temp = float(payload.get("temperature_c", payload.get("temp", 25.0)))
                tilt = float(payload.get("tilt_deg", payload.get("tilt", 30.0)))
                az = float(payload.get("azimuth_deg", payload.get("azimuth", 180.0)))

                records.append((panel_id, ts_val, irr, power_ac, power_dc, temp, tilt, az))
            except Exception as e:
                print(f"Skipping message due to mapping error: {e}")
                continue
    finally:
        try:
            consumer.close()
        except Exception:
            pass

    return records


def retrain_from_kafka(topic: str, max_messages: int = 500, timeout: int = 15):
    """Consume training data from Kafka and retrain the GBT model in-place.
    Returns a dict with summary info.
    """
    global _model, _assembler, _spark

    if not _HAS_KAFKA:
        raise RuntimeError("Kafka client not available in this environment")

    # Prevent concurrent retrains
    if not _retrain_lock.acquire(blocking=False):
        return {"status": "busy", "message": "Retrain already in progress"}

    try:
        # ensure spark session
        _spark = create_spark_session()

        recs = _consume_kafka_messages(topic, max_messages=max_messages, timeout=timeout)
        if not recs:
            return {"status": "no_data", "num_records": 0}

        schema = ["panel_id", "timestamp", "irradiance_w_m2", "power_ac_w", "power_dc_w", "temperature_c", "tilt_deg", "azimuth_deg"]
        df = _spark.createDataFrame(recs, schema=schema)
        df = df.withColumn("hour", hour("timestamp"))

        assembler = VectorAssembler(
            inputCols=["irradiance_w_m2", "temperature_c", "hour", "tilt_deg", "azimuth_deg"],
            outputCol="features"
        )
        train_data = assembler.transform(df)

        gbt = GBTRegressor(featuresCol="features", labelCol="power_dc_w", maxIter=10)
        model = gbt.fit(train_data)

        # replace global model safely
        _model = model
        _assembler = assembler

        return {"status": "retrained", "num_records": len(recs)}
    finally:
        _retrain_lock.release()


@app.post("/retrain")
def retrain(background_tasks: BackgroundTasks, topic: Optional[str] = None, max_messages: int = 500, timeout: int = 15, background: bool = True):
    """Trigger retraining using data read from Kafka.

    Query/body params:
    - topic: Kafka topic to read training data from (defaults to env KAFKA_SENSOR_TOPIC or 'kafka_solar_sensors')
    - max_messages: maximum messages to consume
    - timeout: seconds to wait for messages
    - background: if true, start retrain in background and return immediately
    """
    if not _HAS_KAFKA:
        raise HTTPException(status_code=503, detail="Kafka client not available")

    topic = topic or os.environ.get("KAFKA_SENSOR_TOPIC", "kafka_solar_sensors")

    if background:
        background_tasks.add_task(retrain_from_kafka, topic, max_messages, timeout)
        return {"status": "started", "topic": topic}
    else:
        try:
            res = retrain_from_kafka(topic, max_messages, timeout)
            return res
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


@app.post("/predict", response_model=PredictResponse)
def predict(req: PredictRequest, background_tasks: BackgroundTasks):
    if _model is None or _assembler is None or _spark is None:
        raise HTTPException(status_code=503, detail="Model not loaded yet")

    current_time = req.timestamp or datetime.now()

    optimal_tilt, predicted_power = find_optimal_tilt(req.irradiance_w_m2, req.temperature_c, current_time)

    control_message = {
        "message_id": str(time.time()),
        "panel_id": req.panel_id,
        "command_type": "SET_ANGLE",
        "target_tilt": optimal_tilt,
        "target_azimuth": 180.0,
        "reason": "Maximized power output",
        "timestamp": current_time.isoformat()
    }

    kafka_sent = False
    if req.send_to_kafka:
        # send in background so request is fast
        background_tasks.add_task(_send_control_to_kafka, control_message)
        kafka_sent = True

    return PredictResponse(
        panel_id=req.panel_id,
        optimal_tilt=optimal_tilt,
        predicted_power=predicted_power,
        timestamp=current_time.isoformat(),
        kafka_sent=kafka_sent
    )
