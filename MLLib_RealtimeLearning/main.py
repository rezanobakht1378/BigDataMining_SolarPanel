from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime
import time
import json
import socket
import random
import math

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


def create_spark_session():
    global _spark
    if _spark is None:
        _spark = SparkSession.builder.appName("SolarInferenceAPI").master("local[*]").getOrCreate()
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
