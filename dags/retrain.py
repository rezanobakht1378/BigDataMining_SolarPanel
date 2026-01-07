"""Airflow DAG: retrain_model

This DAG calls the FastAPI /retrain endpoint exposed by the realtime_modeling service.

It performs a synchronous retrain request (background=false) so the task fails on retrain errors.
Adjust the `RETRAIN_URL` if your service is reachable at a different host/port.
"""
from __future__ import annotations

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

import requests


# Configuration
RETRAIN_URL = "http://realtime_modeling:8000/retrain"
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def call_retrain(url: str = RETRAIN_URL, **kwargs):
    """Call the retrain HTTP endpoint synchronously and raise on non-2xx response."""
    logging.info("Calling retrain endpoint: %s", url)
    try:
        # Request parameters: background=false to wait for result
        resp = requests.post(url, params={"background": "false"}, timeout=300)
        resp.raise_for_status()
        logging.info("Retrain completed: %s", resp.text)
    except Exception:
        logging.exception("Retrain API call failed")
        raise


with DAG(
    dag_id="retrain_model",
    default_args=DEFAULT_ARGS,
    description="Trigger retraining of the realtime model via HTTP API",
    schedule=timedelta(minutes=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id="call_retrain_api",
        python_callable=call_retrain,
        op_kwargs={"url": RETRAIN_URL},
    )

    t1
