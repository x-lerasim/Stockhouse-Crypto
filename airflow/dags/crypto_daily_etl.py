from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timezone, timedelta
import logging
import os
import json
import requests
import boto3
from botocore.exceptions import BotoCoreError, ClientError
import time

SPARK_JOBS_PATH = '/opt/spark/jobs'
DBT_PROJECT_DIR = '/usr/app/crypto_analytics'  
DBT_PROFILES_DIR = '/home/dbt/.dbt'

default_args = {
    'owner': 'x-lerasim',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

logger = logging.getLogger(__name__)


def _day_bounds_ms(day_str: str):
    day_start = datetime.strptime(day_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    next_day_start = day_start + timedelta(days=1)
    start_ms = int(day_start.timestamp() * 1000)
    end_ms = int(next_day_start.timestamp() * 1000) - 1
    return start_ms, end_ms


def _fetch_assets_page(base_url: str, headers: dict, limit: int, offset: int):
    max_retries = int(os.environ.get("COINCAP_REQUEST_RETRIES", "3"))
    backoff_base = float(os.environ.get("COINCAP_REQUEST_BACKOFF_SEC", "1"))
    last_exc = None

    for attempt in range(max_retries):
        try:
            response = requests.get(
                base_url,
                headers=headers,
                params={"limit": limit, "offset": offset},
                timeout=30,
            )
            response.raise_for_status()
            payload = response.json()
            return payload.get("data", [])
        except requests.RequestException as exc:
            last_exc = exc
            if attempt == max_retries - 1:
                break
            time.sleep(backoff_base * (2 ** attempt))

    raise last_exc


def _fetch_all_assets(base_url: str, headers: dict, page_size: int):
    offset = 0
    all_assets = []
    while True:
        try:
            page = _fetch_assets_page(base_url, headers, page_size, offset)
        except requests.RequestException as exc:
            if all_assets:
                logger.warning(
                    "Failed to fetch assets page at offset=%s after retries, continuing with %s assets: %s",
                    offset,
                    len(all_assets),
                    exc,
                )
                break
            raise
        if not page:
            break
        all_assets.extend(page)
        if len(page) < page_size:
            break
        offset += page_size
    return all_assets


def _fetch_asset_day_metrics(base_url: str, headers: dict, asset_id: str, start_ms: int, end_ms: int):
    history_url = f"{base_url}/{asset_id}/history"
    max_retries = int(os.environ.get("COINCAP_REQUEST_RETRIES", "3"))
    backoff_base = float(os.environ.get("COINCAP_REQUEST_BACKOFF_SEC", "1"))
    points = []

    for attempt in range(max_retries):
        try:
            response = requests.get(
                history_url,
                headers=headers,
                params={"interval": "h1", "start": start_ms, "end": end_ms},
                timeout=30,
            )
            response.raise_for_status()
            points = response.json().get("data", [])
            break
        except requests.RequestException:
            if attempt == max_retries - 1:
                raise
            time.sleep(backoff_base * (2 ** attempt))

    if not points:
        return None

    first_point = points[0]
    last_point = points[-1]
    first_price = float(first_point.get("priceUsd", 0) or 0)
    last_price = float(last_point.get("priceUsd", 0) or 0)
    if last_price <= 0:
        return None

    change_24h = None
    if first_price > 0:
        change_24h = ((last_price - first_price) / first_price) * 100

    circulating_supply = float(last_point.get("circulatingSupply", 0) or 0)
    market_cap = last_price * circulating_supply if circulating_supply > 0 else None

    return {
        "priceUsd": str(last_price),
        "changePercent24Hr": str(change_24h) if change_24h is not None else None,
        "marketCapUsd": str(market_cap) if market_cap is not None else None,
    }


def fetch_assets_to_raw(execution_date: str, **kwargs):
    base_url = "https://rest.coincap.io/v3/assets"
    api_key = os.environ.get("COINCAP_API_KEY")
    if not api_key:
        raise ValueError("COINCAP_API_KEY environment variable is not set")

    page_size = int(os.environ.get("COINCAP_PAGE_SIZE", "200"))
    sleep_sec = float(os.environ.get("COINCAP_REQUEST_SLEEP_SEC", "0.1"))
    max_assets = int(os.environ.get("COINCAP_MAX_ASSETS", "300"))
    start_ms, end_ms = _day_bounds_ms(execution_date)

    headers = {"Authorization": f"Bearer {api_key}"}
    assets = _fetch_all_assets(base_url, headers, page_size)
    if max_assets > 0 and len(assets) > max_assets:
        logger.info("Limiting assets from %s to %s for this run", len(assets), max_assets)
        assets = assets[:max_assets]

    day_snapshot = []
    for asset in assets:
        asset_id = asset.get("id")
        if not asset_id:
            continue
        try:
            metrics = _fetch_asset_day_metrics(base_url, headers, asset_id, start_ms, end_ms)
        except requests.RequestException as exc:
            logger.warning("Failed history request for asset_id=%s: %s", asset_id, exc)
            continue

        if not metrics:
            continue

        day_snapshot.append(
            {
                "id": asset.get("id"),
                "symbol": asset.get("symbol"),
                "name": asset.get("name"),
                "rank": asset.get("rank"),
                "priceUsd": metrics["priceUsd"],
                "changePercent24Hr": metrics["changePercent24Hr"],
                "marketCapUsd": metrics["marketCapUsd"],
            }
        )
        if sleep_sec > 0:
            time.sleep(sleep_sec)

    payload = {"timestamp": end_ms, "data": day_snapshot}

    if "data" not in payload:
        raise ValueError("CoinCap response does not contain 'data' field")

    s3_client = boto3.client(
        "s3",
        endpoint_url=os.environ.get("AWS_ENDPOINT_URL", "http://minio:9000"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "admin"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "admin123"),
    )

    object_key = f"assets/load_date={execution_date}/assets.json"
    try:
        s3_client.put_object(
            Bucket="raw",
            Key=object_key,
            Body=json.dumps(payload).encode("utf-8"),
            ContentType="application/json",
        )
    except (BotoCoreError, ClientError) as exc:
        logger.exception("Failed to upload raw API payload to MinIO")
        raise exc

    logger.info(
        "Stored %s assets to s3a://raw/%s",
        len(payload.get("data", [])),
        object_key,
    )

with DAG(
    dag_id='crypto_daily_etl',
    default_args=default_args,
    start_date=datetime(2026, 2, 15),
    schedule='0 9 * * *',
    catchup=False,
    max_active_runs=2,
    tags=['crypto', 'spark', 'clickhouse'],
) as dag:
    
    ingest_bronze = PythonOperator(
        task_id='ingest_bronze',
        python_callable=fetch_assets_to_raw,
        op_kwargs={"execution_date": "{{ macros.ds_add(ds, -1) }}"},
        execution_timeout=timedelta(minutes=45),
    )

    process_silver = SparkSubmitOperator(
        task_id='process_silver',
        application=f'{SPARK_JOBS_PATH}/dim_assets_slv.py',
        conn_id='spark_default',
        conf={'spark.master': 'spark://spark-master:7077'},
        application_args=['--execution-date', '{{ macros.ds_add(ds, -1) }}'],
        verbose=True,
    )

    load_clickhouse = SparkSubmitOperator(
        task_id='load_clickhouse',
        application=f'{SPARK_JOBS_PATH}/assets_to_clickhouse.py',
        conn_id='spark_default',
        conf={'spark.master': 'spark://spark-master:7077'},
        application_args=['--execution-date', '{{ macros.ds_add(ds, -1) }}'],
        verbose=True,
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=(
            "docker exec dbt bash -lc "
            f"\"cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir {DBT_PROFILES_DIR}\""
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "docker exec dbt bash -lc "
            f"\"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROFILES_DIR} "
            "--select +marts\""
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "docker exec dbt bash -lc "
            f"\"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR}\""
        ),
    )

    ingest_bronze >> process_silver >> load_clickhouse >> dbt_deps >> dbt_run >> dbt_test