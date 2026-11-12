from datetime import datetime, timezone
import os
import pendulum
import requests
from pymongo import MongoClient
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

FRANKFURTER_API_URL = "https://api.frankfurter.dev/v2/rates?base=EUR&quotes=USD,GBP,JPY"


def extract_currency_exchange_rates(**context):
    # Fetch Daily Exchange Rates And Record Extraction Time
    response = requests.get(FRANKFURTER_API_URL, timeout=30)
    response.raise_for_status()
    retrieved_at = datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds")
    payload = {
        "rates": response.json(),
        "retrieved_at": retrieved_at,
    }
    # Push Raw Api Payload With Named XCom Key
    context["ti"].xcom_push(key="raw_exchange_rates", value=payload)


def transform_exchange_rate_data(**context):
    # Pull Raw Exchange Rates Using Named XCom Key
    payload = context["ti"].xcom_pull(
        task_ids="extract_currency_exchange_rates",
        key="raw_exchange_rates",
    )
    if not payload or not payload.get("rates"):
        raise ValueError("No raw exchange rate data found in XCom")

    retrieved_at = payload["retrieved_at"]
    transformed_records = [
        {
            "base_currency": str(item["base"]),
            "target_currency": str(item["quote"]),
            "exchange_rate": float(item["rate"]),
            "rate_date": str(item["date"]),
            "source": "Frankfurter API",
            "retrieved_at": retrieved_at,
        }
        for item in payload["rates"]
    ]
    # Push Transformed Records With Named XCom Key
    context["ti"].xcom_push(key="transformed_exchange_rates", value=transformed_records)


def load_data_into_mongodb(**context):
    # Pull Transformed Records Using Named XCom Key
    records = context["ti"].xcom_pull(
        task_ids="transform_exchange_rate_data",
        key="transformed_exchange_rates",
    )
    if not records:
        raise ValueError("No transformed exchange rate records found in XCom")

    mongo_host = os.getenv("MONGO_HOST", "mongodb")
    mongo_port = int(os.getenv("MONGO_PORT", "27017"))
    mongo_user = os.environ["MONGO_INITDB_ROOT_USERNAME"]
    mongo_password = os.environ["MONGO_INITDB_ROOT_PASSWORD"]
    mongo_db = os.getenv("MONGO_DB", "currency_exchange")
    mongo_collection = os.getenv("MONGO_COLLECTION", "exchange_rates")

    client = MongoClient(
        host=mongo_host,
        port=mongo_port,
        username=mongo_user,
        password=mongo_password,
        authSource="admin",
        serverSelectionTimeoutMS=10000,
    )
    try:
        collection = client[mongo_db][mongo_collection]
        for record in records:
            # Upsert By Currency Pair And Rate Date For Safe Reruns
            collection.update_one(
                {
                    "base_currency": record["base_currency"],
                    "target_currency": record["target_currency"],
                    "rate_date": record["rate_date"],
                },
                {"$set": record},
                upsert=True,
            )
    finally:
        client.close()


with DAG(
    dag_id="currency_exchange_etl",
    description="Daily ETL pipeline loading Frankfurter currency rates into MongoDB",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    default_args={"owner": "airflow", "retries": 2},
    tags=["etl", "currency"],
) as dag:
    extract_task = PythonOperator(
        task_id="extract_currency_exchange_rates",
        python_callable=extract_currency_exchange_rates,
    )

    transform_task = PythonOperator(
        task_id="transform_exchange_rate_data",
        python_callable=transform_exchange_rate_data,
    )

    load_task = PythonOperator(
        task_id="load_data_into_mongodb",
        python_callable=load_data_into_mongodb,
    )

    extract_task >> transform_task >> load_task
