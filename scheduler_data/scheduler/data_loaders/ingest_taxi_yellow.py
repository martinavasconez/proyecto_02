
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd
import pyarrow.parquet as pq
import requests
from io import BytesIO
from datetime import datetime
import uuid
import time
import gc
from os import path
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader


BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/octet-stream",
}


def _build_url(service: str, year: int, month: int) -> str:
    return f"{BASE_URL}/{service}_tripdata_{year}-{month:02d}.parquet"

def get_snowflake_conn():
    config_path = path.join(get_repo_path(), "io_config.yaml")
    config_profile = "default"
    config_loader = ConfigFileLoader(config_path, config_profile)

    return snowflake.connector.connect(
        user=config_loader.get("SNOWFLAKE_USER"),
        password=config_loader.get("SNOWFLAKE_PASSWORD"),
        account=config_loader.get("SNOWFLAKE_ACCOUNT"),
        warehouse=config_loader.get("SNOWFLAKE_DEFAULT_WH"),
        database=config_loader.get("SNOWFLAKE_DEFAULT_DB"),
        schema=config_loader.get("SNOWFLAKE_DEFAULT_SCHEMA"),
        role=config_loader.get("SNOWFLAKE_ROLE"),
    )

def create_table_if_not_exists(conn, table_name: str):
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        VENDORID NUMBER,
        TPEP_PICKUP_DATETIME TIMESTAMP_NTZ,
        TPEP_DROPOFF_DATETIME TIMESTAMP_NTZ,
        PASSENGER_COUNT NUMBER,
        TRIP_DISTANCE FLOAT,
        RATECODEID NUMBER,
        STORE_AND_FWD_FLAG STRING,
        PULOCATIONID NUMBER,
        DOLOCATIONID NUMBER,
        PAYMENT_TYPE NUMBER,
        FARE_AMOUNT FLOAT,
        EXTRA FLOAT,
        MTA_TAX FLOAT,
        TIP_AMOUNT FLOAT,
        TOLLS_AMOUNT FLOAT,
        IMPROVEMENT_SURCHARGE FLOAT,
        TOTAL_AMOUNT FLOAT,
        CONGESTION_SURCHARGE FLOAT,
        AIRPORT_FEE FLOAT,
        RUN_ID STRING,
        WINDOW_START TIMESTAMP_NTZ,
        WINDOW_END TIMESTAMP_NTZ,
        SERVICE_TYPE STRING,
        YEAR NUMBER,
        MONTH NUMBER,
        SOURCE_URL STRING,
        INGESTED_AT_UTC TIMESTAMP_NTZ
    )
    """
    cur = conn.cursor()
    cur.execute(create_sql)
    cur.close()

@data_loader
def load_data(*args, **kwargs):
    service = "yellow"
    start_year = int(kwargs.get("start_year"))
    end_year = int(kwargs.get("end_year"))

    run_id = str(uuid.uuid4())
    ingested_at = kwargs.get("execution_date") or datetime.utcnow()

    conn = get_snowflake_conn()
    table_name = "TRIPS"

    create_table_if_not_exists(conn, table_name)

    audit_rows = []

    try:
        for year in range(start_year, end_year + 1):
            for month in range(1, 2):  # prueba solo enero
                url = _build_url(service, year, month)
                print(f"\n[INFO] Ingestando {service} {year}-{month:02d} desde {url}")

                row_count = 0
                try:
                    resp = requests.get(url, headers=HEADERS, timeout=60)
                    resp.raise_for_status()

                    parquet_file = pq.ParquetFile(BytesIO(resp.content))
                    total_rows = parquet_file.metadata.num_rows
                    total_batches = (total_rows // 1_000_000) + (1 if total_rows % 1_000_000 else 0)

                    print(f"[INFO] {year}-{month:02d} tiene {total_rows} filas en {total_batches} chunks")

                    for i, batch in enumerate(parquet_file.iter_batches(batch_size=1_000_000), start=1):
                        df = batch.to_pandas()

                        # metadata
                        df["RUN_ID"] = run_id
                        df["WINDOW_START"] = datetime(year, month, 1)
                        df["WINDOW_END"] = (
                            datetime(year + 1, 1, 1) - pd.Timedelta(seconds=1)
                            if month == 12
                            else datetime(year, month + 1, 1) - pd.Timedelta(seconds=1)
                        )
                        df["SERVICE_TYPE"] = service
                        df["YEAR"] = year
                        df["MONTH"] = month
                        df["SOURCE_URL"] = url
                        df["INGESTED_AT_UTC"] = ingested_at
                        df = df.rename(columns=str.upper)


                        # exportar
                        success, nchunks, nrows, _ = write_pandas(
                            conn,
                            df,
                            table_name=table_name,
                            auto_create_table=False,
                            overwrite=False,
                        )

                        row_count += len(df)
                        restantes = total_batches - i
                        print(
                            f"[EXPORT] {year}-{month:02d} → chunk {i}/{total_batches} "
                            f"({len(df)} filas, acumulado {row_count}, faltan {restantes})"
                        )

                        del df
                        gc.collect()
                        time.sleep(0.5)

                    audit_rows.append({
                        "service": service,
                        "year": year,
                        "month": month,
                        "status": "ok",
                        "row_count": row_count,
                        "error_message": None,
                        "run_id": run_id,
                        "ingested_at_utc": ingested_at,
                    })
                    print(f"[OK] {year}-{month:02d} exportado → {row_count} filas totales")

                except Exception as e:
                    audit_rows.append({
                        "service": service,
                        "year": year,
                        "month": month,
                        "status": "fail",
                        "row_count": 0,
                        "error_message": str(e),
                        "run_id": run_id,
                        "ingested_at_utc": ingested_at,
                    })
                    print(f"[ERROR] {year}-{month:02d} → {e}")
                time.sleep(1)

    finally:
        conn.close()

    return {"audit": pd.DataFrame(audit_rows)}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
