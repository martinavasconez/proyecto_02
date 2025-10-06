from mage_ai.data_preparation.shared.secrets import get_secret_value

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


def create_table_if_not_exists(conn, table_name: str, audit_table: str):
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
    VENDORID NUMBER,
    TPEP_PICKUP_DATETIME TIMESTAMP_NTZ,
    TPEP_DROPOFF_DATETIME TIMESTAMP_NTZ,
    PASSENGER_COUNT NUMBER,
    TRIP_DISTANCE FLOAT,
    RATECODEID NUMBER,
    STORE_AND_FWD_FLAG VARCHAR(1),
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
    CBD_CONGESTION_FEE FLOAT,
    RUN_ID STRING,
    VENTANA_TEMPORAL TIMESTAMP_NTZ,
    CHUNK_MES VARCHAR(50),
    YEAR NUMBER,
    MONTH NUMBER,
    PRIMARY KEY (TPEP_PICKUP_DATETIME, TPEP_DROPOFF_DATETIME, PULOCATIONID, DOLOCATIONID)
);
     """
    cur = conn.cursor()
    cur.execute(create_sql)

    # tabla de auditoría
    create_audit_sql = f"""
    CREATE TABLE IF NOT EXISTS {audit_table} (
        SERVICE STRING,
        YEAR NUMBER,
        MONTH NUMBER,
        STATUS STRING,
        ROW_COUNT NUMBER,
        ERROR_MESSAGE STRING,
        RUN_ID STRING,
        INGESTED_AT_UTC TIMESTAMP_NTZ
    );
    """
    cur.execute(create_audit_sql)
    cur.close()


def month_already_loaded(conn, audit_table: str, service: str, year: int, month: int):
    query = f"""
    SELECT 1
    FROM {audit_table}
    WHERE SERVICE = %s
      AND YEAR = %s
      AND MONTH = %s
      AND STATUS = 'ok'
    LIMIT 1
    """
    cur = conn.cursor()
    cur.execute(query, (service, year, month))
    result = cur.fetchone()
    cur.close()
    return result is not None


@data_loader
def load_data(*args, **kwargs):
    service = "yellow"
    start_year = int(kwargs.get("start_year"))
    end_year = int(kwargs.get("end_year"))

    run_id = str(uuid.uuid4())
    ingested_at = kwargs.get("execution_date") or datetime.utcnow()

    conn = get_snowflake_conn()
    table_name = "YELLOW_TAXIS"
    audit_table = "AUDIT_YELLOW"

    create_table_if_not_exists(conn, table_name, audit_table)

    audit_rows = []

    try:
        for year in range(start_year, end_year + 1):
            for month in range(1, 13):  
                if month_already_loaded(conn, audit_table, service, year, month):
                    print(f"[SKIP] {service} {year}-{month:02d} ya cargado, se omite.")
                    continue

                url = _build_url(service, year, month)
                print(f"\n[INFO] Ingestando {service} {year}-{month:02d} desde {url}")

                row_count = 0
                try:
                    resp = requests.get(url, headers=HEADERS, timeout=60)
                    if resp.status_code == 403:
                        print(f"[SKIP] {year}-{month:02d} no disponible (404)")
                        audit_rows.append({
                        "service": service,
                        "year": year,
                        "month": month,
                        "status": "skip",
                        "row_count": 0,
                        "error_message": "Archivo no disponible (404)",
                        "run_id": run_id,
                        "ingested_at_utc": ingested_at,
                    })
                        continue
                    
                    resp.raise_for_status()

                    parquet_file = pq.ParquetFile(BytesIO(resp.content))
                    total_rows = parquet_file.metadata.num_rows
                    total_batches = (total_rows // 1_000_000) + (1 if total_rows % 1_000_000 else 0)

                    print(f"[INFO] {year}-{month:02d} tiene {total_rows} filas en {total_batches} chunks")

                    for i, batch in enumerate(parquet_file.iter_batches(batch_size=1_000_000), start=1):
                        df = batch.to_pandas()

                        # metadata
                        df["RUN_ID"] = run_id
                        df["VENTANA_TEMPORAL"] = datetime.now()
                        df["CHUNK_MES"] = f"{i}/{month}"
                        df["YEAR"] = year
                        df["MONTH"] = month
                        df = df.rename(columns=str.upper)

                        for col in ["TPEP_PICKUP_DATETIME", "TPEP_DROPOFF_DATETIME", "VENTANA_TEMPORAL"]:
                            if col in df.columns: 
                                df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")


                        # exportar
                        success, nchunks, nrows, _ = write_pandas(
                            conn,
                            df,
                            table_name=table_name,
                            auto_create_table=False,
                            overwrite=False,
                            quote_identifiers=True,
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

                    # insertar en audit
                    cur = conn.cursor()
                    cur.execute(f"""
                        INSERT INTO {audit_table}
                        (SERVICE, YEAR, MONTH, STATUS, ROW_COUNT, ERROR_MESSAGE, RUN_ID, INGESTED_AT_UTC)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (service, year, month, 'ok', row_count, None, run_id, ingested_at))
                    cur.close()

                    print(f"[OK] {year}-{month:02d} exportado → {row_count} filas totales")

                except Exception as e:
                    cur = conn.cursor()
                    cur.execute(f"""
                        INSERT INTO {audit_table}
                        (SERVICE, YEAR, MONTH, STATUS, ROW_COUNT, ERROR_MESSAGE, RUN_ID, INGESTED_AT_UTC)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (service, year, month, 'fail', 0, str(e), run_id, ingested_at))
                    cur.close()
                    print(f"[ERROR] {year}-{month:02d} → {e}")

                time.sleep(1)

    finally:
        conn.close()

    return {"audit": pd.DataFrame(audit_rows)}
