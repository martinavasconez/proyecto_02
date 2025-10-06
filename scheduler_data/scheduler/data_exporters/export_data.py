if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from os import path


def get_snowflake_conn():
    config_path = path.join(get_repo_path(), "io_config.yaml")
    config_profile = "default"
    config_loader = ConfigFileLoader(config_path, config_profile)

    print("[INFO] Conectando a Snowflake...")
    conn = snowflake.connector.connect(
        user=config_loader.get("SNOWFLAKE_USER"),
        password=config_loader.get("SNOWFLAKE_PASSWORD"),
        account=config_loader.get("SNOWFLAKE_ACCOUNT"),
        warehouse=config_loader.get("SNOWFLAKE_DEFAULT_WH"),
        database=config_loader.get("SNOWFLAKE_DEFAULT_DB"),
        schema=config_loader.get("SNOWFLAKE_DEFAULT_SCHEMA"),
        role=config_loader.get("SNOWFLAKE_ROLE"),
    )
    print("[INFO] Conexión establecida")
    return conn


@data_exporter
def export_data(data, *args, **kwargs):

    table_name = "TAXI_ZONES"  
    conn = get_snowflake_conn()

    print(f"[INFO] Exportando DataFrame a la tabla {table_name}")
    print(f"[INFO] DataFrame con {len(data)} filas y {len(data.columns)} columnas")


    success, nchunks, nrows, _ = write_pandas(
        conn,
        data,
        table_name=table_name,
        auto_create_table=True,
        overwrite=False,
        quote_identifiers=True
    )

    if success:
        print(f"[OK] {nrows} filas exportadas a Snowflake ({nchunks} chunk(s)).")
    else:
        print("[ERROR] Exportación fallida")

    conn.close()
    print("[INFO] Conexión cerrada")
