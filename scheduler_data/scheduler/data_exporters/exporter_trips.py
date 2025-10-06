from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.snowflake import Snowflake
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_snowflake(dfs: dict, **kwargs) -> None:
    config_path = path.join(get_repo_path(), "io_config.yaml")
    config_profile = "default"

    database = "NY_TAXI"
    schema = "RAW"
    table_name = "TRIPS_yelllow"

    trips_df: DataFrame = dfs.get("trips")
    if trips_df is None or trips_df.empty:
        print("[WARN] No hay datos en trips para exportar.")
        return

    trips_df = trips_df.rename(columns=str.upper)

    with Snowflake.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        cols = ", ".join([f"{col} STRING" for col in trips_df.columns])
        create_sql = f"CREATE TABLE IF NOT EXISTS {schema}.{table_name} ({cols});"
        loader.execute(create_sql)

        loader.export(
            trips_df,
            table_name,
            database,
            schema,
            if_exists="append",  
            chunk_size=100000,
        )

        print(f"[OK] Export completado → {schema}.{table_name}, filas insertadas: {len(trips_df)}")
