from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.snowflake import Snowflake
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_snowflake(batch: dict, **kwargs) -> None:
    config_path = path.join(get_repo_path(), "io_config.yaml")
    config_profile = "default"
    database = "NY_TAXI"
    schema = "RAW"
    table_name = "testing"

    trips_df: DataFrame = batch.get("trips")

    if trips_df is not None and not trips_df.empty:
        print(f"[EXPORT] Subiendo {len(trips_df)} filas a {schema}.{table_name}")

        with Snowflake.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
            loader.export(
                trips_df.rename(columns=str.upper),
                table_name,
                database,
                schema,
                if_exists="append",
                chunk_size=100_000,
            )

        print(f"[OK] {len(trips_df)} filas insertadas en {schema}.{table_name}")
