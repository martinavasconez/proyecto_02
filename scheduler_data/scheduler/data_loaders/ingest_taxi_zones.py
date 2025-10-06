if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd

@data_loader
def load_data(*args, **kwargs):

    url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    print(f"[INFO] Descargando datos desde {url}")

    data_raw = pd.read_csv(url)
    print(f"[INFO] Archivo cargado correctamente con {len(data_raw)} filas y {len(data_raw.columns)} columnas")

    return data_raw

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
