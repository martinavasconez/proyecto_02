if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

import pandas as pd

@transformer()
def transform_batch(batch, *args, **kwargs):
    trips = batch["trips"]
    audit = batch["audit"]

    # hacer validaciones / filtros mínimos aquí
    return {"trips": trips, "audit": audit}
