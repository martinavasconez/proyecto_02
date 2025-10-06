
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd
@transformer
def combine_batches(batches, *args, **kwargs):
    trips_all = []
    audits_all = []

    for batch in batches:
        if isinstance(batch, dict):
            trips_all.append(batch["trips"])
            audits_all.append(batch["audit"])
        else:
            print(f"[WARN] Batch inválido: {type(batch)}")

    trips_df = pd.concat(trips_all, ignore_index=True) if trips_all else pd.DataFrame()
    audit_df = pd.concat(audits_all, ignore_index=True) if audits_all else pd.DataFrame()

    return {"trips": trips_df, "audit": audit_df}
