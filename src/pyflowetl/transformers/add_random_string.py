import random
from typing import List

import pandas as pd

from pyflowetl.log import get_logger, log_memory_usage


class AddRandomStringTransformer:
    def __init__(self, values: List[str], output_column: str = "RANDOM_STRING"):
        self.logger = get_logger()
        self.values = values
        self.output_column = output_column

        if not self.values:
            self.logger.warning("[AddRandomStringTransformer] La lista di valori è vuota.")

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        if not self.values:
            self.logger.warning(f"[AddRandomStringTransformer] Lista vuota, imposto colonna '{self.output_column}' a stringa vuota.")
            data[self.output_column] = ""
            return data

        self.logger.info(f"[AddRandomStringTransformer] Genero valori casuali in colonna '{self.output_column}' da {len(self.values)} valori possibili.")
        data[self.output_column] = [random.choice(self.values) for _ in range(len(data))]

        log_memory_usage("Dopo AddRandomStringTransformer")
        return data
