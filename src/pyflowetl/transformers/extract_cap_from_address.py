import pandas as pd
import re
from pyflowetl.log import get_logger, log_memory_usage

class ExtractCapFromAddressTransformer:
    def __init__(self, address_column: str, output_column: str = "CAP_ESTRATTO"):
        self.address_column = address_column
        self.output_column = output_column
        self.logger = get_logger()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info(f"[ExtractCapFromAddressTransformer] Estrazione CAP da '{self.address_column}' in '{self.output_column}'")

        def extract_cap(address: str) -> str:
            if not isinstance(address, str):
                return None
            match = re.search(r'\b\d{5}\b', address)
            return str(match.group(0)) if match else None

        df[self.output_column] = df[self.address_column].apply(extract_cap)


        log_memory_usage("[ExtractCapFromAddressTransformer] post-transform")
        return df
