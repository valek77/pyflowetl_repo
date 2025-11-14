import pandas as pd
import random
from datetime import datetime, timedelta
from pyflowetl.log import get_logger, log_memory_usage

class AddRandomDatetimeTransformer:
    def __init__(self, output_column="DATA_ORA", start_datetime="2025-01-01 00:00:00",
                 end_datetime="2025-12-31 23:59:59", date_format="%Y-%m-%d %H:%M:%S"):
        """
        Aggiunge una colonna con data e ora random entro un intervallo specificato.

        :param output_column: Nome della colonna da aggiungere
        :param start_datetime: Inizio intervallo (stringa in formato date_format)
        :param end_datetime: Fine intervallo (stringa in formato date_format)
        :param date_format: Formato della data (es. "%Y-%m-%d %H:%M:%S")
        """
        self.output_column = output_column
        self.start = datetime.strptime(start_datetime, date_format)
        self.end = datetime.strptime(end_datetime, date_format)
        self.date_format = date_format
        self.logger = get_logger()

    def _random_datetime(self):
        delta = self.end - self.start
        random_seconds = random.randint(0, int(delta.total_seconds()))
        return (self.start + timedelta(seconds=random_seconds)).strftime(self.date_format)

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        self.logger.info(f"[AddRandomDatetimeTransformer] Aggiungo colonna '{self.output_column}' con date random tra {self.start} e {self.end}")
        data[self.output_column] = [self._random_datetime() for _ in range(len(data))]
        log_memory_usage("Dopo AddRandomDatetimeTransformer")
        return data
