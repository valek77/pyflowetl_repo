import os
import pandas as pd
import random
import socket
import struct
from pyflowetl.log import get_logger, log_memory_usage

class AddRandomIpTransformer:
    def __init__(self, regione_column="REGIONE", output_column="IP", csv_filename="ips.csv"):
        self.logger = get_logger()
        self.regione_column = regione_column
        self.output_column = output_column
        self.ip_ranges = {}

        # Percorso del file ips.csv
        self.csv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", csv_filename)

        self._load_ip_ranges()

    def _load_ip_ranges(self):
        if not os.path.exists(self.csv_path):
            self.logger.error(f"[AddRandomIpTransformer] File IP non trovato: {self.csv_path}")
            return

        df = pd.read_csv(self.csv_path, sep=';', header=0)
        df.columns = [col.strip() for col in df.columns]

        for _, row in df.iterrows():
            regione = str(row['regione']).strip().lower()
            start = int(row['start'])
            end = int(row['end'])
            self.ip_ranges.setdefault(regione, []).append((start, end))

        self.logger.info(f"[AddRandomIpTransformer] Caricati IP per {len(self.ip_ranges)} regioni.")

    def _decimal_to_ip(self, decimal_ip: int) -> str:
        return socket.inet_ntoa(struct.pack("!I", decimal_ip))

    def _get_random_ip_for_region(self, regione: str) -> str:
        regione = str(regione).strip().lower()
        ranges = self.ip_ranges.get(regione)

        if not ranges:
           # self.logger.warning(f"[AddRandomIpTransformer] Ip con disponibile  per regione '{regione}'")
            return "0.0.0.0"

        start, end = random.choice(ranges)
        ip_decimal = random.randint(start, end)
        return self._decimal_to_ip(ip_decimal)

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        if self.regione_column not in data.columns:
            self.logger.warning(f"[AddRandomIpTransformer] Colonna '{self.regione_column}' non trovata.")
            data[self.output_column] = "0.0.0.0"
            return data

        self.logger.info(f"[AddRandomIpTransformer] Genero IP casuali in colonna '{self.output_column}' dalla colonna '{self.regione_column}'")
        data[self.output_column] = data[self.regione_column].apply(self._get_random_ip_for_region)

        log_memory_usage("Dopo AddRandomIpTransformer")
        return data
