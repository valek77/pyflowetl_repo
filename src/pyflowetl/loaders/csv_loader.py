import os
from email import header

from pyflowetl.log import get_logger, log_memory_usage

import math

class CsvLoader:
    def __init__(
        self,
        output_path,
        encoding="utf-8",
        delimiter=",",
        header=True,
        rows_per_file=None,   # <-- numero righe per file (se None: file unico)
        part_digits=4,        # <-- padding: part0001, part0002...
    ):
        self.header = header
        self.output_path = output_path
        self.encoding = encoding
        self.delimiter = delimiter
        self.rows_per_file = rows_per_file
        self.part_digits = part_digits

    def _split_output_path(self, part_index: int) -> str:
        """
        Genera: base_part0001.csv
        """
        base, ext = os.path.splitext(self.output_path)
        if not ext:
            ext = ".csv"
        suffix = f"_part{part_index:0{self.part_digits}d}"
        return f"{base}{suffix}{ext}"

    def load(self, data):
        logger = get_logger()

        total_rows = len(data)
        if total_rows == 0:
            logger.warning(f"[CsvLoader] DataFrame vuoto, nessun file scritto: {self.output_path}")
            log_memory_usage(f"Dopo CsvLoader (vuoto): {self.output_path}")
            return

        # Se rows_per_file non è impostato -> comportamento originale (file singolo)
        if self.rows_per_file is None:
            logger.info(f"[CsvLoader] Scrittura su file: {self.output_path}")
            try:
                out_dir = os.path.dirname(self.output_path)
                if out_dir:
                    os.makedirs(out_dir, exist_ok=True)

                data.to_csv(
                    self.output_path,
                    index=False,
                    encoding=self.encoding,
                    sep=self.delimiter,
                    header=self.header,
                )
                logger.info(f"[CsvLoader] Scrittura completata: {total_rows} record")
                log_memory_usage(f"Dopo Scrittura su file: {self.output_path}")
                return
            except Exception as e:
                logger.exception(f"[CsvLoader] Errore durante la scrittura del file: {e}")
                raise

        # Split in più file
        if not isinstance(self.rows_per_file, int) or self.rows_per_file <= 0:
            raise ValueError("rows_per_file deve essere un intero > 0")

        num_parts = math.ceil(total_rows / self.rows_per_file)
        logger.info(
            f"[CsvLoader] Split output: {total_rows} record in {num_parts} file "
            f"da {self.rows_per_file} righe ciascuno (circa). Base: {self.output_path}"
        )

        try:
            out_dir = os.path.dirname(self.output_path)
            if out_dir:
                os.makedirs(out_dir, exist_ok=True)

            for part in range(1, num_parts + 1):
                start = (part - 1) * self.rows_per_file
                end = min(start + self.rows_per_file, total_rows)

                part_path = self._split_output_path(part)
                logger.info(f"[CsvLoader] Scrittura chunk {part}/{num_parts}: righe {start}:{end} -> {part_path}")

                chunk = data.iloc[start:end]
                chunk.to_csv(
                    part_path,
                    index=False,
                    encoding=self.encoding,
                    sep=self.delimiter,
                    header=self.header,
                )

                log_memory_usage(f"Dopo Scrittura chunk {part}/{num_parts}: {part_path}")

            logger.info(f"[CsvLoader] Split completato: scritti {num_parts} file, totale {total_rows} record")
        except Exception as e:
            logger.exception(f"[CsvLoader] Errore durante lo split/write: {e}")
            raise
