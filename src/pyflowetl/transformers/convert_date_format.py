import pandas as pd
from pyflowetl.log import get_logger, log_memory_usage

logger = get_logger()

class ConvertDateFormatTransformer:
    def __init__(self, columns, input_format, output_format, errors="raise"):
        """
        Converte una o più colonne di date da un formato all'altro.

        :param columns: colonna o lista di colonne
        :param input_format: formato di input (es. "%d-%b-%y")
        :param output_format: formato di output (es. "%Y-%m-%d")
        :param errors: 'coerce' (null su errori), 'raise', 'ignore'

        Esempio:
        ConvertDateFormatTransformer("data attivazione", "%d-%b-%y", "%Y-%m-%d")
        """
        self.columns = [columns] if isinstance(columns, str) else columns
        self.input_format = input_format
        self.output_format = output_format
        self.errors = errors

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        for col in self.columns:
            if col not in data.columns:
                logger.warning(f"[ConvertDateFormatTransformer] Colonna '{col}' non trovata")
                continue

            logger.info(f"[ConvertDateFormatTransformer] Converto '{col}' da '{self.input_format}' a '{self.output_format}'")
            try:
                data[col] = pd.to_datetime(data[col], format=self.input_format, errors=self.errors)
                data[col] = data[col].dt.strftime(self.output_format)
            except Exception as e:
                logger.exception(f"Errore nella conversione della colonna {col}: {e}")
                raise

        log_memory_usage("Dopo ConvertDateFormatTransformer")
        return data


# ESEMPIO D'USO
if __name__ == "__main__":
    df = pd.DataFrame({
        "data attivazione": ["01-JUN-25", "05-JUN-25", "31-DEC-24"]
    })

    transformer = ConvertDateFormatTransformer(
        columns="data attivazione",
        input_format="%d-%b-%y",    # formato tipo Oracle
        output_format="%Y-%m-%d"
    )

    df = transformer.transform(df)
    print(df)
