import pandas as pd
from dateutil.relativedelta import relativedelta
from pyflowetl.log import get_logger, log_memory_usage

class DateShiftTransformer:
    def __init__(
        self,
        date_column: str,
        output_column: str | None = None,
        *,
        years: int = 0,
        months: int = 0,
        weeks: int = 0,
        days: int = 0,
        hours: int = 0,
        minutes: int = 0,
        seconds: int = 0,
        dayfirst: bool = True,
        errors: str = "coerce",          # 'coerce' -> NaT su valori non parsabili
        input_date_format:str | None = None,
        to_string_format: str | None = None  # es. "%Y-%m-%d"
    ):
        self.date_column = date_column
        self.output_column = output_column or date_column
        self.delta = relativedelta(
            years=years, months=months, weeks=weeks, days=days,
            hours=hours, minutes=minutes, seconds=seconds
        )
        self.dayfirst = dayfirst
        self.errors = errors
        self.to_string_format = to_string_format
        self.input_date_format = input_date_format
        self.logger = get_logger()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        s = pd.to_datetime(df[self.date_column], dayfirst=self.dayfirst, errors=self.errors, format=self.input_date_format)
        shifted = s.apply(lambda d: d + self.delta if pd.notna(d) else pd.NaT)

        if self.to_string_format:
            df[self.output_column] = shifted.dt.strftime(self.to_string_format)
        else:
            df[self.output_column] = shifted

        self.logger.info(f"[DateShiftTransformer] shift applicato su '{self.date_column}' -> '{self.output_column}'")
        log_memory_usage("[DateShiftTransformer] dopo transform")
        return df
