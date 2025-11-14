from datetime import datetime
from pyflowetl.validators.base import BaseValidator

class IsValidDateFormatValidator(BaseValidator):
    def __init__(self, expected_format: str):
        """
        Valida che una stringa rappresenti una data in uno specifico formato.

        :param expected_format: il formato atteso (es. "%Y-%m-%d %H:%M:%S")
        """
        self.expected_format = expected_format

    def validate(self, value) -> bool:
        if not value or not isinstance(value, str):
            return False
        try:
            datetime.strptime(value.strip(), self.expected_format)
            return True
        except ValueError:
            return False

    def error_message(self):
        return f"Data non valida. Formato atteso: {self.expected_format}"
