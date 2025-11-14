import re
import pandas as pd
from pyflowetl.validators.base import BaseValidator

class TelefonoItalianoValidator(BaseValidator):
    pattern = re.compile(r'^(?!00)(0\d{6,}|3\d{9}|(330|335|360|368)\d{6,7})$')

    def validate(self, value) -> bool:
        if pd.isna(value):
            return False
        numero = str(value).strip()
        return bool(self.pattern.match(numero))

    def error_message(self):
        return "Numero di telefono non valido"

# ESEMPIO D'USO
if __name__ == "__main__":
    v = TelefonoItalianoValidator()
    print(v.validate("0811234567"))      # True (fisso)
    print(v.validate("3351234567"))      # True (mobile specifico)
    print(v.validate("330123456"))       # True (mobile specifico 6 cifre)
    print(v.validate("3481234567"))      # True (mobile generico)
    print(v.validate("00391234567"))     # False
    print(v.validate(""))                # False
