import pandas as pd
from pyflowetl.validators.base import BaseValidator

class NotEmptyValidator(BaseValidator):
    def validate(self, value) -> bool:
        return not pd.isna(value) and str(value).strip() != ""


    def error_message(self):
        return "Il campo non può essere vuoto"


# ESEMPIO D'USO
if __name__ == "__main__":
    v = NotEmptyValidator()
    print(v.validate("ciao"))     # True
    print(v.validate("   "))      # False
    print(v.validate(None))       # False
