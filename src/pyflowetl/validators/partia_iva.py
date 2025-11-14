from pyflowetl.validators.base import BaseValidator
import pandas as pd

class PartitaIVAValidator(BaseValidator):
    def validate(self, value) -> bool:
        if pd.isna(value):
            return False

        piva = str(value).strip()
        if not piva.isdigit() or len(piva) != 11:
            return False

        # Calcolo del codice di controllo
        pari = sum(int(piva[i]) for i in range(0, 10, 2))
        dispari = 0
        for i in range(1, 10, 2):
            doppio = 2 * int(piva[i])
            if doppio > 9:
                doppio -= 9
            dispari += doppio
        totale = pari + dispari
        check_digit = (10 - (totale % 10)) % 10
        return check_digit == int(piva[-1])

    def error_message(self):
        return "Partita IVA non valida"

# ESEMPIO D'USO
if __name__ == "__main__":
    v = PartitaIVAValidator()
    print(v.validate("12345678901"))  # False
    print(v.validate("07643520567"))  # True (Acea Energia Spa)
    print(v.validate(None))           # False
