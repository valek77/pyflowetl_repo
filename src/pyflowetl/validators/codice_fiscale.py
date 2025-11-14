from pyflowetl.log import get_logger

class CodiceFiscaleValidator:
    def __init__(self):
        self.logger = get_logger()
        self._last_error = ""

    def validate(self, value: str) -> bool:
        if not value or not isinstance(value, str) or len(value) != 16:
            self._last_error = "Codice fiscale non valido (lunghezza ≠ 16)"
            return False

        value = value.upper()

        if not value[:15].isalnum() or not value[15].isalpha():
            self._last_error = "Formato codice fiscale non valido"
            return False

        expected = self._calcola_codice_controllo(value[:15])
        if expected != value[15]:
            self._last_error = f"Carattere di controllo errato: atteso {expected}, trovato {value[15]}"
            return False

        return True

    def error_message(self):
        return self._last_error or "Codice fiscale non valido"

    def _calcola_codice_controllo(self, codice: str) -> str:
        dispari = {
            **{ch: val for ch, val in zip("0123456789", [1, 0, 5, 7, 9, 13, 15, 17, 19, 21])},
            **{ch: val for ch, val in zip("ABCDEFGHIJKLMNOPQRSTUVWXYZ",
                                          [1, 0, 5, 7, 9, 13, 15, 17, 19, 21,
                                           2, 4, 18, 20, 11, 3, 6, 8, 12, 14,
                                           16, 10, 22, 25, 24, 23])}
        }

        pari = {
            **{ch: int(ch) for ch in "0123456789"},
            **{ch: ord(ch) - ord('A') for ch in "ABCDEFGHIJKLMNOPQRSTUVWXYZ"}
        }

        somma = 0
        for i, ch in enumerate(codice):
            somma += dispari[ch] if i % 2 == 0 else pari[ch]

        return chr((somma % 26) + ord('A'))
