import re

class RegexValidator:
    def __init__(self, pattern: str, error_message: str = None, flags=0):
        """
        :param pattern: Espressione regolare da usare per la validazione.
        :param error_message: Messaggio di errore personalizzato.
        :param flags: Flag opzionali per la regex (es. re.IGNORECASE).
        """
        self.pattern = re.compile(pattern, flags)
        self._error_message = error_message or f"Il valore non corrisponde al pattern {pattern}"

    def validate(self, value) -> bool:
        if value is None:
            return False
        return bool(self.pattern.fullmatch(str(value).strip()))

    def error_message(self) -> str:
        return self._error_message
