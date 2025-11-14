import re

def clean_string(text: str) -> str:
    """
    Rimuove spazi, caratteri speciali e converte in maiuscolo.
    Se il valore è NaN o non stringa, restituisce stringa vuota.
    """
    if not isinstance(text, str):
        return ""
    return re.sub(r'\s+', '', text).upper().strip()
