import pandas as pd
import re
from functools import lru_cache
from cleanco import basename
from nameparser import HumanName
from unidecode import unidecode
from pyflowetl.log import get_logger, log_memory_usage

logger = get_logger()

# --- Regex e dizionari rapidi ---
RE_ORG = re.compile(r"\b(srls?|s\.r\.l\.|spa|s\.p\.a\.|snc|sas|s\.n\.c\.|s\.a\.s\.|coop|cooperativa|ditta|cond\.?)\b", re.I)
RE_ONLY_LETTERS = re.compile(r"^[A-ZÀ-ÖØ-Þ' \-\.]+$")

IT_SURNAME_PREFIXES = {"de","di","del","della","delle","dei","degli","da","d’","d'","lo","la","le","li"}
ES_PARTICLES = {"de","del","de la","y"}

AR_ARTICLES = {"al","el","ad","as","az","ash","at"}
AR_JOINERS  = {"bin","ibn","bint","ben","abu","abdel","abdul","abd","oul","ould","oulad"}
AR_SET = AR_ARTICLES | AR_JOINERS

CN_SURNAMES = {"chen","zhang","li","liu","wang","zhou","wu","hu","xu","lin","zhu","ye","jin","yu","lai"}

def _glue_prefixes(tokens, prefixes):
    out = []; i = 0
    while i < len(tokens):
        t = tokens[i]
        if t in prefixes and i+1 < len(tokens):
            out.append(t + " " + tokens[i+1]); i += 2
        else:
            out.append(t); i += 1
    return out

def _postprocess_last_generic(last: str) -> str:
    if not last:
        return ""
    toks = last.lower().split()
    for prefset in (IT_SURNAME_PREFIXES, ES_PARTICLES):
        toks = _glue_prefixes(toks, prefset)
    return " ".join(t.title() for t in toks)

def _looks_all_caps_person(s: str) -> bool:
    up = s.upper()
    return s == up and RE_ONLY_LETTERS.match(up) is not None and " SRL" not in up

def _arabic_compose_last_from_right(tokens):
    if not tokens: return [], tokens
    i = len(tokens) - 1
    last_tokens = [tokens[i]]; i -= 1
    while i >= 0:
        t = tokens[i]
        if t in {"al","el"} and i-1 >= 0 and tokens[i-1] in {"abd","abdel","abdul"}:
            last_tokens.insert(0, t); last_tokens.insert(0, tokens[i-1]); i -= 2; continue
        if t in AR_SET:
            last_tokens.insert(0, t); i -= 1; continue
        break
    first_tokens = tokens[:i+1]
    return last_tokens, first_tokens

@lru_cache(maxsize=200_000)
def _is_org_fast(s: str) -> bool:
    if not s:
        return False
    # match espliciti azienda
    if RE_ORG.search(s):
        return True
    # cleanco
    try:
        if basename(s) != s:
            return True
    except Exception:
        pass
    # guard: stringa alfabetica breve -> NON ORG
    up = s.upper().strip()
    if RE_ONLY_LETTERS.match(up) and len(up.split()) <= 3:
        return False
    return False

@lru_cache(maxsize=200_000)
def _split_person_fast(s: str):
    if not s: return "",""
    s = unidecode(" ".join(str(s).split()).replace("’","'")).strip()
    if not s: return "",""

    # "Cognome, Nome"
    if "," in s:
        left, right = [p.strip() for p in s.split(",", 1)]
        if left and right:
            return right.title(), _postprocess_last_generic(left)

    tokens = [t.strip("',.- ").lower().replace("-", " ") for t in s.split()]
    tokens = " ".join(tokens).split()

    # Cinese
    if len(tokens) == 2 and tokens[0] in CN_SURNAMES:
        return tokens[1].title(), tokens[0].title()

    # ALL CAPS: di default è "COGNOME NOME"
    if _looks_all_caps_person(s):
        # Se contiene legatori arabi, usa la composizione da destra
        if any(t in AR_SET for t in tokens):
            last_tokens, first_tokens = _arabic_compose_last_from_right(tokens)
            first = " ".join(t.title() for t in first_tokens)
            last  = " ".join(t.title() for t in last_tokens)
            return first, last
        # Altrimenti semplice COGNOME NOME
        if len(tokens) >= 2:
            return " ".join(t.title() for t in tokens[1:]), tokens[0].title()
        # token singolo
        return tokens[0].title(), ""

    # nameparser base
    n = HumanName(s.title())
    first = " ".join([n.first, n.middle]).strip()
    last  = n.last.strip()

    if not last:
        last_tokens, first_tokens = _arabic_compose_last_from_right(tokens)
        if last_tokens:
            last = " ".join(t.title() for t in last_tokens)
            if not first:
                first = " ".join(t.title() for t in first_tokens)

    last = _postprocess_last_generic(last)

    if not first or not last:
        if len(tokens) >= 2:
            last_tokens, first_tokens = _arabic_compose_last_from_right(tokens)
            first = " ".join(t.title() for t in first_tokens) or " ".join(t.title() for t in tokens[:-1])
            last  = " ".join(t.title() for t in last_tokens)  or tokens[-1].title()
        else:
            first, last = tokens[0].title(), ""

    return first.strip(), last.strip()


class SplitNameTransformer:
    def __init__(self, source_column: str, first_col="FIRST_NAME", last_col="LAST_NAME", type_col=None):
        self.source_column = source_column
        self.first_col = first_col
        self.last_col = last_col
        self.type_col = type_col
        logger.info(f"[SplitNameTransformer] Inizializzato su '{source_column}' -> ({first_col},{last_col})")

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.source_column not in df.columns:
            raise KeyError(f"Colonna '{self.source_column}' non trovata")

        logger.info(
            f"[SplitNameTransformer] Split '{self.source_column}' -> '{self.first_col}','{self.last_col}'"
        )

        # Sorgente normalizzata a stringa
        col = df[self.source_column].fillna("").astype(str).map(lambda s: s.strip())

        # PERSON vs ORG (fast)
        is_org = col.map(_is_org_fast)

        # Split solo per PERSON
        persons = col[~is_org]
        split_vals = persons.map(_split_person_fast)

        # Crea DataFrame con colonne corrette, indicizzato sulle stesse righe dei PERSON
        pairs = pd.DataFrame(
            split_vals.tolist(),
            index=persons.index,
            columns=[self.first_col, self.last_col],
        )

        # Inizializza colonne di output se mancanti
        if self.first_col not in df.columns:
            df[self.first_col] = ""
        if self.last_col not in df.columns:
            df[self.last_col] = ""

        # Scrive solo le righe PERSON, non tocca la colonna sorgente
        df.loc[persons.index, [self.first_col, self.last_col]] = pairs

        # Opzionale: colonna TYPE
        if self.type_col:
            df[self.type_col] = is_org.map(lambda x: "ORG" if x else "PERSON")

        logger.info(
            f"[SplitNameTransformer] PERSON: {int((~is_org).sum())}, ORG: {int(is_org.sum())}"
        )
        log_memory_usage("[SplitNameTransformer] post-transform")
        return df


# --- ESEMPIO ---
if __name__ == "__main__":
    df = pd.DataFrame({
        "FULLNAME": [
            "ABD EL RAHMAN AHMED",
            "Abdel Rahman Ahmed",
            "Ben Ali Mohamed",
            "Ibn Sina",
            "Ould Mohamed Salem",
            "Al Hussein, Firas",
            "ZULFIQAR ALI'",
            "Zouhair Zyad",
            "Techno S.r.l.",
            "DE ROSSI MARCO",
            "Zhang Wei",
            "Cooperativa Alfa"
        ]
    })
    t = SplitNameTransformer(source_column="FULLNAME", type_col="TYPE")
    print(t.transform(df))
