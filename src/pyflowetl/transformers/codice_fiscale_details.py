import os
import pandas as pd
import numpy as np
from datetime import date

from pyflowetl.log import get_logger, log_memory_usage


class AddCodiceFiscaleDetailsTransformer:
    """
    Transformer che, data una colonna con il codice fiscale italiano,
    aggiunge le colonne derivate:

      - DATA_NASCITA
      - ANNO_NASCITA
      - MESE_NASCITA
      - GIORNO_NASCITA
      - SESSO
      - ETA
      - NAZIONE_CODICE   (codice catastale/nazione dal CF)
      - NAZIONE_NASCITA  (ITALIA o nazione estera ricavata dai codici Z***)

    Se il codice di nascita NON è in elenco (o non è del tipo Z***),
    la NAZIONE_NASCITA viene impostata a 'ITALIA'.
    """

    MONTH_MAP = {
        "A": 1,  "B": 2,  "C": 3,  "D": 4,
        "E": 5,  "H": 6,  "L": 7,  "M": 8,
        "P": 9,  "R": 10, "S": 11, "T": 12,
    }

    # Mappa codici Z*** -> nome nazione (o territorio)
    COUNTRY_MAP = {
        # EUROPA
        "Z100": "Albania",
        "Z137": "Armenia",
        "Z141": "Azerbaigian",
        "Z101": "Andorra",
        "Z102": "Austria",
        "Z103": "Belgio",
        "Z139": "Bielorussia",
        "Z153": "Bosnia ed Erzegovina",
        "Z104": "Bulgaria",
        "Z156": "Ceca Repubblica",
        "Z105": "Cecoslovacchia",
        "Z106": "Citta' del Vaticano",
        "Z149": "Croazia",
        "Z107": "Danimarca",
        "Z144": "Estonia",
        "Z108": "Faer Oer (Isole)",
        "Z109": "Finlandia",
        "Z110": "Francia",
        "Z136": "Georgia",
        "Z111": "Germania Ovest",
        "Z112": "Germania Est",
        "Z113": "Gibilterra",
        "Z114": "Gran Bretagna",
        "Z115": "Grecia",
        "Z116": "Irlanda",
        "Z117": "Islanda",
        "Z152": "Kazakistan",
        "Z142": "Kirghizistan",
        "Z145": "Lettonia",
        "Z146": "Lituania",
        "Z119": "Liechtenstein",
        "Z120": "Lussemburgo",
        "Z148": "Macedonia",
        "Z121": "Malta (Isole)",
        "Z122": "Man (Isole)",
        "Z140": "Moldavia",
        "Z123": "Monaco",
        "Z124": "Normanne (Isole)",
        "Z125": "Norvegia",
        "Z126": "Paesi Bassi",
        "Z127": "Polonia",
        "Z128": "Portogallo",
        "Z129": "Romania",
        "Z154": "Russia",
        "Z130": "San Marino",
        "Z118": "Serbia e Montenegro",
        "Z155": "Slovacchia",
        "Z150": "Slovenia",
        "Z131": "Spagna",
        "Z132": "Svezia",
        "Z133": "Svizzera",
        "Z151": "Turkmenistan",
        "Z138": "Ucraina",
        "Z134": "Ungheria",
        "Z135": "U.R.S.S.",
        "Z143": "Uzbekistan",

        # ASIA / MEDIO ORIENTE (Z2**)
        "Z200": "Afghanistan",
        "Z201": "Arabia Meridionale",
        "Z252": "Armenia",
        "Z253": "Azerbaigian",
        "Z202": "Protettorato Arabia Mer.",
        "Z203": "Arabia Saudita",
        "Z204": "Bahrein",
        "Z205": "Bhutan",
        "Z206": "Birmania",
        "Z207": "Brunei",
        "Z208": "Cambogia",
        "Z210": "Cina Repubblica Popolare",
        "Z211": "Cipro",
        "Z212": "Cocos (Isole)",
        "Z213": "Repubblica Di Corea",
        "Z214": "Corea Repubblica Popolare",
        "Z215": "Emirati Arabi Uniti",
        "Z216": "Filippine",
        "Z217": "Cina Repubblica Nazionale",
        "Z218": "Territorio Di Gaza",
        "Z254": "Georgia",
        "Z219": "Giappone",
        "Z220": "Giordania",
        "Z221": "Hong Kong",
        "Z222": "India",
        "Z223": "Indonesia",
        "Z224": "Iran",
        "Z225": "Iraq",
        "Z226": "Israele",
        "Z255": "Kazakistan",
        "Z256": "Kirghizistan",
        "Z227": "Kuwait",
        "Z228": "Laos",
        "Z229": "Libano",
        "Z230": "Malesia",
        "Z231": "Macao",
        "Z232": "Maldive",
        "Z233": "Mongolia Rep. Popolare",
        "Z234": "Nepal",
        "Z235": "Oman",
        "Z236": "Pakistan",
        "Z237": "Qatar",
        "Z238": "Ryukyu (Isole)",
        "Z239": "Sikkim",
        "Z240": "Siria",
        "Z209": "Sri Lanka",
        "Z257": "Tagikistan",
        "Z241": "Thailandia",
        "Z242": "Timor (Isola)",
        "Z243": "Turchia",
        "Z258": "Turkmenistan",
        "Z259": "Uzbekistan",
        "Z244": "Vietnam Del Sud",
        "Z245": "Vietnam Del Nord",
        "Z246": "Yemen",
        "Z247": "Malaysia",
        "Z248": "Singapore",
        "Z249": "Bangladesh",
        "Z250": "Yemen Rep.Dem. Popolare",
        "Z251": "Vietnam",

        # AFRICA (Z3**)
        "Z300": "Namibia",
        "Z301": "Algeria",
        "Z302": "Angola",
        "Z303": "Basutoland Sud Africa B.",
        "Z304": "Beciuania Sud Africa B.",
        "Z305": "Burundi",
        "Z306": "Camerun",
        "Z307": "Capo Verde (Isole)",
        "Z308": "Impero Centroafricano",
        "Z309": "Chad",
        "Z310": "Comore (Isole)",
        "Z311": "Congo Repubblica Popolare",
        "Z312": "Congo Repubblica Democratica",
        "Z313": "Costa D'avorio",
        "Z314": "Dahomey",
        "Z315": "Etiopia",
        "Z316": "Gabon",
        "Z317": "Gambia",
        "Z318": "Ghana",
        "Z319": "Guinea",
        "Z320": "Guinea Bissau",
        "Z321": "Guinea Equatoriale",
        "Z322": "Kenya",
        "Z323": "Ifni",
        "Z324": "La Reunion (Isole)",
        "Z325": "Liberia",
        "Z326": "Libia",
        "Z327": "Madagascar",
        "Z328": "Malawi",
        "Z329": "Mali",
        "Z330": "Marocco",
        "Z331": "Mauritania",
        "Z332": "Maurizio (Isole)",
        "Z333": "Mozambico",
        "Z334": "Niger",
        "Z335": "Nigeria",
        "Z336": "Egitto",
        "Z337": "Rhodesia",
        "Z338": "Ruanda",
        "Z339": "Sahara Spagnolo",
        "Z340": "Sant'elena (Isola)",
        "Z341": "Principe (Isole)",
        "Z342": "Seicelle (Isole)",
        "Z343": "Senegal",
        "Z344": "Sierra Leone",
        "Z345": "Somalia",
        "Z346": "Somalia Francese",
        "Z347": "Repubblica Sudafricana",
        "Z348": "Sudan",
        "Z349": "Swaziland",
        "Z350": "Tanganica",
        "Z351": "Togo",
        "Z352": "Tunisia",
        "Z353": "Uganda",
        "Z354": "Altovolta",
        "Z355": "Zambia",
        "Z356": "Zanzibar",
        "Z357": "Tanzania",
        "Z358": "Botswana",
        "Z359": "Lesotho",
        "Z360": "Mayotte",
        "Z361": "Gibuti",
        "Z362": "Sahara Meridionale",
        "Z363": "Sahara Settentrionale",
        "Z368": "Eritrea",

        # NORD AMERICA / CARAIBI (Z4**, Z5**)
        "Z400": "Bermude (Isole)",
        "Z401": "Canada",
        "Z402": "Groenlandia",
        "Z403": "Saint Pierre et Miquelon",
        "Z404": "USA",
        "Z500": "Antille Britanniche",
        "Z501": "Antille Olandesi",
        "Z502": "Bahama (Isole)",
        "Z503": "Costa Rica",
        "Z504": "Cuba",
        "Z505": "Repubblica Dominicana",
        "Z506": "El Salvador",
        "Z507": "Giamaica",
        "Z508": "Guadalupa",
        "Z509": "Guatemala",
        "Z510": "Haiti",
        "Z511": "Honduras",
        "Z512": "Belize",
        "Z513": "Martinica",
        "Z514": "Messico",
        "Z515": "Nicaragua",
        "Z516": "Panama",
        "Z517": "Panama Zona Del Canale",
        "Z518": "Puerto Rico",
        "Z519": "Turks E Caicos (Isole)",
        "Z520": "Vergini (Isole) Americane",
        "Z522": "Barbados",
        "Z523": "Antille Britanniche",
        "Z524": "Grenada",

        # SUD AMERICA (Z6**)
        "Z600": "Argentina",
        "Z601": "Bolivia",
        "Z602": "Brasile",
        "Z603": "Cile",
        "Z604": "Colombia",
        "Z605": "Ecuador (Isole)",
        "Z606": "Repubblica Della Guayana",
        "Z607": "Guayana Francese",
        "Z608": "Suriname",
        "Z609": "Malvine (Isole)",
        "Z610": "Paraguay",
        "Z611": "Perù",
        "Z612": "Trinidad E Tobago",
        "Z613": "Uruguay",
        "Z614": "Venezuela",

        # OCEANIA / PACIFICO (Z7**)
        "Z700": "Australia",
        "Z701": "Caroline (Isole)",
        "Z702": "Christmas (Isole)",
        "Z703": "Cook (Isole)",
        "Z704": "Figi O Viti (Isole)",
        "Z705": "Gilbert E Ellice (Isole)",
        "Z706": "Guam (Isola)",
        "Z707": "Irian Occidentale",
        "Z708": "Macquarie (Isole)",
        "Z709": "Marcus (Isole)",
        "Z710": "Marianne (Isole)",
        "Z711": "Marshall (Isole)",
        "Z712": "Midway (Isole)",
        "Z713": "Nauru (Isole)",
        "Z714": "Niue O Savage (Isole)",
        "Z715": "Norfolk (Isole)",
        "Z716": "Nuova Caledonia (Isole)",
        "Z717": "Nuove Ebridi (Isole)",
        "Z718": "Nuova Guinea",
        "Z719": "Nuova Zelanda",
        "Z720": "Papuasia",
        "Z721": "Pasqua (Isola)",
        "Z722": "Pitcairn",
        "Z723": "Polinesia (Isole)",
        "Z724": "Salomone (Isole)",
        "Z725": "Samoa Americane (Isole)",
        "Z726": "Samoa Occidentali (Isole)",
        "Z727": "Tokelau (Isole)",
        "Z728": "Tonga O Degli Amici",
        "Z729": "Wallis E Futuna (Isole)",
        "Z730": "Papua Nuova Guinea",
        "Z734": "Palau",
        "Z735": "Micronesia Stati Federati",

        # DIPENDENZE (Z8**, Z9**)
        "Z800": "Dipendenze Canadesi",
        "Z801": "Dipendenze Norvegesi",
        "Z802": "Dipendenze Sovietiche",
        "Z900": "Dipendenze Australiane",
        "Z901": "Dipendenze Britanniche",
        "Z902": "Dipendenze Francesi",
        "Z903": "Dipendenze Neozelandesi",
        "Z905": "Dipendenze Statunitensi",
        "Z906": "Dipendenze Sudafricane",
    }

    def __init__(
        self,
        cf_column: str,
        output_prefix: str = "",
        reference_date: date | None = None,
    ):
        self.cf_column = cf_column
        self.output_prefix = (output_prefix or "").upper()
        self.reference_date = reference_date or date.today()
        self.logger = get_logger()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info(
            f"[AddCodiceFiscaleDetailsTransformer] Calcolo dati da CF "
            f"per colonna '{self.cf_column}' (reference_date={self.reference_date})"
        )

        if self.cf_column not in df.columns:
            self.logger.warning(
                f"[AddCodiceFiscaleDetailsTransformer] Colonna '{self.cf_column}' non trovata."
            )
            log_memory_usage("[AddCodiceFiscaleDetailsTransformer] post-transform (no-op)")
            return df

        cf_series = (
            df[self.cf_column]
            .astype(str)
            .str.strip()
            .str.upper()
        )

        mask_valid_len = cf_series.str.len() == 16
        valid_count = mask_valid_len.sum()
        total_count = len(cf_series)

        # nomi colonne output
        col_data_nascita = self.output_prefix + "DATA_NASCITA"
        col_anno_nascita = self.output_prefix + "ANNO_NASCITA"
        col_mese_nascita = self.output_prefix + "MESE_NASCITA"
        col_giorno_nascita = self.output_prefix + "GIORNO_NASCITA"
        col_sesso = self.output_prefix + "SESSO"
        col_eta = self.output_prefix + "ETA"
        col_nazione_codice = self.output_prefix + "NAZIONE_CODICE"
        col_nazione_nascita = self.output_prefix + "NAZIONE_NASCITA"

        # inizializza colonne
        df[col_data_nascita] = pd.NaT
        df[col_anno_nascita] = np.nan
        df[col_mese_nascita] = np.nan
        df[col_giorno_nascita] = np.nan
        df[col_sesso] = pd.NA
        df[col_eta] = np.nan
        df[col_nazione_codice] = pd.NA
        df[col_nazione_nascita] = "ITALIA"

        if valid_count > 0:
            cf_valid = cf_series[mask_valid_len]

            # Codice luogo/nazione di nascita (posizioni 11-14)
            birthplace_code = cf_valid.str[11:15]
            df.loc[mask_valid_len, col_nazione_codice] = birthplace_code

            # Mappatura su nazione: se non in elenco -> ITALIA
            country_series = birthplace_code.map(self.COUNTRY_MAP)
            df.loc[mask_valid_len, col_nazione_nascita] = country_series.fillna("Italia")

            # YY, M, GG
            yy = pd.to_numeric(cf_valid.str[6:8], errors="coerce")
            month = cf_valid.str[8:9].map(self.MONTH_MAP).astype("float")
            day_gender_raw = pd.to_numeric(cf_valid.str[9:11], errors="coerce")

            # sesso
            sex = pd.Series(pd.NA, index=cf_valid.index)
            sex.loc[day_gender_raw <= 40] = "M"
            sex.loc[day_gender_raw > 40] = "F"

            # giorno reale
            day = day_gender_raw.copy()
            day.loc[day_gender_raw > 40] = day_gender_raw.loc[day_gender_raw > 40] - 40

            # secolo
            current_year = self.reference_date.year
            current_yy = current_year % 100
            base_century = current_year - current_yy

            full_year = yy.copy()
            mask_same_century = yy <= current_yy
            full_year.loc[mask_same_century] = base_century + yy.loc[mask_same_century]
            full_year.loc[~mask_same_century] = (base_century - 100) + yy.loc[~mask_same_century]

            # data nascita
            birth_date = pd.to_datetime(
                dict(
                    year=full_year,
                    month=month,
                    day=day,
                ),
                errors="coerce",
            )

            # età
            ref_ts = pd.Timestamp(self.reference_date)
            age = ((ref_ts - birth_date).dt.days / 365.25).astype("float")
            age = np.floor(age)

            # scrittura risultati
            df.loc[mask_valid_len, col_data_nascita] = birth_date
            df.loc[mask_valid_len, col_anno_nascita] = full_year
            df.loc[mask_valid_len, col_mese_nascita] = month
            df.loc[mask_valid_len, col_giorno_nascita] = day
            df.loc[mask_valid_len, col_sesso] = sex
            df.loc[mask_valid_len, col_eta] = age

            self.logger.info(
                f"[AddCodiceFiscaleDetailsTransformer] CF validi (16 char): {valid_count}/{total_count}"
            )

        log_memory_usage("[AddCodiceFiscaleDetailsTransformer] post-transform")
        return df
