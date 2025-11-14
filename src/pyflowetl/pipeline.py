from copy import deepcopy
from pyflowetl.log import get_logger, log_memory_usage
import pandas as pd

from pyflowetl.transformers.custom_sql_filter import CustomSqlFilterTransformer
from pyflowetl.transformers.filter import FilterTransformer


class EtlPipeline:
    def __init__(self, data=None):
        if isinstance(data, EtlPipeline):
            raise TypeError("[EtlPipeline] Non puoi passare una pipeline come data. Devi passare un DataFrame!")
        self.data = data

    def extract(self, extractor):
        self.data = extractor.extract()
        log_memory_usage("[EtlPipeline] dopo extract")
        return self

    def preprocess(self, preprocessor):
        self.data = preprocessor.process(self.data)
        log_memory_usage("[EtlPipeline] dopo preprocess")
        return self

    def transform(self, transformer):
        self.data = transformer.transform(self.data)
        log_memory_usage("[EtlPipeline] dopo transform")
        return self

    def transform_and_get_df(self, transformer):
        """
        Applica il transformer e ritorna solo il DataFrame risultante
        SENZA modificare la pipeline originale
        """
        return transformer.transform(self.data)

    def sql_filter(self, filter_expression: str) -> "EtlPipeline":
        """
        Applica un filtro usando FilterTransformer e ritorna una NUOVA pipeline coi dati filtrati.
        """
        df_filtrato = self.transform_and_get_df(CustomSqlFilterTransformer(filter_expression))
        return EtlPipeline(data=df_filtrato)

    def filter(self, filter_expression: str) -> "EtlPipeline":
        """
        Applica un filtro usando FilterTransformer e ritorna una NUOVA pipeline coi dati filtrati.
        """
        df_filtrato = self.transform_and_get_df(FilterTransformer(filter_expression))
        return EtlPipeline(data=df_filtrato)

    def load(self, loader):
        loader.load(self.data)
        log_memory_usage("[EtlPipeline] dopo load")
        return self

    def clone(self):
        new_pipeline = EtlPipeline()
        if self.data is not None:
            new_pipeline.data = self.data.copy(deep=True)
        return new_pipeline

    def split(self, flow_names: tuple[str], row_selector_fn) -> dict:
        """
        Divide i dati della pipeline in più sottopipeline in base a una funzione di routing per riga.
        :param flow_names: tuple con i nomi delle nuove pipeline (es. ("a", "b", "c"))
        :param row_selector_fn: funzione che prende una riga e restituisce uno dei nomi della tupla
        def instradamento_personalizzato(row):
            if row["PROVINCIA"] == "Napoli":
                return "napoli"
            elif row["PROVINCIA"] == "Roma":
                return "roma"
            else:
                return "altre"

        :return: dict {flow_name: EtlPipeline con subset dei dati}
        """
        logger = get_logger()
        if self.data is None:
            raise RuntimeError("Nessun dato disponibile nella pipeline. Hai dimenticato extract()?")

        logger.info(f"[EtlPipeline] Split su flussi: {flow_names}")
        data_by_key = {name: [] for name in flow_names}

        for _, row in self.data.iterrows():
            key = row_selector_fn(row)
            if key in data_by_key:
                data_by_key[key].append(row)
            else:
                logger.warning(f"[EtlPipeline] Chiave '{key}' non prevista in flow_names. Riga ignorata.")

        result = {}
        for key in flow_names:
            df = pd.DataFrame(data_by_key[key])
            new_pipeline = EtlPipeline()
            new_pipeline.data = df
            result[key] = new_pipeline
            logger.info(f"[EtlPipeline] Creato sottopipeline '{key}' con {len(df)} righe.")

        return result

    def join_with(
            self,
            other_pipeline: "EtlPipeline",
            how: str = "left",
            on: str | list[str] = None,
            left_on: str | list[str] = None,
            right_on: str | list[str] = None,
            suffixes: tuple = ("", "_right")
    ) -> "EtlPipeline":
        """
        Esegue una join tra due pipeline e restituisce una nuova pipeline con i dati uniti.

        :param other_pipeline: Un'altra istanza di EtlPipeline.
        :param how: Tipo di join (left, right, inner, outer).
        :param on: Colonna o lista di colonne comuni su cui effettuare la join.
        :param left_on: Colonna o lista di colonne del dataframe di sinistra.
        :param right_on: Colonna o lista di colonne del dataframe di destra.
        :param suffixes: Suffissi da applicare in caso di colonne duplicate.
        :return: Nuova istanza di EtlPipeline con i dati uniti.
        """
        logger = get_logger()
        logger.info(f"[EtlPipeline.join_with] Join tipo '{how}' tra due pipeline")

        if self.data is None or other_pipeline.data is None:
            raise ValueError("Entrambe le pipeline devono contenere dati")

        # Determina le colonne coinvolte nella join
        if on is not None:
            join_cols_left = join_cols_right = on
        elif left_on is not None and right_on is not None:
            join_cols_left = left_on
            join_cols_right = right_on
        else:
            raise ValueError("Specificare 'on' oppure sia 'left_on' che 'right_on'")

        # Cast a string per evitare errori di dtype
        def force_str(df: pd.DataFrame, cols: str | list[str]) -> None:
            if isinstance(cols, str):
                df[cols] = df[cols].astype(str)
            else:
                for col in cols:
                    df[col] = df[col].astype(str)

        force_str(self.data, join_cols_left)
        force_str(other_pipeline.data, join_cols_right)

        merged_df = pd.merge(
            self.data,
            other_pipeline.data,
            how=how,
            on=on,
            left_on=left_on,
            right_on=right_on,
            suffixes=suffixes
        )

        logger.info(f"[EtlPipeline.join_with] Righe post-join: {len(merged_df)}")
        log_memory_usage("[EtlPipeline.join_with] post-join")

        new_pipeline = EtlPipeline()
        new_pipeline.data = merged_df
        return new_pipeline

    def anti_join_with(
            self,
            other_pipeline: "EtlPipeline",
            on: str | list[str] = None,
            left_on: str | list[str] = None,
            right_on: str | list[str] = None
    ) -> "EtlPipeline":
        """
        Esegue una anti-join tra due pipeline, restituendo solo le righe presenti
        nella pipeline corrente (left) ma non in quella destra (other).

        Effettua il cast delle colonne di join a stringa per evitare errori di tipo.

        :param other_pipeline: pipeline con cui escludere le righe in comune
        :param on: nome della colonna (o lista) su cui fare il confronto
        :param left_on: nome della colonna (o lista) nella pipeline corrente
        :param right_on: nome della colonna (o lista) nella pipeline other
        :return: nuova pipeline contenente solo righe "left-only"
        """
        logger = get_logger()
        logger.info("[EtlPipeline.anti_join_with] Avvio anti join (left only)")

        if self.data is None or other_pipeline.data is None:
            raise ValueError("Entrambe le pipeline devono contenere dati")

        left_df = self.data.copy()
        right_df = other_pipeline.data.copy()

        # Cast a stringa delle colonne usate per la join
        if on:
            if isinstance(on, str):
                on = [on]
            for col in on:
                left_df[col] = left_df[col].astype(str)
                right_df[col] = right_df[col].astype(str)
        else:
            if isinstance(left_on, str):
                left_on = [left_on]
            if isinstance(right_on, str):
                right_on = [right_on]
            for lcol, rcol in zip(left_on, right_on):
                left_df[lcol] = left_df[lcol].astype(str)
                right_df[rcol] = right_df[rcol].astype(str)

        initial_rows = len(left_df)

        merged = pd.merge(
            left_df,
            right_df,
            how="left",
            indicator=True,
            on=on,
            left_on=left_on,
            right_on=right_on
        )

        filtered = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])
        remaining_rows = len(filtered)
        dropped_rows = initial_rows - remaining_rows

        logger.info(f"[EtlPipeline.anti_join_with] Righe escluse: {dropped_rows}")
        logger.info(f"[EtlPipeline.anti_join_with] Righe superstiti: {remaining_rows}")

        new_pipeline = EtlPipeline()
        new_pipeline.data = filtered
        log_memory_usage("[EtlPipeline.anti_join_with] post-anti-join")
        return new_pipeline

    def log_dataframe_structure(self):
        """
        Logga la struttura del DataFrame (colonne, tipi di dato, conteggio valori non nulli),
        in modo simile al DDL di una tabella.
        """
        logger = get_logger()

        if self.data is None:
            logger.warning("[EtlPipeline] Nessun DataFrame caricato.")
            return

        if not isinstance(self.data, pd.DataFrame):
            logger.error("[EtlPipeline] Il dato corrente non è un DataFrame.")
            return

        logger.info(f"[EtlPipeline] Struttura DataFrame: {len(self.data)} record")
        logger.info("----------------------------------------------------")
        for col in self.data.columns:
            dtype = self.data[col].dtype
            not_null_count = self.data[col].notnull().sum()
            logger.info(f" - {col:<30} {str(dtype):<15} NOT NULL: {not_null_count}/{len(self.data)}")
        logger.info("----------------------------------------------------")