from pyflowetl.log import get_logger, log_memory_usage

class SetOutputColumnsTransformer:
    def __init__(self, columns, rename=False):
        """
        Transformer per impostare, ordinare e rinominare colonne in output.

        :param columns: lista di colonne da mantenere (e ordinare), oppure dict {old_name: new_name} se rename=True
        :param rename: se True, rinomina le colonne secondo il dizionario

        Esempio 1 – ordinamento colonne:
        transformer = SetOutputColumnsTransformer(columns=["cognome_nome", "telefono"])

        Esempio 2 – rinominare e ordinare:
        transformer = SetOutputColumnsTransformer(columns={"cognome_nome": "destinatario", "telefono": "cell"}, rename=True)
        """
        self.columns = columns
        self.rename = rename

    def transform(self, data):
        logger = get_logger()
        logger.info("[SetOutputColumnsTransformer] Inizio trasformazione")

        if self.rename:
            rename_map = self.columns
            missing = [col for col in rename_map if col not in data.columns]
            if missing:
                msg = f"[SetOutputColumnsTransformer] Colonne mancanti da rinominare: {missing}"
                logger.error(msg)
                raise ValueError(msg)
            data = data.rename(columns=rename_map)
            columns_to_keep = list(rename_map.values())
        else:
            columns_to_keep = self.columns
            missing = [col for col in columns_to_keep if col not in data.columns]
            if missing:
                msg = f"[SetOutputColumnsTransformer] Colonne mancanti da selezionare: {missing}"
                logger.error(msg)
                raise ValueError(msg)

        data = data[columns_to_keep]
        logger.info(f"[SetOutputColumnsTransformer] Colonne finali: {list(data.columns)}")
        log_memory_usage("Dopo SetOutputColumnsTransformer")
        return data


# ESEMPIO DI UTILIZZO
if __name__ == "__main__":
    import pandas as pd

    df = pd.DataFrame({
        "nome": ["Mario", "Luca"],
        "cognome": ["Rossi", "Bianchi"],
        "telefono": ["123", "456"]
    })

    from pyflowetl.transformers.concat_columns import ConcatColumnsTransformer

    df = ConcatColumnsTransformer(["cognome", "nome"], "cognome_nome").transform(df)

    transformer = SetOutputColumnsTransformer(columns=["cognome_nome", "telefono"])
    df_out = transformer.transform(df)

    print(df_out)
