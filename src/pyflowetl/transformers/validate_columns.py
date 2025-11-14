from pyflowetl.log import get_logger, log_memory_usage
import pandas as pd

logger = get_logger()

class ValidateColumnsTransformer:
    def __init__(self, rules: dict, reject_output_path=None, log_step=10000):
        """
        :param rules: dict del tipo {"colonna": [Validator1(), Validator2()]}
        :param reject_output_path: path CSV in cui salvare i record non validi
        :param log_step: ogni quante righe loggare l'avanzamento
        """
        self.rules = rules
        self.reject_output_path = reject_output_path
        self.log_step = log_step

    def transform(self, data):
        logger = get_logger()
        logger.info("[ValidateColumnsTransformer] Inizio validazione")

        valid_mask = pd.Series([True] * len(data), index=data.index)
        error_list = [[] for _ in range(len(data))]

        for col, validators in self.rules.items():
            values = data[col] if col in data.columns else pd.Series([None] * len(data))
            for validator in validators:
                failed_mask = ~values.apply(validator.validate)
                valid_mask &= ~failed_mask
                for i in data[failed_mask].index:
                    error_list[i].append(f"{col}: {validator.error_message()}")

        data["error"] = [" | ".join(errors) if errors else "" for errors in error_list]

        df_valid = data[valid_mask].drop(columns=["error"])
        df_rejected = data[~valid_mask]

        if self.reject_output_path and not df_rejected.empty:
            logger.warning(f"[ValidateColumnsTransformer] Scrittura righe non valide: {len(df_rejected)}")
            df_rejected.to_csv(self.reject_output_path, index=False, encoding="utf-8-sig", sep=";")

        logger.info(f"[ValidateColumnsTransformer] Validi: {len(df_valid)} / Invalidi: {len(df_rejected)}")
        log_memory_usage("Dopo ValidateColumnsTransformer")

        return df_valid
