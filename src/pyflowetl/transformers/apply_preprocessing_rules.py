from pyflowetl.log import get_logger, log_memory_usage
import pandas as pd

logger = get_logger()

class ApplyPreprocessingRulesTransformer:
    def __init__(self, rules: dict):
        """
        :param rules: dict del tipo { "colonna": [PreProcessor1(), PreProcessor2()] }

        Esempio:
        ApplyPreprocessingRulesTransformer({
            "CELL": [NormalizePhoneNumberPreProcessor()],
            "NOME": [ToUpperPreProcessor()]
        })
        """
        self.rules = rules

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        logger.info("[ApplyPreprocessingRulesTransformer] Inizio preprocessing a colonne")

        for column, processors in self.rules.items():
            if column not in data.columns:
                msg = f"Colonna '{column}' non presente nel DataFrame"
                logger.warning(msg)
                raise  ValueError(msg)
                continue
            for processor in processors:
                logger.info(f"  Applico {processor.__class__.__name__} su '{column}'")
                data[column] = data[column].apply(processor.apply_to_value)

        log_memory_usage("Dopo ApplyPreprocessingRulesTransformer")
        return data


# ESEMPIO
if __name__ == "__main__":
    import pandas as pd
    from pyflowetl.preprocessors.normalize_phone import NormalizePhoneNumberPreProcessor
   # from pyflowetl.preprocessors.to_upper import ToUpperPreProcessor

    df = pd.DataFrame({
        "CELL": ["+39 335 1234567", "00393351234567", "333-123-4567"],
        "NOME": ["mario", "luca", "paola"]
    })

    transformer = ApplyPreprocessingRulesTransformer({
        "CELL": [NormalizePhoneNumberPreProcessor()],
    #    "NOME": [ToUpperPreProcessor()]
    })

    df = transformer.transform(df)
    print(df)
