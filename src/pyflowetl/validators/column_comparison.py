from pyflowetl.validators.base import BaseValidator

class ColumnComparisonValidator(BaseValidator):
    def __init__(self, operator: str, value):
        self.operator = operator
        self.value = value

    def validate(self, value, row=None):
        try:
            if self.operator == "==":
                return value == self.value
            elif self.operator == "!=":
                return value != self.value
            elif self.operator == ">":
                return value > self.value
            elif self.operator == ">=":
                return value >= self.value
            elif self.operator == "<":
                return value < self.value
            elif self.operator == "<=":
                return value <= self.value
            else:
                return False
        except Exception:
            return False

    def error_message(self):
        return f"Valore non soddisfa la condizione {self.operator} {self.value}"
