from abc import ABC, abstractmethod

class BaseValidator(ABC):
    @abstractmethod
    def validate(self, value) -> bool:
        pass

    def error_message(self):
        return f"{self.__class__.__name__} failed"
