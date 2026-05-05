from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self):
        self._ret: str = ()

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass
    
    def output(self) -> tuple[int, str]:
        return  self._ret


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        try:
            float(data)
            return True
        except ValueError:
            return False

    def ingest(self, data: Any) -> None:
        try:
            if type(data) == list:
                for x in data:
                    if type(x) == float or '.' in data:
                        self._ret = self._ret + (str(float(x)),)
                    else:
                        self._ret = self._ret + (str(int(x)),)
            elif type(data) == float or '.' in data:
                self._ret = self._ret + (str(float(data),))
            else:
                self._ret = self._ret + (str(int(data),))
        except ValueError:
            print("Got exception: Improper numeric data")


a = NumericProcessor()
a.ingest([1,2,3,4,5])
print(a.output())
