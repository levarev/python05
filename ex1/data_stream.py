from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._ret: list[tuple[int, str]] = []
        self._index: int = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if not self._ret:
            raise TypeError()
        return self._ret.pop(0)


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if type(data) is list:
            for item in data:
                if type(item) is not float and type(item) is not int:
                    return False
        elif type(data) is not float and type(data) is not int:
            return False
        return True

    def ingest(self, data: Any) -> None:
        try:
            if type(data) is list:
                for x in data:
                    if type(x) is float or type(x) is int:
                        self._ret.append((self._index, str(x)))
                        self._index += 1
                    else:
                        raise TypeError()
            elif type(data) is float or type(data) is int:
                self._ret.append((self._index, str(data)))
                self._index += 1
            else:
                raise TypeError()
        except TypeError:
            print("Got exception: Improper numeric data")
            self._ret.clear()


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if type(data) is list:
            for item in data:
                if type(item) is not str:
                    return False
        elif type(data) is not str:
            return False
        return True

    def ingest(self, data: Any) -> None:
        try:
            if type(data) is list:
                for x in data:
                    if type(x) is str:
                        self._ret.append((self._index, x))
                        self._index += 1
                    else:
                        raise TypeError()
            elif type(data) is str:
                self._ret.append((self._index, data))
                self._index += 1
            else:
                raise TypeError()
        except TypeError:
            print("Got exception: Improper text data")
            self._ret.clear()


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if type(data) is list:
            for item in data:
                if type(item) is not dict:
                    return False
        elif type(data) is not dict:
            return False
        return True

    def ingest(self, data: Any) -> None:
        try:
            if type(data) is list:
                for x in data:
                    if type(x) is dict:
                        format = f"{x['log_level']}: {x['log_message']}"
                        self._ret.append((self._index, format))
                        self._index += 1
                    else:
                        raise TypeError()
            elif type(data) is dict:
                format = f"{data['log_level']}: {data['log_message']}"
                self._ret.append((self._index, format))
                self._index += 1
            else:
                raise TypeError()
        except TypeError:
            print("Got exception: Improper log data")
            self._ret.clear()


class DataStream():
    def __init__(self) -> None:
        self._processored: int = 0
        self._not: int = 0

    def register_processor(self, proc: DataProcessor) -> None:
        self._proc = proc()

    def process_stream(self, stream: list[typing.Any]) -> None:
        for x in stream:
            if self._proc.validate(x) is True:
                self._proc.ingest(x)
                self._processored += 1
            else:
                print(f"DataStream error - Can't process element in stream: {x}")
                self._not += 1
    
    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        print(f"{self._proc}: total {self._processored} items processed, remaining {self._not} on processor")


if __name__ == "__main__":
    print("=== Code Nexus - Data Processor ===\n")

    print("Testing Numeric Processor...")
    np = NumericProcessor()
    print(f" Trying to validate input '42': {np.validate(42)}")
    print(f" Trying to validate input 'Hello': {np.validate('Hello')}")
    print(" Test invalid ingestion of string 'foo' without prior validation:")
    try:
        np.ingest("foo")
    except TypeError as e:
        print(f" Got exception: {e}")
    print(" Processing data: [1, 2, 3, 4, 5]")
    print(" Extracting 3 values...")
    np.ingest([1, 2, 3, 4, 5])
    for i in range(3):
        rank, value = np.output()
        print(f" Numeric value {rank}: {value}")
    print("")

    print("Testing Text Processor...")
    tp = TextProcessor()
    print(f" Trying to validate input '42': {tp.validate(42)}")
    print(" Processing data: ['Hello', 'Nexus', 'World']")
    tp.ingest(['Hello', 'Nexus', 'World'])
    rank, value = tp.output()
    print(" Extracting 1 value...")
    print(f" Text value {rank}: {value}")
    print("")

    print("Testing Log Processor...")
    lp = LogProcessor()
    print(f" Trying to validate input 'Hello': {lp.validate('Hello')}")
    log_data = [
        {'log_level': 'NOTICE', 'log_message': 'Connection to server'},
        {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}
    ]
    print(f" Processing data: {log_data}")
    lp.ingest(log_data)
    print("Extracting 2 values...")
    for i in range(2):
        rank, value = lp.output()
        print(f" Log entry {rank}: {value}")
