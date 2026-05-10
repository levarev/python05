from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._ret: list[tuple[int, str]] = []
        self._index: int = 0
        self._not: int = 0

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

    def ret_index(self) -> int:
        return self._index

    def ret_ret(self) -> int:
        return len(self._ret)


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
        self._procs: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self._procs.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        check = False
        for x in stream:
            for proc in self._procs:
                if proc.validate(x) is True:
                    proc.ingest(x)
                    check = True
            if check is False:
                print(f"DataStream error - Can't process element in stream: {x}")
            check = False
    
    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if len(self._procs) == 0:
            print("No processor found, no data")
        else:
            for x in self._procs:
                print(f"{x.__class__.__name__}: total {x.ret_index()} items processed, remaining {x.ret_ret()} on processor")


if __name__ == "__main__":
    a = DataStream()
    num = NumericProcessor()
    text = TextProcessor()
    log = LogProcessor()
    print('=== Code Nexus - Data Stream ===')
    a.print_processors_stats()
    print("\nRegistering Numeric Processor\n")
    lst = ['Hello world', [3.14, -1, 2.71], [{'log_level': 'WARNING', 'log_message': 'Telnet access! Use ssh instead'},
            {'log_level': 'INFO', 'log_message': 'User wil is connected'}], 42, ['Hi', 'five']]
    print(f"Send first batch of data {lst}")
    a.register_processor(num)
    a.process_stream(lst)
    a.print_processors_stats()
    print("\nRegistering other data processors")
    a.register_processor(text)
    a.register_processor(log)
    a.process_stream(lst)
    a.print_processors_stats()
    print("\nConsume some elements from the data processors: Numeric 3, Text 2, Log 1")
    for _ in range(3):
        num.output()
    for _ in range(2):
        text.output()
    for _ in range(1):
        log.output()
    a.print_processors_stats()
