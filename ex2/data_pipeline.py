from abc import ABC, abstractmethod
from typing import Any
from typing import Protocol


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

    def ret_list(self) -> list[tuple[int, str]]:
        return (self._ret)


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


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        ...


class ExportCsv():
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("CSV Output:")
        print(",".join(item[1] for item in data))


class ExportJson():
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("Json Output:")
        body = ", ".join(f'"item_{item[0]}": "{item[1]}"' for item in data)
        print('{' + body + '}')


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
                print(f"DataStream error - Can't process element in stream:"
                      f"{x}")
            check = False

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        l: list[tuple[int, str]] = []
        for p in self._procs:
            if p.ret_ret() < nb:
                length = p.ret_ret()
            else:
                length = nb
            if p.ret_ret() > 0:
                for i in range(length):
                    l.append(p.output())
                plugin.process_output(l)
                l.clear()

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if len(self._procs) == 0:
            print("No processor found, no data")
        else:
            for x in self._procs:
                print(f"{x.__class__.__name__}: total {x.ret_index()}"
                      f" items processed, remaining "
                      f"{x.ret_ret()} on processor")


if __name__ == "__main__":
    a = DataStream()
    num = NumericProcessor()
    text = TextProcessor()
    log = LogProcessor()
    print("=== Code Nexus - Data Stream ===")
    print()
    a.print_processors_stats()
    lst = ['Hello world', [3.14, -1, 2.71],
           [{'log_level': 'WARNING',
            'log_message': 'Telnet access! Use ssh instead'},
            {'log_level': 'INFO', 'log_message': 'User wil is connected'}],
           42, ['Hi', 'five']]
    print(f"Send first batch of data {lst}")
    a.register_processor(num)
    a.register_processor(text)
    a.register_processor(log)
    a.process_stream(lst)
    a.print_processors_stats()
    print("\nSend 3 processed data from each processor to a CSV plugin:")
    a.output_pipeline(3, ExportCsv())
    print()
    a.print_processors_stats()
    lst1 = [21, ['I love AI', 'LLMs are wonderful', 'Stay healthy'],
            [{'log_level': 'ERROR', 'log_message': '500 server crash'},
            {'log_level': 'NOTICE', 'log_message':
             'Certificateexpires in 10 days'}],
            [32, 42, 64, 84, 128, 168], 'World hello']
    print(f"Send another batch of data: {lst1}")
    a.process_stream(lst1)
    a.print_processors_stats()
    print("\nSend 5 processed data from each processor to a JSON plugin:")
    a.output_pipeline(5, ExportJson())
