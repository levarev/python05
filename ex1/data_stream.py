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
