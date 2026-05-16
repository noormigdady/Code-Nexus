from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self) -> None:
        self.data: list[tuple[int, Any]] = []
        self.rank: int = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, Any]:
        if len(self.data) == 0:
            raise Exception("No data to be extracted!")
        return self.data.pop(0)


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if type(data) in [int, float]:
            return True
        if isinstance(data, list):
            for num in data:
                if type(num) not in [int, float]:
                    return False
            return True
        return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        try:
            if not self.validate(data):
                raise ValueError("Improper numeric data")
            if isinstance(data, list):
                for i in data:
                    self.data.append((self.rank, str(i)))
                    self.rank += 1
            else:
                self.data.append((self.rank, str(data)))
                self.rank += 1
        except ValueError as e:
            print(e)


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if type(data) is str:
            return True
        if type(data) is list:
            for s in data:
                if type(s) is not str:
                    return False
            return True
        return False

    def ingest(self, data: str | list[str]) -> None:
        try:
            if not self.validate(data):
                raise ValueError("Improper string data")
            if type(data) is str:
                self.data.append((self.rank, data))
                self.rank += 1
            else:
                for s in data:
                    self.data.append((self.rank, s))
                    self.rank += 1
        except ValueError as e:
            print(e)


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if type(data) is dict:
            for k, v in data.items():
                if type(k) is not str or type(v) is not str:
                    return False
                if k != "log_level" and k != "log_message":
                    return False
            return True
        if type(data) is list:
            for s in data:
                if type(s) is not dict:
                    return False
                for k, v in s.items():
                    if type(k) is not str or type(v) is not str:
                        return False
                    if k != "log_level" and k != "log_message":
                        return False
            return True
        return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        try:
            if not self.validate(data):
                raise ValueError("Improper Log data")
            if type(data) is dict:
                self.data.append((self.rank, data))
                self.rank += 1
            else:
                for s in data:
                    self.data.append((self.rank, s))
                    self.rank += 1
        except ValueError as e:
            print(e)


class DataStream():
    def __init__(self):
        self.processors: list[DataStream] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self.processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        try:
            for element in stream:
                handled = False
                for proc in self.processors:
                    if proc.validate(element):
                        proc.ingest(element)
                        handled = True
                        break
            if not handled:
                raise Exception(f"DataStream error -"
                                f" Can't process element in stream: {element}")
        except Exception as e:
            print(e)

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if not self.processors:
            print("No processor found, no data")
            return
        for proc in self.processors:
            name = type(proc).__name__
            print(f"{name}: total {proc.rank} items processed,"
                  f" remaining {len(proc.data)} on processor")
            print()


def main() -> None:
    print("=== Code Nexus - Data Stream ===")
    print("\nInitialize Data Stream...")
    stream = DataStream()
    stream.print_processors_stats()

    print("Registering Numeric Processor")
    numeric = NumericProcessor()
    stream.register_processor(numeric)
    my_lst = ["Hello world", [3.14, -1, 2.71], [
        {
            "log_level": "WARNING",
            "log_message": "Telnet access! Use ssh instead"
        },
        {
            "log_level": "INFO",
            "log_message": "User wil is connected"

        }
    ], 42, ["Hi", "Five"]]
    print(f"Send first batch of data on stream: {my_lst}")
    stream.process_stream(my_lst)
    stream.print_processors_stats()

    print("\nRegistering other data processors")
    print("Send the same batch again")
    stream.process_stream(my_lst)


if __name__ == "__main__":
    main()
