from abc import ABC, abstractmethod
import typing


class DataProcessor(ABC):
    def __init__(self) -> None:
        self.data: list[tuple[int, typing.Any]] = []
        self.rank: int = 0

    @abstractmethod
    def validate(self, data: typing.Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: typing.Any) -> None:
        pass

    def output(self) -> tuple[int, typing.Any]:
        if len(self.data) == 0:
            raise Exception("No data to be extracted!")
        return self.data.pop(0)


class NumericProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
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
    def validate(self, data: typing.Any) -> bool:
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
    def validate(self, data: typing.Any) -> bool:
        if type(data) is dict:
            for k, v in data.items():
                if type(k) is not str or type(v) is not str:
                    return False
            return True
        if type(data) is list:
            for s in data:
                if type(s) is not dict:
                    return False
                for k, v in s.items():
                    if type(k) is not str or type(v) is not str:
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


class DataStream:
    def __init__(self):
        self.processors = []

    def register_processor(self, proc: DataProcessor) -> None:
        self.processors.append(proc)

    def process_stream(self, stream: list[typing.Any]) -> None:
        try:
            for element in stream:
                handled = False
                for proc in self.processors:
                    if proc.validate(element):
                        proc.ingest(element)
                        handled = True
                        break
            if not handled:
                raise Exception(f"DataStream error - Can't process element in stream: {element}")
        except Exception as e:
            print(e)

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if not self.processors:
            print("No processor found, no data")
            return
        for proc in self.processors:
            name = type(proc)