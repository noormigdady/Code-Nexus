from abc import ABC, abstractmethod
from typing import Any, Protocol


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
        if not self.validate(data):
            raise ValueError("Improper numeric data")
        if isinstance(data, list):
            for i in data:
                self.data.append((self.rank, str(i)))
                self.rank += 1
        else:
            self.data.append((self.rank, str(data)))
            self.rank += 1


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
        if not self.validate(data):
            raise ValueError("Improper string data")
        if type(data) is str:
            self.data.append((self.rank, data))
            self.rank += 1
        else:
            for s in data:
                self.data.append((self.rank, s))
                self.rank += 1


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
        if not self.validate(data):
            raise ValueError("Improper Log data")
        if isinstance(data, dict):
            formatted = f"{data['log_level']}: {data['log_message']}"
            self.data.append((self.rank, formatted))
            self.rank += 1
        else:
            for log in data:
                formatted = f"{log['log_level']}: {log['log_message']}"
                self.data.append((self.rank, formatted))
                self.rank += 1


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        ...


class CSVPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        output = []
        for _, val in data:
            output.append(val)
        print("CSV Output:")
        print(",".join(output))


class JSONPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        output = []
        for rank, val in data:
            output.append(f'"item_{rank}": "{val}"')
        print("JSON Output")
        print("{" + ",".join(output) + "}")


class DataStream():
    def __init__(self) -> None:
        self.processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self.processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for element in stream:
            handled = False
            for proc in self.processors:
                if proc.validate(element):
                    proc.ingest(element)
                    handled = True
                    break
            if not handled:
                print(f"DataStream error -"
                      f" Can't process element in stream: {element}")

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if not self.processors:
            print("No processor found, no data")
            return
        for proc in self.processors:
            name = type(proc).__name__
            print(f"{name}: total {proc.rank} items processed,"
                  f" remaining {len(proc.data)} on processor")

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for proc in self.processors:
            collected_data = []
            for _ in range(nb):
                try:
                    item = proc.output()
                    collected_data.append(item)
                except Exception:
                    break
            if collected_data:
                plugin.process_output(collected_data)


def main() -> None:
    print("=== Code Nexus - Data Pipeline ===")
    print("\nInitialize Data Stream...")
    stream1 = DataStream()
    stream1.print_processors_stats()

    print("\nRegistering Processors\n")
    numeric = NumericProcessor()
    text = TextProcessor()
    log = LogProcessor()
    stream1.register_processor(numeric)
    stream1.register_processor(text)
    stream1.register_processor(log)
    lst1 = ["Hello world", [3.14, -1, 2.71], [
        {
            "log_level": "WARNING",
            "log_message": "Telnet access! Use ssh instead"
        },
        {
            "log_level": "INFO",
            "log_message": "User wil is connected"

        }
    ], 42, ["Hi", "Five"]]
    print(f"Send first batch of data on stream: {lst1}")
    stream1.process_stream(lst1)
    stream1.print_processors_stats()
    print()
    print("Send 3 processed data from each processor to a CSV plugin:")
    stream1.output_pipeline(3, CSVPlugin())
    print()
    stream1.print_processors_stats()

    lst2 = [21,
            ["I love AI", "LLMs are wonderful", "Stay healthy"],
            [
                {
                    "log_level": "ERROR",
                    "log_message": "500 server crash"
                },
                {
                    "log_level": "NOTICE",
                    "log_message": "Certificate expires in 10 days"

                }
            ],  [32, 42, 64, 84, 128, 168], "World Hello"
            ]
    print(f"Send another batch of data: {lst2}")
    stream1.process_stream(lst2)
    print()
    stream1.print_processors_stats()
    print()
    print("Send 5 processed data from each processor to a JSON plugin:")
    stream1.output_pipeline(5, JSONPlugin())
    print()
    stream1.print_processors_stats()


if __name__ == "__main__":
    main()
