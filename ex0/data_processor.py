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
            return (
                "log_level" in data
                and "log_message" in data
                and isinstance(data["log_level"], str)
                and isinstance(data["log_message"], str)
            )
        if type(data) is list:
            return all(
                isinstance(d, dict)
                and "log_level" in d
                and "log_message" in d
                and isinstance(d["log_level"], str)
                and isinstance(d["log_message"], str)
                for d in data
            )
        return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if not self.validate(data):
            raise ValueError("Improper Log data")
        entries: list[dict[str, str]]
        if isinstance(data, dict):
            entries = [data]
        else:
            entries = data
        for entry in entries:
            formatted = f"{entry['log_level']}: {entry['log_message']}"
            self.data.append((self.rank, formatted))
            self.rank += 1


def main() -> None:
    print("=== Code Nexus - Data Processor ===")

    print("\nTesting Numeric Processor...")
    obj1 = NumericProcessor()
    lst_num: list[int | float] = [1, 2, 3, 4, 5]

    print(f"Trying to validate input '42' : {obj1.validate(42)}")
    print(f"Trying to validate input 'Hello' : {obj1.validate("Hello")}")
    print("Test invalid ingestion of string 'foo' without prior validation:")
    try:
        obj1.ingest("foo")
    except ValueError as e:
        print(f"Got exception {e}")

    print(f"Processing data: {lst_num}")
    if obj1.validate(lst_num):
        obj1.ingest(lst_num)
    else:
        print("Invalid data!")

    print("Extracting 3 values...")
    try:
        for i in range(3):
            rank, item = obj1.output()
            print(f"Numeric value {rank}: {item}")
    except Exception as e:
        print(e)

    print("\nTesting Text Processor...")
    obj2 = TextProcessor()
    lst_str = ["Hello", "Nexus", "World"]

    print(f"Trying to validate input ’42’: {obj2.validate(42)}")
    print(f"Trying to validate input ’Hello’: {obj2.validate("Hello")}")
    print("Test invalid ingestion of number '100' without prior validation:")
    try:
        obj2.ingest(100)
    except ValueError as e:
        print(f"Got exception {e}")

    print(f"Processing data: {lst_str}")
    if obj2.validate(lst_str):
        obj2.ingest(lst_str)
    else:
        print("Invalid data")
    print("Extracting 1 value...")
    try:
        rank, item = obj2.output()
        print(f"Text value {rank}: {item}")
    except Exception as e:
        print(e)

    print("\nTesting Log Processor...")
    obj3 = LogProcessor()
    lst_dict = [
        {
            "log_level": "NOTICE",
            "log_message": "Connection to server"
        },
        {
            "log_level": "ERROR",
            "log_message": "Unauthorized access!!"
        }
    ]
    print(f"Trying to validate input 'Hello': {obj3.validate("Hello")}")
    print(f"Trying to validate input 'Hello : world': "
          f"{obj3.validate({"Hello": "world"})}")

    print(f"Trying to validate input 'log_level' : 'Error', 'log_message':'mm'"
          f": {obj3.validate({"log_level": "Error", "log_message": "mm"})}")
    print("Test invalid ingestion of string '42 Irbid' without validation:")
    try:
        obj3.ingest("42 Irbid")
    except ValueError as e:
        print(f"Got exception: {e}")

    print(f"Processing data: {lst_dict}")
    if obj3.validate(lst_dict):
        obj3.ingest(lst_dict)
    else:
        print("Invalid data")
    print("Extracting 2 values...")
    try:
        for _ in range(2):
            rank, item = obj3.output()
            print(f"Log entry {rank}:  {item}")
    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()
