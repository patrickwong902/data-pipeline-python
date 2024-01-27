from dataclasses import dataclass


@dataclass
class SinkConfig:
    mode: str
    storage_account: str
    container_name: str