from dataclasses import dataclass


@dataclass
class SourceConfig:
    storage_account: str
    container_name: str
