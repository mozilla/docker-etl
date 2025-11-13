from dataclasses import dataclass


@dataclass
class Config:
    write: bool
    stage: bool
