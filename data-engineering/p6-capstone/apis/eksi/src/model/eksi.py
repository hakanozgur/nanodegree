from enum import Enum


class ProcessStatus(Enum):
    NOT_PROCESSED = 0
    PENDING = 1
    PROCESSED = 2
