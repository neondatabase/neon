from .exceptions import (
    FrameDataMissingError as FrameDataMissingError,
)
from .exceptions import (
    FrameTooLargeError as FrameTooLargeError,
)
from .exceptions import (
    ProtocolError as ProtocolError,
)

CONTINUATION_BACKLOG: int

class FrameBuffer:
    data: bytes
    max_frame_size: int
    def __init__(self, server: bool = ...) -> None: ...
    def add_data(self, data) -> None: ...
    def __iter__(self): ...
    def __next__(self): ...
