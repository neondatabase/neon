from _typeshed import Incomplete

class _BooleanConfigOption:
    name: Incomplete
    attr_name: Incomplete
    def __init__(self, name) -> None: ...
    def __get__(self, instance, owner): ...
    def __set__(self, instance, value) -> None: ...

class DummyLogger:
    def __init__(self, *vargs) -> None: ...
    def debug(self, *vargs, **kwargs) -> None: ...
    def trace(self, *vargs, **kwargs) -> None: ...

class OutputLogger:
    file: Incomplete
    trace_level: Incomplete
    def __init__(self, file: Incomplete | None = ..., trace_level: bool = ...) -> None: ...
    def debug(self, fmtstr, *args) -> None: ...
    def trace(self, fmtstr, *args) -> None: ...

class H2Configuration:
    client_side: Incomplete
    validate_outbound_headers: Incomplete
    normalize_outbound_headers: Incomplete
    validate_inbound_headers: Incomplete
    normalize_inbound_headers: Incomplete
    logger: Incomplete
    def __init__(
        self,
        client_side: bool = ...,
        header_encoding: Incomplete | None = ...,
        validate_outbound_headers: bool = ...,
        normalize_outbound_headers: bool = ...,
        validate_inbound_headers: bool = ...,
        normalize_inbound_headers: bool = ...,
        logger: Incomplete | None = ...,
    ) -> None: ...
    @property
    def header_encoding(self): ...
    @header_encoding.setter
    def header_encoding(self, value) -> None: ...
