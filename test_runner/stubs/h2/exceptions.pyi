from _typeshed import Incomplete

class H2Error(Exception): ...

class ProtocolError(H2Error):
    error_code: Incomplete

class FrameTooLargeError(ProtocolError):
    error_code: Incomplete

class FrameDataMissingError(ProtocolError):
    error_code: Incomplete

class TooManyStreamsError(ProtocolError): ...

class FlowControlError(ProtocolError):
    error_code: Incomplete

class StreamIDTooLowError(ProtocolError):
    stream_id: Incomplete
    max_stream_id: Incomplete
    def __init__(self, stream_id, max_stream_id) -> None: ...

class NoAvailableStreamIDError(ProtocolError): ...

class NoSuchStreamError(ProtocolError):
    stream_id: Incomplete
    def __init__(self, stream_id) -> None: ...

class StreamClosedError(NoSuchStreamError):
    stream_id: Incomplete
    error_code: Incomplete
    def __init__(self, stream_id) -> None: ...

class InvalidSettingsValueError(ProtocolError, ValueError):
    error_code: Incomplete
    def __init__(self, msg, error_code) -> None: ...

class InvalidBodyLengthError(ProtocolError):
    expected_length: Incomplete
    actual_length: Incomplete
    def __init__(self, expected, actual) -> None: ...

class UnsupportedFrameError(ProtocolError): ...
class RFC1122Error(H2Error): ...

class DenialOfServiceError(ProtocolError):
    error_code: Incomplete
