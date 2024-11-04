import enum

class ErrorCodes(enum.IntEnum):
    NO_ERROR: int
    PROTOCOL_ERROR: int
    INTERNAL_ERROR: int
    FLOW_CONTROL_ERROR: int
    SETTINGS_TIMEOUT: int
    STREAM_CLOSED: int
    FRAME_SIZE_ERROR: int
    REFUSED_STREAM: int
    CANCEL: int
    COMPRESSION_ERROR: int
    CONNECT_ERROR: int
    ENHANCE_YOUR_CALM: int
    INADEQUATE_SECURITY: int
    HTTP_1_1_REQUIRED: int
