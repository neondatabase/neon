from enum import Enum, IntEnum

from _typeshed import Incomplete

from .config import H2Configuration as H2Configuration
from .errors import ErrorCodes as ErrorCodes
from .events import AlternativeServiceAvailable as AlternativeServiceAvailable
from .events import ConnectionTerminated as ConnectionTerminated
from .events import PingAckReceived as PingAckReceived
from .events import PingReceived as PingReceived
from .events import PriorityUpdated as PriorityUpdated
from .events import RemoteSettingsChanged as RemoteSettingsChanged
from .events import SettingsAcknowledged as SettingsAcknowledged
from .events import UnknownFrameReceived as UnknownFrameReceived
from .events import WindowUpdated as WindowUpdated
from .exceptions import DenialOfServiceError as DenialOfServiceError
from .exceptions import FlowControlError as FlowControlError
from .exceptions import FrameTooLargeError as FrameTooLargeError
from .exceptions import NoAvailableStreamIDError as NoAvailableStreamIDError
from .exceptions import NoSuchStreamError as NoSuchStreamError
from .exceptions import ProtocolError as ProtocolError
from .exceptions import RFC1122Error as RFC1122Error
from .exceptions import StreamClosedError as StreamClosedError
from .exceptions import StreamIDTooLowError as StreamIDTooLowError
from .exceptions import TooManyStreamsError as TooManyStreamsError
from .frame_buffer import FrameBuffer as FrameBuffer
from .settings import SettingCodes as SettingCodes
from .settings import Settings as Settings
from .stream import H2Stream as H2Stream
from .stream import StreamClosedBy as StreamClosedBy
from .utilities import guard_increment_window as guard_increment_window
from .windows import WindowManager as WindowManager

class ConnectionState(Enum):
    IDLE: int
    CLIENT_OPEN: int
    SERVER_OPEN: int
    CLOSED: int

class ConnectionInputs(Enum):
    SEND_HEADERS: int
    SEND_PUSH_PROMISE: int
    SEND_DATA: int
    SEND_GOAWAY: int
    SEND_WINDOW_UPDATE: int
    SEND_PING: int
    SEND_SETTINGS: int
    SEND_RST_STREAM: int
    SEND_PRIORITY: int
    RECV_HEADERS: int
    RECV_PUSH_PROMISE: int
    RECV_DATA: int
    RECV_GOAWAY: int
    RECV_WINDOW_UPDATE: int
    RECV_PING: int
    RECV_SETTINGS: int
    RECV_RST_STREAM: int
    RECV_PRIORITY: int
    SEND_ALTERNATIVE_SERVICE: int
    RECV_ALTERNATIVE_SERVICE: int

class AllowedStreamIDs(IntEnum):
    EVEN: int
    ODD: int

class H2ConnectionStateMachine:
    state: Incomplete
    def __init__(self) -> None: ...
    def process_input(self, input_): ...

class H2Connection:
    DEFAULT_MAX_OUTBOUND_FRAME_SIZE: int
    DEFAULT_MAX_INBOUND_FRAME_SIZE: Incomplete
    HIGHEST_ALLOWED_STREAM_ID: Incomplete
    MAX_WINDOW_INCREMENT: Incomplete
    DEFAULT_MAX_HEADER_LIST_SIZE: Incomplete
    MAX_CLOSED_STREAMS: Incomplete
    state_machine: Incomplete
    streams: Incomplete
    highest_inbound_stream_id: int
    highest_outbound_stream_id: int
    encoder: Incomplete
    decoder: Incomplete
    config: Incomplete
    local_settings: Incomplete
    remote_settings: Incomplete
    outbound_flow_control_window: Incomplete
    max_outbound_frame_size: Incomplete
    max_inbound_frame_size: Incomplete
    incoming_buffer: Incomplete
    def __init__(self, config: Incomplete | None = ...) -> None: ...
    @property
    def open_outbound_streams(self): ...
    @property
    def open_inbound_streams(self): ...
    @property
    def inbound_flow_control_window(self): ...
    def initiate_connection(self) -> None: ...
    def initiate_upgrade_connection(self, settings_header: Incomplete | None = ...): ...
    def get_next_available_stream_id(self): ...
    def send_headers(
        self,
        stream_id,
        headers,
        end_stream: bool = ...,
        priority_weight: Incomplete | None = ...,
        priority_depends_on: Incomplete | None = ...,
        priority_exclusive: Incomplete | None = ...,
    ) -> None: ...
    def send_data(
        self, stream_id, data, end_stream: bool = ..., pad_length: Incomplete | None = ...
    ) -> None: ...
    def end_stream(self, stream_id) -> None: ...
    def increment_flow_control_window(
        self, increment, stream_id: Incomplete | None = ...
    ) -> None: ...
    def push_stream(self, stream_id, promised_stream_id, request_headers) -> None: ...
    def ping(self, opaque_data) -> None: ...
    def reset_stream(self, stream_id, error_code: int = ...) -> None: ...
    def close_connection(
        self,
        error_code: int = ...,
        additional_data: Incomplete | None = ...,
        last_stream_id: Incomplete | None = ...,
    ) -> None: ...
    def update_settings(self, new_settings) -> None: ...
    def advertise_alternative_service(
        self, field_value, origin: Incomplete | None = ..., stream_id: Incomplete | None = ...
    ) -> None: ...
    def prioritize(
        self,
        stream_id,
        weight: Incomplete | None = ...,
        depends_on: Incomplete | None = ...,
        exclusive: Incomplete | None = ...,
    ) -> None: ...
    def local_flow_control_window(self, stream_id): ...
    def remote_flow_control_window(self, stream_id): ...
    def acknowledge_received_data(self, acknowledged_size, stream_id) -> None: ...
    def data_to_send(self, amount: Incomplete | None = ...): ...
    def clear_outbound_data_buffer(self) -> None: ...
    def receive_data(self, data): ...
