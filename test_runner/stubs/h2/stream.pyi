from enum import Enum, IntEnum

from _typeshed import Incomplete

from .errors import ErrorCodes as ErrorCodes
from .events import (
    AlternativeServiceAvailable as AlternativeServiceAvailable,
)
from .events import (
    DataReceived as DataReceived,
)
from .events import (
    InformationalResponseReceived as InformationalResponseReceived,
)
from .events import (
    PushedStreamReceived as PushedStreamReceived,
)
from .events import (
    RequestReceived as RequestReceived,
)
from .events import (
    ResponseReceived as ResponseReceived,
)
from .events import (
    StreamEnded as StreamEnded,
)
from .events import (
    StreamReset as StreamReset,
)
from .events import (
    TrailersReceived as TrailersReceived,
)
from .events import (
    WindowUpdated as WindowUpdated,
)
from .exceptions import (
    FlowControlError as FlowControlError,
)
from .exceptions import (
    InvalidBodyLengthError as InvalidBodyLengthError,
)
from .exceptions import (
    ProtocolError as ProtocolError,
)
from .exceptions import (
    StreamClosedError as StreamClosedError,
)
from .utilities import (
    HeaderValidationFlags as HeaderValidationFlags,
)
from .utilities import (
    authority_from_headers as authority_from_headers,
)
from .utilities import (
    extract_method_header as extract_method_header,
)
from .utilities import (
    guard_increment_window as guard_increment_window,
)
from .utilities import (
    is_informational_response as is_informational_response,
)
from .utilities import (
    normalize_inbound_headers as normalize_inbound_headers,
)
from .utilities import (
    normalize_outbound_headers as normalize_outbound_headers,
)
from .utilities import (
    validate_headers as validate_headers,
)
from .utilities import (
    validate_outbound_headers as validate_outbound_headers,
)
from .windows import WindowManager as WindowManager

class StreamState(IntEnum):
    IDLE: int
    RESERVED_REMOTE: int
    RESERVED_LOCAL: int
    OPEN: int
    HALF_CLOSED_REMOTE: int
    HALF_CLOSED_LOCAL: int
    CLOSED: int

class StreamInputs(Enum):
    SEND_HEADERS: int
    SEND_PUSH_PROMISE: int
    SEND_RST_STREAM: int
    SEND_DATA: int
    SEND_WINDOW_UPDATE: int
    SEND_END_STREAM: int
    RECV_HEADERS: int
    RECV_PUSH_PROMISE: int
    RECV_RST_STREAM: int
    RECV_DATA: int
    RECV_WINDOW_UPDATE: int
    RECV_END_STREAM: int
    RECV_CONTINUATION: int
    SEND_INFORMATIONAL_HEADERS: int
    RECV_INFORMATIONAL_HEADERS: int
    SEND_ALTERNATIVE_SERVICE: int
    RECV_ALTERNATIVE_SERVICE: int
    UPGRADE_CLIENT: int
    UPGRADE_SERVER: int

class StreamClosedBy(Enum):
    SEND_END_STREAM: int
    RECV_END_STREAM: int
    SEND_RST_STREAM: int
    RECV_RST_STREAM: int

STREAM_OPEN: Incomplete

class H2StreamStateMachine:
    state: Incomplete
    stream_id: Incomplete
    client: Incomplete
    headers_sent: Incomplete
    trailers_sent: Incomplete
    headers_received: Incomplete
    trailers_received: Incomplete
    stream_closed_by: Incomplete
    def __init__(self, stream_id) -> None: ...
    def process_input(self, input_): ...
    def request_sent(self, previous_state): ...
    def response_sent(self, previous_state): ...
    def request_received(self, previous_state): ...
    def response_received(self, previous_state): ...
    def data_received(self, previous_state): ...
    def window_updated(self, previous_state): ...
    def stream_half_closed(self, previous_state): ...
    def stream_ended(self, previous_state): ...
    def stream_reset(self, previous_state): ...
    def send_new_pushed_stream(self, previous_state): ...
    def recv_new_pushed_stream(self, previous_state): ...
    def send_push_promise(self, previous_state): ...
    def recv_push_promise(self, previous_state): ...
    def send_end_stream(self, previous_state) -> None: ...
    def send_reset_stream(self, previous_state) -> None: ...
    def reset_stream_on_error(self, previous_state) -> None: ...
    def recv_on_closed_stream(self, previous_state) -> None: ...
    def send_on_closed_stream(self, previous_state) -> None: ...
    def recv_push_on_closed_stream(self, previous_state) -> None: ...
    def send_push_on_closed_stream(self, previous_state) -> None: ...
    def send_informational_response(self, previous_state): ...
    def recv_informational_response(self, previous_state): ...
    def recv_alt_svc(self, previous_state): ...
    def send_alt_svc(self, previous_state) -> None: ...

class H2Stream:
    state_machine: Incomplete
    stream_id: Incomplete
    max_outbound_frame_size: Incomplete
    request_method: Incomplete
    outbound_flow_control_window: Incomplete
    config: Incomplete
    def __init__(self, stream_id, config, inbound_window_size, outbound_window_size) -> None: ...
    @property
    def inbound_flow_control_window(self): ...
    @property
    def open(self): ...
    @property
    def closed(self): ...
    @property
    def closed_by(self): ...
    def upgrade(self, client_side) -> None: ...
    def send_headers(self, headers, encoder, end_stream: bool = ...): ...
    def push_stream_in_band(self, related_stream_id, headers, encoder): ...
    def locally_pushed(self): ...
    def send_data(self, data, end_stream: bool = ..., pad_length: Incomplete | None = ...): ...
    def end_stream(self): ...
    def advertise_alternative_service(self, field_value): ...
    def increase_flow_control_window(self, increment): ...
    def receive_push_promise_in_band(self, promised_stream_id, headers, header_encoding): ...
    def remotely_pushed(self, pushed_headers): ...
    def receive_headers(self, headers, end_stream, header_encoding): ...
    def receive_data(self, data, end_stream, flow_control_len): ...
    def receive_window_update(self, increment): ...
    def receive_continuation(self) -> None: ...
    def receive_alt_svc(self, frame): ...
    def reset_stream(self, error_code: int = ...): ...
    def stream_reset(self, frame): ...
    def acknowledge_received_data(self, acknowledged_size): ...
