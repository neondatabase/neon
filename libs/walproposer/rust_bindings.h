#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

/**
 * List of all possible AnyMessage.
 */
enum AnyMessageTag {
  None,
  InternalConnect,
  Just32,
  ReplCell,
  Bytes,
  LSN,
};
typedef uint8_t AnyMessageTag;

/**
 * List of all possible NodeEvent.
 */
enum EventTag {
  Timeout,
  Accept,
  Closed,
  Message,
  Internal,
};
typedef uint8_t EventTag;

/**
 * Event returned by epoll_recv.
 */
typedef struct Event {
  EventTag tag;
  int64_t tcp;
  AnyMessageTag any_message;
} Event;

void rust_function(uint32_t a);

/**
 * C API for the node os.
 */
void sim_sleep(uint64_t ms);

uint64_t sim_random(uint64_t max);

uint32_t sim_id(void);

int64_t sim_open_tcp(uint32_t dst);

/**
 * Send MESSAGE_BUF content to the given tcp.
 */
void sim_tcp_send(int64_t tcp);

struct Event sim_epoll_rcv(int64_t timeout);

struct Event sim_epoll_peek(int64_t timeout);

int64_t sim_now(void);

void sim_exit(int32_t code, const uint8_t *msg);

void sim_set_result(int32_t code, const uint8_t *msg);

/**
 * Get tag of the current message.
 */
AnyMessageTag sim_msg_tag(void);

/**
 * Read AnyMessage::Just32 message.
 */
void sim_msg_get_just_u32(uint32_t *val);

/**
 * Read AnyMessage::LSN message.
 */
void sim_msg_get_lsn(uint64_t *val);

/**
 * Write AnyMessage::ReplCell message.
 */
void sim_msg_set_repl_cell(uint32_t value, uint32_t client_id, uint32_t seqno);

/**
 * Write AnyMessage::Bytes message.
 */
void sim_msg_set_bytes(const char *bytes, uintptr_t len);

/**
 * Read AnyMessage::Bytes message.
 */
const char *sim_msg_get_bytes(uintptr_t *len);
