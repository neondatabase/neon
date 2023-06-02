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
};
typedef uint8_t AnyMessageTag;

/**
 * List of all possible NodeEvent.
 */
enum EventTag {
  Accept,
  Closed,
  Message,
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

struct Event sim_epoll_rcv(void);

/**
 * Read AnyMessage::Just32 message.
 */
void sim_msg_get_just_u32(uint32_t *val);

/**
 * Write AnyMessage::ReplCell message.
 */
void sim_msg_set_repl_cell(uint32_t value, uint32_t client_id, uint32_t seqno);
