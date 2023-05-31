#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

typedef struct ReplCell {
  uint32_t value;
  uint32_t client_id;
  uint32_t seqno;
} ReplCell;

typedef struct Event {
  int64_t tcp;
  uint32_t value;
  uint32_t tag;
} Event;

void rust_function(uint32_t a);

/**
 * C API for the node os.
 */
void sim_sleep(uint64_t ms);

uint64_t sim_random(uint64_t max);

uint32_t sim_id(void);

int64_t sim_open_tcp(uint32_t dst);

void sim_tcp_send(int64_t tcp, struct ReplCell value);

struct Event sim_epoll_rcv(void);
