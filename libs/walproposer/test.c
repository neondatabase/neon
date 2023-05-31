#include "bindgen_deps.h"
#include "rust_bindings.h"
#include <stdio.h>
#include "postgres.h"
#include "utils/memutils.h"

// From src/backend/main/main.c
const char *progname = "fakepostgres";

int TestFunc(int a, int b) {
    MemoryContextInit();

    printf("TestFunc: %d + %d = %d\n", a, b, a + b);
    rust_function(0);
    elog(LOG, "postgres elog test");
    printf("After rust_function\n");
    return a + b;
}

// This is a quick experiment with rewriting existing Rust code in C.
void RunClientC(uint32_t serverId) {
    MemoryContextInit();

    uint32_t clientId = sim_id();

    elog(LOG, "started client");

    int data_len = 5;

    int delivered = 0;
    int tcp = sim_open_tcp(serverId);
    while (delivered < data_len) {
        ReplCell cell = {
            .value = delivered+1,
            .client_id = clientId,
            .seqno = delivered,
        };
        sim_tcp_send(tcp, cell);

        Event event = sim_epoll_rcv();
        if (event.tag == 2) {
            // closed
            elog(LOG, "connection closed");
            tcp = sim_open_tcp(serverId);
        } else if (event.tag == 3) {
            // got message
            if (event.value == delivered + 1) {
                delivered += 1;
            }
        }
    }
}
