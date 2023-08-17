#include "bindgen_deps.h"
#include "rust_bindings.h"
#include <stdio.h>
#include <pthread.h>
#include <stdlib.h>
#include "postgres.h"
#include "utils/memutils.h"
#include "utils/guc.h"
#include "miscadmin.h"
#include "common/pg_prng.h"

// From src/backend/main/main.c
const char *progname = "fakepostgres";

int TestFunc(int a, int b) {
    printf("TestFunc: %d + %d = %d\n", a, b, a + b);
    rust_function(0);
    elog(LOG, "postgres elog test");
    printf("After rust_function\n");
    return a + b;
}

// This is a quick experiment with rewriting existing Rust code in C.
void RunClientC(uint32_t serverId) {
    uint32_t clientId = sim_id();

    elog(LOG, "started client");

    int data_len = 5;

    int delivered = 0;
    int tcp = sim_open_tcp(serverId);
    while (delivered < data_len) {
        sim_msg_set_repl_cell(delivered+1, clientId, delivered);
        sim_tcp_send(tcp);

        Event event = sim_epoll_rcv(-1);
        switch (event.tag)
        {
        case Closed:
            elog(LOG, "connection closed");
            tcp = sim_open_tcp(serverId);
            break;

        case Message:
            Assert(event.any_message == Just32);
            uint32_t val;
            sim_msg_get_just_u32(&val);
            if (val == delivered + 1) {
                delivered += 1;
            }
            break;

        default:
            Assert(false);
        }
    }
}

bool initializedMemoryContext = false;
// pthread_mutex_init(&lock, NULL)?
pthread_mutex_t lock;

void MyContextInit() {
    // initializes global variables, TODO how to make them thread-local?
    pthread_mutex_lock(&lock);
    if (!initializedMemoryContext) {
        initializedMemoryContext = true;
        MemoryContextInit();
        pg_prng_seed(&pg_global_prng_state, 0);

        setenv("PGDATA", "/home/admin/simulator/libs/walproposer/pgdata", 1);

        /*
         * Set default values for command-line options.
         */
        InitializeGUCOptions();

        /* Acquire configuration parameters */
        if (!SelectConfigFiles(NULL, progname))
            exit(1);

        log_min_messages = LOG;

        InitializeMaxBackends();
        ChangeToDataDir();
        CreateSharedMemoryAndSemaphores();
        SetInstallXLogFileSegmentActive();
        // CreateAuxProcessResourceOwner();
        // StartupXLOG();
    }
    pthread_mutex_unlock(&lock);
}
