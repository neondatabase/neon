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
