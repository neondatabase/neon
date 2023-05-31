#include "bindgen_deps.h"
#include "rust_bindings.h"
#include <stdio.h>
#include "postgres.h"

// From src/backend/main/main.c
const char *progname = "fakepostgres";

int hohoho(int a, int b);

int TestFunc(int a, int b) {
    printf("TestFunc: %d + %d = %d\n", a, b, a + b);
    // elog(LOG, "postgres elog test");
    rust_function(0);
    return hohoho(a, b);
}
