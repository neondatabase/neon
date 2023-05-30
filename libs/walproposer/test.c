#include "bindgen_deps.h"
#include "rust_bindings.h"
#include <stdio.h>

int TestFunc(int a, int b) {
    printf("TestFunc: %d + %d = %d\n", a, b, a + b);
    rust_function(0);
    return a + b;
}
