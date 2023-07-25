/*
 * This header file is the input to bindgen. It includes all the
 * PostgreSQL headers that we need to auto-generate Rust structs
 * from. If you need to expose a new struct to Rust code, add the
 * header here, and whitelist the struct in the build.rs file.
 */
#include "c.h"
#include "walproposer.h"

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

// Calc a sum of two numbers. Used to test Rust->C function calls.
int TestFunc(int a, int b);

// Run a client for simple simlib test.
void RunClientC(uint32_t serverId);

void WalProposerRust();

void WalProposerCleanup();

// Initialize global variables before calling any Postgres C code.
void MyContextInit();
