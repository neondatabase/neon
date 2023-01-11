#include <stdint.h>

void* shmempipe_open_via_env();

/**
 * Reads at least one more byte out of the pipe, compatible with the stdio read.
 */
ssize_t shmempipe_read(void* pipe, void* buffer, uint32_t len);

ssize_t shmempipe_write_all(void* pipe, void* buffer, uint32_t len);

void shmempipe_destroy(void* pipe);
