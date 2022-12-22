#include <stdint.h>

void* shmempipe_open_via_env();

/**
 * Reads the next frame length and stores it at len.
 *
 * Returns:
 * - 0 if frame length was read correctly and `len` updated
 * - -1 if any of the pointers were invalid
 * - -2 if there is current frame remaining
 */
int shmempipe_read_frame_len(void* pipe, uint32_t* len);

/**
 * Reads at least one more byte out of the pipe, compatible with the stdio read.
 */
ssize_t shmempipe_read(void* pipe, void* buffer, uint32_t len);

/**
 * Reads all of the `len` bytes.
 *
 * This will deadlock if used in place of stdio read. Should only be used with the
 * shmempipe_read_frame_len call.
 */
ssize_t shmempipe_read_exact(void* pipe, void* buffer, uint32_t len);

ssize_t shmempipe_write_all(void* pipe, void* buffer, uint32_t len);
void shmempipe_destroy(void* pipe);
