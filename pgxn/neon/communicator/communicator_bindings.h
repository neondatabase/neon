#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#define BLCKSZ 8192

#define MAX_GETPAGEV_PAGES 32

typedef struct CommunicatorBackendStruct CommunicatorBackendStruct;

/**
 * This struct is created in the postmaster process, and inherited to
 * the communicator process and all backend processes through fork()
 */
typedef struct CommunicatorInitStruct CommunicatorInitStruct;

typedef struct CommunicatorWorkerProcessStruct CommunicatorWorkerProcessStruct;

typedef struct LoggingState LoggingState;

typedef uint32_t COid;

typedef struct CRelExistsRequest {
  COid spc_oid;
  COid db_oid;
  uint32_t rel_number;
  uint8_t fork_number;
} CRelExistsRequest;

typedef struct CRelSizeRequest {
  COid spc_oid;
  COid db_oid;
  uint32_t rel_number;
  uint8_t fork_number;
} CRelSizeRequest;

/**
 * ShmemBuf represents a buffer in shared memory.
 *
 * SAFETY: The pointer must point to an area in shared memory. The functions allow you to liberally
 * get a mutable pointer to the contents; it is the caller's responsibility to ensure that you
 * don't access a buffer that's you're not allowed to. Inappropriate access to the buffer doesn't
 * violate Rust's safety semantics, but it will mess up and crash Postgres.
 *
 */
typedef struct ShmemBuf {
  uint8_t *ptr;
} ShmemBuf;

typedef struct CGetPageVRequest {
  COid spc_oid;
  COid db_oid;
  uint32_t rel_number;
  uint8_t fork_number;
  uint32_t block_number;
  uint8_t nblocks;
  struct ShmemBuf dest[MAX_GETPAGEV_PAGES];
} CGetPageVRequest;

typedef struct CPrefetchVRequest {
  COid spc_oid;
  COid db_oid;
  uint32_t rel_number;
  uint8_t fork_number;
  uint32_t block_number;
  uint8_t nblocks;
} CPrefetchVRequest;

typedef uint64_t CLsn;

typedef struct CDbSizeRequest {
  COid db_oid;
  CLsn request_lsn;
} CDbSizeRequest;

typedef struct CWritePageRequest {
  COid spc_oid;
  COid db_oid;
  uint32_t rel_number;
  uint8_t fork_number;
  uint32_t block_number;
  CLsn lsn;
  struct ShmemBuf src;
} CWritePageRequest;

typedef struct CRelExtendRequest {
  COid spc_oid;
  COid db_oid;
  uint32_t rel_number;
  uint8_t fork_number;
  uint32_t block_number;
  CLsn lsn;
  uintptr_t src_ptr;
  uint32_t src_size;
} CRelExtendRequest;

typedef struct CRelZeroExtendRequest {
  COid spc_oid;
  COid db_oid;
  uint32_t rel_number;
  uint8_t fork_number;
  uint32_t block_number;
  uint32_t nblocks;
  CLsn lsn;
} CRelZeroExtendRequest;

typedef struct CRelCreateRequest {
  COid spc_oid;
  COid db_oid;
  uint32_t rel_number;
  uint8_t fork_number;
} CRelCreateRequest;

typedef struct CRelTruncateRequest {
  COid spc_oid;
  COid db_oid;
  uint32_t rel_number;
  uint8_t fork_number;
  uint32_t nblocks;
} CRelTruncateRequest;

typedef struct CRelUnlinkRequest {
  COid spc_oid;
  COid db_oid;
  uint32_t rel_number;
  uint8_t fork_number;
  uint32_t block_number;
  uint32_t nblocks;
} CRelUnlinkRequest;

typedef enum NeonIORequest_Tag {
  NeonIORequest_Empty,
  NeonIORequest_RelExists,
  NeonIORequest_RelSize,
  NeonIORequest_GetPageV,
  NeonIORequest_PrefetchV,
  NeonIORequest_DbSize,
  NeonIORequest_WritePage,
  NeonIORequest_RelExtend,
  NeonIORequest_RelZeroExtend,
  NeonIORequest_RelCreate,
  NeonIORequest_RelTruncate,
  NeonIORequest_RelUnlink,
} NeonIORequest_Tag;

typedef struct NeonIORequest {
  NeonIORequest_Tag tag;
  union {
    struct {
      struct CRelExistsRequest rel_exists;
    };
    struct {
      struct CRelSizeRequest rel_size;
    };
    struct {
      struct CGetPageVRequest get_page_v;
    };
    struct {
      struct CPrefetchVRequest prefetch_v;
    };
    struct {
      struct CDbSizeRequest db_size;
    };
    struct {
      struct CWritePageRequest write_page;
    };
    struct {
      struct CRelExtendRequest rel_extend;
    };
    struct {
      struct CRelZeroExtendRequest rel_zero_extend;
    };
    struct {
      struct CRelCreateRequest rel_create;
    };
    struct {
      struct CRelTruncateRequest rel_truncate;
    };
    struct {
      struct CRelUnlinkRequest rel_unlink;
    };
  };
} NeonIORequest;

typedef enum NeonIOResult_Tag {
  NeonIOResult_Empty,
  NeonIOResult_RelExists,
  NeonIOResult_RelSize,
  /**
   * the result pages are written to the shared memory addresses given in the request
   */
  NeonIOResult_GetPageV,
  /**
   * A prefetch request returns as soon as the request has been received by the communicator.
   * It is processed in the background.
   */
  NeonIOResult_PrefetchVLaunched,
  NeonIOResult_DbSize,
  NeonIOResult_Error,
  NeonIOResult_Aborted,
  /**
   * used for all write requests
   */
  NeonIOResult_WriteOK,
} NeonIOResult_Tag;

typedef struct NeonIOResult {
  NeonIOResult_Tag tag;
  union {
    struct {
      bool rel_exists;
    };
    struct {
      uint32_t rel_size;
    };
    struct {
      uint64_t db_size;
    };
    struct {
      int32_t error;
    };
  };
} NeonIOResult;

typedef struct CCachedGetPageVResult {
  uint64_t cache_block_numbers[MAX_GETPAGEV_PAGES];
} CCachedGetPageVResult;

typedef uint64_t CacheBlock;

#define INVALID_CACHE_BLOCK UINT64_MAX

struct CommunicatorBackendStruct *rcommunicator_backend_init(struct CommunicatorInitStruct *cis,
                                                             int32_t my_proc_number);

/**
 * Start a request. You can poll for its completion and get the result by
 * calling bcomm_poll_dbsize_request_completion(). The communicator will wake
 * us up by setting our process latch, so to wait for the completion, wait on
 * the latch and call bcomm_poll_dbsize_request_completion() every time the
 * latch is set.
 *
 * Safety: The C caller must ensure that the references are valid.
 */
int32_t bcomm_start_io_request(struct CommunicatorBackendStruct *bs,
                               const struct NeonIORequest *request,
                               struct NeonIOResult *immediate_result_ptr);

int32_t bcomm_start_get_page_v_request(struct CommunicatorBackendStruct *bs,
                                       const struct NeonIORequest *request,
                                       struct CCachedGetPageVResult *immediate_result_ptr);

/**
 * Check if a request has completed. Returns:
 *
 * -1 if the request is still being processed
 * 0 on success
 */
int32_t bcomm_poll_request_completion(struct CommunicatorBackendStruct *bs,
                                      uint32_t request_idx,
                                      struct NeonIOResult *result_p);

/**
 * Finish a local file cache read
 *
 */
bool bcomm_finish_cache_read(struct CommunicatorBackendStruct *bs);

uint64_t rcommunicator_shmem_size(uint32_t max_procs);

/**
 * Initialize the shared memory segment. Returns a backend-private
 * struct, which will be inherited by backend processes through fork
 */
struct CommunicatorInitStruct *rcommunicator_shmem_init(int submission_pipe_read_fd,
                                                        int submission_pipe_write_fd,
                                                        uint32_t max_procs,
                                                        uint8_t *shmem_area_ptr,
                                                        uint64_t shmem_area_len,
                                                        uint64_t initial_file_cache_size,
                                                        uint64_t max_file_cache_size);

extern void notify_proc_unsafe(int procno);

extern void callback_set_my_latch_unsafe(void);

extern uint64_t callback_get_request_lsn_unsafe(void);

/**
 * Called once, at worker process startup. The returned LoggingState is passed back
 * in the subsequent calls to `pump_logging`. It is opaque to the C code.
 */
struct LoggingState *configure_logging(void);

/**
 * Read one message from the logging queue. This is essentially a wrapper to Receiver,
 * with a C-friendly signature.
 *
 * The message is copied into *errbuf, which is a caller-supplied buffer of size `errbuf_len`.
 * If the message doesn't fit in the buffer, it is truncated. It is always NULL-terminated.
 *
 * The error level is returned *elevel_p. It's one of the PostgreSQL error levels, see elog.h
 */
int32_t pump_logging(struct LoggingState *state,
                     uint8_t *errbuf,
                     uint32_t errbuf_len,
                     int32_t *elevel_p);

/**
 * Launch the communicator's tokio tasks, which do most of the work.
 *
 * The caller has initialized the process as a regular PostgreSQL
 * background worker process. The shared memory segment used to
 * communicate with the backends has been allocated and initialized
 * earlier, at postmaster startup, in rcommunicator_shmem_init().
 */
const struct CommunicatorWorkerProcessStruct *communicator_worker_process_launch(struct CommunicatorInitStruct *cis,
                                                                                 const char *tenant_id,
                                                                                 const char *timeline_id,
                                                                                 const char *auth_token,
                                                                                 char **shard_map,
                                                                                 uint32_t nshards,
                                                                                 const char *file_cache_path,
                                                                                 uint64_t initial_file_cache_size);

/**
 * Inform the rust code about a configuration change
 */
void communicator_worker_config_reload(const struct CommunicatorWorkerProcessStruct *proc_handle,
                                       uint64_t file_cache_size);
