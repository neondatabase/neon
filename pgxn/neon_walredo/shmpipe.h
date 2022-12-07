struct pipe_t;

typedef unsigned int u32;

extern void shmem_pipe_process_request(struct pipe_t* pipe, char const* req, size_t req_size, char* resp);
extern void shmem_pipe_get_request(struct pipe_t* pipe, char** data, u32* size, u32* msg_id);
extern void shmem_pipe_send_response(struct pipe_t* pipe, u32 msg_id, char const* resp, size_t resp_size);
extern struct pipe_t* shmem_pipe_init(char const* name);
extern struct pipe_t* shmem_pipe_open(char const* name);
extern void shmem_pipe_close(struct pipe_t* pipe);


