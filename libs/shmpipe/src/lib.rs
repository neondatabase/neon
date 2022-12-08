extern "C" {
    pub fn shmem_pipe_process_request(pipe: usize, data: *const u8, req_size: usize, resp: *mut u8);
    pub fn shmem_pipe_init(tenant: *const i8) -> usize;
    pub fn shmem_pipe_close(pipe: usize);
}
