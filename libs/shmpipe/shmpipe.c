#include <sys/mman.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <malloc.h>
#include <pthread.h>
#include <assert.h>
#include <unistd.h>
#include <sys/fcntl.h>
#include <stdatomic.h>
#include "shmpipe.h"

#define QUEUE_BUF_SIZE (128*1024) /* should be power of two */

/* Make sure that massge header always fits in buffer */
#define MESSAGE_DATA_ALIGNMENT sizeof(message_header_t)

#define RELEASE_CPU()  sched_yield()

typedef struct {
	uint32 id;    /* message id is used to identify responses */
	uint32 size;  /* message size not including header */
} message_header_t;

typedef struct {
	pthread_cond_t cv;  /* signaled when position is changed */
	size_t         pos; /* position in ring buffer */
} queue_pos_t;

typedef atomic_bool latch_t;

typedef struct {
	latch_t cs;    /* queue critical section */
	queue_pos_t head;
	queue_pos_t tail;
	int busy;              /* in process of sending message */
	char data[QUEUE_BUF_SIZE];
} queue_t;

typedef struct pipe_t {
	queue_t req;
	queue_t resp;
	uint32  msg_id; /* generator of message ids, protected by request queue mutex */
} pipe_t;


static void latch_acquire(latch_t* latch)
{
    latch_t unset = 0;
 	while (!atomic_compare_exchange_strong(latch, &unset, 1)) {
		RELEASE_CPU();
		unset = 0;
	}
}

static void latch_release(latch_t* latch)
{
	atomic_store(latch, 0);
}

#define ALIGN(x,y) (((x) + (y) - 1) & ~((y) - 1))

void shmem_pipe_process_request(pipe_t* pipe, char const* req, size_t req_size, char* resp)
{
	message_header_t req_hdr;
	message_header_t resp_hdr;
	int header_sent = 0;
	int header_received = 0;
	size_t resp_size = sizeof(resp_hdr);

	latch_acquire(&pipe->req.cs);

	/* append data to queue head */
	while (req_size != 0)
	{
		/* To distinguish empty buffer from full buffer we need to reserve some space */
		size_t available = (pipe->req.head.pos >= pipe->req.tail.pos
					 ? QUEUE_BUF_SIZE - (pipe->req.head.pos - pipe->req.tail.pos)
					 : pipe->req.tail.pos - pipe->req.head.pos) - MESSAGE_DATA_ALIGNMENT;
		if (available == 0)
		{
			do
			{
				/* Wait until tail is advanced */
				latch_release(&pipe->req.cs);
				RELEASE_CPU();
				latch_acquire(&pipe->req.cs);
			} while (!header_sent && pipe->req.busy);
		}
		else
		{
			size_t tail = QUEUE_BUF_SIZE - pipe->req.head.pos;
			assert(available >= MESSAGE_DATA_ALIGNMENT);
			if (!header_sent)
			{
				assert(!pipe->req.busy);
				req_hdr.id = ++pipe->msg_id; /* protected by req.mutex */
				req_hdr.size = req_size;
				assert(tail >= MESSAGE_DATA_ALIGNMENT);
				memcpy(&pipe->req.data[pipe->req.head.pos], &req_hdr, sizeof req_hdr);
				pipe->req.head.pos = (pipe->req.head.pos + sizeof req_hdr) % QUEUE_BUF_SIZE;
				pipe->req.busy = 1; /* prevent interleving with other messages */
				header_sent = 1;
			}
			else
			{
				if (available > req_size)
					available = req_size;

				if (tail >= available)
				{
					memcpy(&pipe->req.data[pipe->req.head.pos], req, available);
				}
				else
				{
					memcpy(&pipe->req.data[pipe->req.head.pos], req, tail);
					memcpy(&pipe->req.data[0], req + tail, available - tail);
				}
				req += available;
				req_size -= available;
				pipe->req.head.pos = (pipe->req.head.pos + available) % QUEUE_BUF_SIZE;
			}
		}
	}
	/* Align position on header length */
	pipe->req.head.pos = ALIGN(pipe->req.head.pos, MESSAGE_DATA_ALIGNMENT) % QUEUE_BUF_SIZE;

	assert(pipe->req.busy);
	pipe->req.busy = 0;

	latch_release(&pipe->req.cs);

	latch_acquire(&pipe->resp.cs);

	/* Get response from queue tail */
	while (resp_size != 0)
	{
		size_t available = pipe->resp.head.pos >= pipe->resp.tail.pos
			? pipe->resp.head.pos - pipe->resp.tail.pos
			: QUEUE_BUF_SIZE - (pipe->resp.tail.pos - pipe->resp.head.pos);
		if (available == 0)
		{
			do
			{
				/* wait until head is advanced */
				latch_release(&pipe->resp.cs);
				RELEASE_CPU();
				latch_acquire(&pipe->resp.cs);
			} while (!header_received && pipe->resp.busy);
		}
		else
		{
			size_t tail = QUEUE_BUF_SIZE - pipe->resp.tail.pos;
			assert(available >= MESSAGE_DATA_ALIGNMENT);
			if (!header_received)
			{
				assert(tail >= MESSAGE_DATA_ALIGNMENT);
				memcpy(&resp_hdr, &pipe->resp.data[pipe->resp.tail.pos], sizeof resp_hdr);
				if (resp_hdr.id != req_hdr.id)
				{
					/* Not my response */
					latch_release(&pipe->resp.cs);
					RELEASE_CPU();
					latch_acquire(&pipe->resp.cs);
				}
				else
				{
					header_received = 1;
					pipe->resp.busy = 1; /* prevent interleaving with other messages */
					resp_size = resp_hdr.size;
					pipe->resp.tail.pos = (pipe->resp.tail.pos + sizeof resp_hdr) % QUEUE_BUF_SIZE;
				}
			}
			else
			{
				if (available > resp_size)
					available = resp_size;

				if (tail >= available)
				{
					memcpy(resp, &pipe->resp.data[pipe->resp.tail.pos], available);
				}
				else
				{
					memcpy(resp, &pipe->resp.data[pipe->resp.tail.pos], tail);
					memcpy(resp + tail, &pipe->resp.data[0], available - tail);
				}
				resp += available;
				resp_size -= available;
				pipe->resp.tail.pos = (pipe->resp.tail.pos + available) % QUEUE_BUF_SIZE;
			}
		}
	}
	/* Align position on header length */
	pipe->resp.tail.pos = ALIGN(pipe->resp.tail.pos, MESSAGE_DATA_ALIGNMENT) % QUEUE_BUF_SIZE;

	assert(pipe->resp.busy);
	pipe->resp.busy = 0;

	latch_release(&pipe->resp.cs);
}

void shmem_pipe_get_request(pipe_t* pipe, char** data, uint32* size, uint32* msg_id)
{
	message_header_t req_hdr;
	int header_received = 0;
	size_t req_size = sizeof req_hdr;
	char* req  = NULL;

	latch_acquire(&pipe->req.cs);

	/* Take request from queue head */
	while (req_size != 0)
	{
		size_t available = pipe->req.head.pos >= pipe->req.tail.pos
			? pipe->req.head.pos - pipe->req.tail.pos
			: QUEUE_BUF_SIZE - (pipe->req.tail.pos - pipe->req.head.pos);
		if (available == 0)
		{
			/* wait until head is advanced */
			latch_release(&pipe->req.cs);
			RELEASE_CPU();
			latch_acquire(&pipe->req.cs);
		}
		else
		{
			size_t tail = QUEUE_BUF_SIZE - pipe->req.tail.pos;
			assert(available >= MESSAGE_DATA_ALIGNMENT);
			if (!header_received)
			{
				assert(tail >= MESSAGE_DATA_ALIGNMENT);
				memcpy(&req_hdr, &pipe->req.data[pipe->req.tail.pos], sizeof req_hdr);
				header_received = 1;
				req_size = req_hdr.size;
				*msg_id = req_hdr.id;
				*size = req_size;
				req = *data = (char*)malloc(req_size);
				pipe->req.tail.pos = (pipe->req.tail.pos + sizeof req_hdr) % QUEUE_BUF_SIZE;
			}
			else
			{
				if (available > req_size)
					available = req_size;

				if (tail >= available)
				{
					memcpy(req, &pipe->req.data[pipe->req.tail.pos], available);
				}
				else
				{
					memcpy(req, &pipe->req.data[pipe->req.tail.pos], tail);
					memcpy(req + tail, &pipe->req.data[0], available - tail);
				}
				req += available;
				req_size -= available;
				pipe->req.tail.pos = (pipe->req.tail.pos + available) % QUEUE_BUF_SIZE;
			}
		}
	}
	/* Align position on header length */
	pipe->req.tail.pos = ALIGN(pipe->req.tail.pos, MESSAGE_DATA_ALIGNMENT) % QUEUE_BUF_SIZE;

	latch_release(&pipe->req.cs);
}

void shmem_pipe_send_response(pipe_t* pipe, uint32 msg_id, char const* resp, size_t resp_size)
{
	int header_sent = 0;

	latch_acquire(&pipe->resp.cs);

	/* Put response at queue head */
	while (resp_size != 0)
	{
		/* To distinguish empty buffer from full buffer we need to reserve some space */
		size_t available = (pipe->resp.head.pos >= pipe->resp.tail.pos
					 ? QUEUE_BUF_SIZE - (pipe->resp.head.pos - pipe->resp.tail.pos)
					 : pipe->resp.tail.pos - pipe->resp.head.pos) - MESSAGE_DATA_ALIGNMENT;
		if (available == 0)
		{
			/* wait untul tail is advanced */
			latch_release(&pipe->resp.cs);
			RELEASE_CPU();
			latch_acquire(&pipe->resp.cs);
		}
		else
		{
			size_t tail = QUEUE_BUF_SIZE - pipe->resp.head.pos;
			assert(available >= MESSAGE_DATA_ALIGNMENT);
			if (!header_sent)
			{
				message_header_t resp_hdr;
				resp_hdr.id = msg_id;
				resp_hdr.size = resp_size;
				assert(tail >= MESSAGE_DATA_ALIGNMENT);
				memcpy(&pipe->resp.data[pipe->resp.head.pos], &resp_hdr, sizeof resp_hdr);
				pipe->resp.head.pos = (pipe->resp.head.pos + sizeof resp_hdr) % QUEUE_BUF_SIZE;
				header_sent = 1;
			}
			else
			{
				if (available > resp_size)
					available = resp_size;

				if (tail >= available)
				{
					memcpy(&pipe->resp.data[pipe->resp.head.pos], resp, available);
				}
				else
				{
					memcpy(&pipe->resp.data[pipe->resp.head.pos], resp, tail);
					memcpy(&pipe->resp.data[0], resp + tail, available - tail);
				}
				resp += available;
				resp_size -= available;
				pipe->resp.head.pos = (pipe->resp.head.pos + available) % QUEUE_BUF_SIZE;
			}
		}
	}
	/* Align position on header length */
	pipe->resp.head.pos = ALIGN(pipe->resp.head.pos, MESSAGE_DATA_ALIGNMENT) % QUEUE_BUF_SIZE;

	latch_release(&pipe->resp.cs);
}

pipe_t* shmem_pipe_init(char const* name)
{
	char buf[64];
	int fd;
	pipe_t* pipe;

	sprintf(buf, "/walredo-pipe-%s", name);
	if ((fd = shm_open(buf, O_CREAT | O_RDWR | O_TRUNC, 0600)) < 0)
	{
		perror("shm_open");
		return NULL;
	}
	if (ftruncate(fd, ALIGN(sizeof(pipe_t), 4096)) < 0)
	{
		perror("ftruncate");
		close(fd);
		return NULL;
	}
	pipe = (pipe_t*)mmap(NULL, sizeof(pipe_t), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	if (pipe == NULL)
	{
		perror("mmap");
		close(fd);
		return NULL;
	}
	close(fd);

	atomic_init(&pipe->resp.cs, 0);
	atomic_init(&pipe->req.cs, 0);

	pipe->resp.head.pos = 0;
	pipe->resp.tail.pos = 0;
	pipe->req.head.pos = 0;
	pipe->req.tail.pos = 0;

	pipe->req.busy = 0;
	pipe->resp.busy = 0;

	pipe->msg_id = 0;

	return pipe;
}


pipe_t* shmem_pipe_open(char const* name)
{
	char buf[64];
	int fd;
	pipe_t* pipe;
	sprintf(buf, "/walredo-pipe-%s", name);
	if ((fd = shm_open(buf, O_RDWR, 0600)) < 0)
	{
		perror("shm_open");
		return NULL;
	}
	pipe = (pipe_t*)mmap(NULL, sizeof(pipe_t), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	if (pipe == NULL)
	{
		perror("mmap");
	}
	close(fd);
	return pipe;
}

void shmem_pipe_close(pipe_t* pipe)
{
	munmap(pipe, sizeof(pipe_t));
}

#ifdef SHMEM_PIPE_TEST
#define MAX_THREADS 100
#define N_REQUESTS  100000

void* perform_req(void* data)
{
	pipe_t* pipe = (pipe_t*)data;
	int i;
	char buf[8192+1];

	for (i = 0; i < N_REQUESTS; i++)
	{
		shmem_pipe_process_request(pipe, buf, sizeof buf, buf);
	}
	return NULL;
}

int main(int argc, char** argv)
{
	char buf[8192];
	int n_threads = argc > 2 ? atoi(argv[2]) : 1;
	int is_client = *argv[1] == 'c';
	pthread_t thread[MAX_THREADS];
	char const* tenant = "iam";
	int i;

	if (is_client)
	{
		void* status;
		pipe_t* pipe = shmem_pipe_open(tenant);
		for (i = 0; i < n_threads; i++)
		{
			pthread_create(&thread[i], NULL, perform_req, pipe);
		}
		for (i = 0; i < n_threads; i++)
		{
			pthread_join(thread[i], &status);
		}
		shmem_pipe_close(pipe);
	}
	else
	{
		char* data;
		uint32 msg_id;
		uint32 size;
		pipe_t* pipe = shmem_pipe_init(tenant);

		for (i = 0; i < n_threads * N_REQUESTS; i++)
		{
			shmem_pipe_get_request(pipe, &data, &size, &msg_id);
			shmem_pipe_send_response(pipe, msg_id, data, size);
			free(data);
		}
	}
	return 0;
}
#endif
