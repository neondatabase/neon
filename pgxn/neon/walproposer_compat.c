#include <stdio.h>
#include "walproposer.h"

unsigned int pq_getmsgint(StringInfo msg, int b) {}
int64 pq_getmsgint64(StringInfo msg) {}

int	pq_getmsgbyte(StringInfo msg) {}
const char *pq_getmsgbytes(StringInfo msg, int datalen) {}
void pq_copymsgbytes(StringInfo msg, char *buf, int datalen) {}
const char *pq_getmsgstring(StringInfo msg) {}
void pq_getmsgend(StringInfo msg) {}

const char *
timestamptz_to_str(TimestampTz dt) {}

bool TimestampDifferenceExceeds(TimestampTz start_time,
									   TimestampTz stop_time,
									   int msec) {}


static XLogReaderState *
wal_reader_allocate(void)
{
    return NULL;
}

static bool
strong_random(void *buf, size_t len)
{
    return true;
}

static void
init_event_set(int n_safekeepers)
{
}

static TimeLineID
get_timeline_id(void)
{
    return 1;
}

const walproposer_api my_api = {
    .wal_reader_allocate = wal_reader_allocate,
    .strong_random = strong_random,
    .init_event_set = init_event_set,
    .get_timeline_id = get_timeline_id,
};

WalProposerConfig config = {
    .neon_tenant = "9e4c8f36063c6c6e93bc20d65a820f3d",
    .neon_timeline = "9e4c8f36063c6c6e93bc20d65a820f3d",
    .safekeeper_connection_timeout = 10000,
    .safekeeper_reconnect_timeout = 1000,
    .syncSafekeepers = true,
    .wal_segment_size = 16 * 1024 * 1024,
};

void ExceptionalCondition(const char *conditionName,
								 const char *fileName, int lineNumber) {
    exit(1);
}

void test_call(void) {
    config.safekeepers_list = malloc(200);
    strcpy(config.safekeepers_list, "localhost:5000");

    printf("Hello, world!\n");
    WalProposer *wp = WalProposerCreate(&config, my_api);
    // WalProposerBroadcast(0, 0);
}
