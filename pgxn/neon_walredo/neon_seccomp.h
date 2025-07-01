#ifndef NEON_SECCOMP_H
#define NEON_SECCOMP_H

#include <seccomp.h>

typedef struct {
    int    psr_syscall; /* syscall number */
    uint32 psr_action;  /* libseccomp action, e.g. SCMP_ACT_ALLOW */
} PgSeccompRule;

#define PG_SCMP(syscall, action)                \
    (PgSeccompRule) {                           \
        .psr_syscall = SCMP_SYS(syscall),       \
        .psr_action = (action),                 \
    }

#define PG_SCMP_ALLOW(syscall) \
    PG_SCMP(syscall, SCMP_ACT_ALLOW)

extern void seccomp_load_rules(PgSeccompRule *syscalls, int count);

#endif /* NEON_SECCOMP_H */
