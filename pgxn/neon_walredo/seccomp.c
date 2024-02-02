/*-------------------------------------------------------------------------
 *
 * seccomp.c
 *	  Secure Computing BPF API wrapper.
 *
 * Pageserver delegates complex WAL decoding duties to postgres,
 * which means that the latter might fall victim to carefully designed
 * malicious WAL records and start doing harmful things to the system.
 * To prevent this, it has been decided to limit possible interactions
 * with the outside world using the Secure Computing BPF mode.
 *
 * This code is intended to support both x86_64 and aarch64. The latter
 * doesn't implement some syscalls like open and select. We allow both
 * select (absent on aarch64) and pselect6 (present on both architectures)
 * We call select(2) through libc, and the libc wrapper calls select or pselect6
 * depending on the architecture. You can check which syscalls are present on
 * different architectures with the `scmp_sys_resolver` tool from the
 * seccomp package.
 *
 * We use this mode to disable all syscalls not in the allowlist. This
 * approach has its pros & cons:
 *
 *  - We have to carefully handpick and maintain the set of syscalls
 *    required for the WAL redo process. Core dumps help with that.
 *    The method of trial and error seems to work reasonably well,
 *    but it would be nice to find a proper way to "prove" that
 *    the set in question is both necessary and sufficient.
 *
 *  - Once we enter the seccomp bpf mode, it's impossible to lift those
 *    restrictions (otherwise, what kind of "protection" would that be?).
 *    Thus, we have to either enable extra syscalls for the clean shutdown,
 *    or exit the process immediately via _exit() instead of proc_exit().
 *
 *  - Should we simply use SCMP_ACT_KILL_PROCESS, or implement a custom
 *    facility to deal with the forbidden syscalls? If we'd like to embed
 *    a startup security test, we should go with the latter; In that
 *    case, which one of the following options is preferable?
 *
 *      * Catch the denied syscalls with a signal handler using SCMP_ACT_TRAP.
 *        Provide a common signal handler with a static switch to override
 *        its behavior for the test case. This would undermine the whole
 *        purpose of such protection, so we'd have to go further and remap
 *        the memory backing the switch as readonly, then ban mprotect().
 *        Ugly and fragile, to say the least.
 *
 *      * Yet again, catch the denied syscalls using SCMP_ACT_TRAP.
 *        Provide 2 different signal handlers: one for a test case,
 *        another for the main processing loop. Install the first one,
 *        enable seccomp, perform the test, switch to the second one,
 *        finally ban sigaction(), presto!
 *
 *      * Spoof the result of a syscall using SECCOMP_RET_ERRNO for the
 *        test, then ban it altogether with another filter. The downside
 *        of this solution is that we don't actually check that
 *        SCMP_ACT_KILL_PROCESS/SCMP_ACT_TRAP works.
 *
 *    Either approach seems to require two eBPF filter programs,
 *    which is unfortunate: the man page tells this is uncommon.
 *    Maybe I (@funbringer) am missing something, though; I encourage
 *    any reader to get familiar with it and scrutinize my conclusions.
 *
 * TODOs and ideas in no particular order:
 *
 *  - Do something about mmap() in musl's malloc().
 *    Definitely not a priority if we don't care about musl.
 *
 *  - See if we can untangle PG's shutdown sequence (involving unlink()):
 *
 *      * Simplify (or rather get rid of) shmem setup in PG's WAL redo mode.
 *      * Investigate chroot() or mount namespaces for better FS isolation.
 *      * (Per Heikki) Simply call _exit(), no big deal.
 *      * Come up with a better idea?
 *
 *  - Make use of seccomp's argument inspection (for what?).
 *    Unfortunately, it views all syscall arguments as scalars,
 *    so it won't work for e.g. string comparison in unlink().
 *
 *  - Benchmark with bpf jit on/off, try seccomp_syscall_priority().
 *
 *  - Test against various linux distros & glibc versions.
 *    I suspect that certain libc functions might involve slightly
 *    different syscalls, e.g. select/pselect6/pselect6_time64/whatever.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

/*
 * I couldn't find a good way to do a conditional OBJS += seccomp.o in
 * the Makefile, so this file is compiled even when seccomp is disabled,
 * it's just empty in that case.
 */
#ifdef HAVE_LIBSECCOMP

#include <fcntl.h>
#include <unistd.h>

#include "miscadmin.h"

#include "neon_seccomp.h"

static void die(int code, const char *str);

static bool seccomp_test_sighandler_done = false;
static void seccomp_test_sighandler(int signum, siginfo_t *info, void *cxt);
static void seccomp_deny_sighandler(int signum, siginfo_t *info, void *cxt);

static int do_seccomp_load_rules(PgSeccompRule *rules, int count, uint32 def_action);

void
seccomp_load_rules(PgSeccompRule *rules, int count)
{
	struct sigaction action = { .sa_flags = SA_SIGINFO };
	PgSeccompRule rule;
	long fd;

	/*
	 * Install a test signal handler.
	 * XXX: pqsignal() is too restrictive for our purposes,
	 * since we'd like to examine the contents of siginfo_t.
	 */
	action.sa_sigaction = seccomp_test_sighandler;
	if (sigaction(SIGSYS, &action, NULL) != 0)
		ereport(FATAL,
				(errcode(ERRCODE_SYSTEM_ERROR),
				 errmsg("seccomp: could not install test SIGSYS handler")));

	/*
	 * First, check that open of a well-known file works.
	 * XXX: We use raw syscall() to call the very openat() which is
	 * present both on x86_64 and on aarch64.
	 */
	fd = syscall(SCMP_SYS(openat), AT_FDCWD, "/dev/null", O_RDONLY, 0);
	if (seccomp_test_sighandler_done)
		ereport(FATAL,
				(errcode(ERRCODE_SYSTEM_ERROR),
				 errmsg("seccomp: signal handler test flag was set unexpectedly")));
	if (fd < 0)
		ereport(FATAL,
				(errcode(ERRCODE_SYSTEM_ERROR),
				 errmsg("seccomp: could not open /dev/null for seccomp testing: %m")));
	close((int) fd);

	/* Set a trap on openat() to test seccomp bpf */
	rule = PG_SCMP(openat, SCMP_ACT_TRAP);
	if (do_seccomp_load_rules(&rule, 1, SCMP_ACT_ALLOW) != 0)
		ereport(FATAL,
				(errcode(ERRCODE_SYSTEM_ERROR),
				 errmsg("seccomp: could not load test trap")));

	/* Finally, check that openat() now raises SIGSYS */
	(void) syscall(SCMP_SYS(openat), AT_FDCWD, "/dev/null", O_RDONLY, 0);
	if (!seccomp_test_sighandler_done)
		ereport(FATAL,
				(errcode(ERRCODE_SYSTEM_ERROR),
				 errmsg("seccomp: SIGSYS handler doesn't seem to work")));

	/* Now that everything seems to work, install a proper handler */
	action.sa_sigaction = seccomp_deny_sighandler;
	if (sigaction(SIGSYS, &action, NULL) != 0)
		ereport(FATAL,
				(errcode(ERRCODE_SYSTEM_ERROR),
				 errmsg("seccomp: could not install SIGSYS handler")));

	/* If this succeeds, any syscall not in the list will crash the process */
	if (do_seccomp_load_rules(rules, count, SCMP_ACT_TRAP) != 0)
		ereport(FATAL,
				(errcode(ERRCODE_SYSTEM_ERROR),
				 errmsg("seccomp: could not enter seccomp mode")));
}

/*
 * Enter seccomp mode with a BPF filter that will only allow
 * certain syscalls to proceed.
 */
static int
do_seccomp_load_rules(PgSeccompRule *rules, int count, uint32 def_action)
{
	scmp_filter_ctx ctx;
	int rc = -1;

	/* Create a context with a default action for syscalls not in the list */
	if ((ctx = seccomp_init(def_action)) == NULL)
		goto cleanup;

	for (int i = 0; i < count; i++)
	{
		PgSeccompRule *rule = &rules[i];
		if ((rc = seccomp_rule_add(ctx, rule->psr_action, rule->psr_syscall, 0)) != 0)
			goto cleanup;
	}

	/* Try building & loading the program into the kernel */
	if ((rc = seccomp_load(ctx)) != 0)
		goto cleanup;

cleanup:
	/*
	 * We don't need the context anymore regardless of the result,
	 * since either we failed or the eBPF program has already been
	 * loaded into the linux kernel.
	 */
	seccomp_release(ctx);
	return rc;
}

static void
die(int code, const char *str)
{
	/* work around gcc ignoring that it shouldn't warn on (void) result being unused */
	ssize_t _unused pg_attribute_unused();
	/* Best effort write to stderr */
	_unused = write(fileno(stderr), str, strlen(str));

	/* XXX: we don't want to run any atexit callbacks */
	_exit(code);
}

static void
seccomp_test_sighandler(int signum, siginfo_t *info, void *cxt pg_attribute_unused())
{
#define DIE_PREFIX "seccomp test signal handler: "

	/* Check that this signal handler is used only for a single test case */
	if (seccomp_test_sighandler_done)
		die(1, DIE_PREFIX "test handler should only be used for 1 test\n");
	seccomp_test_sighandler_done = true;

	if (signum != SIGSYS)
		die(1, DIE_PREFIX "bad signal number\n");

	/* TODO: maybe somehow extract the hardcoded syscall number */
	if (info->si_syscall != SCMP_SYS(openat))
		die(1, DIE_PREFIX "bad syscall number\n");

#undef DIE_PREFIX
}

static void
seccomp_deny_sighandler(int signum, siginfo_t *info, void *cxt pg_attribute_unused())
{
	/*
	 * Unfortunately, we can't use seccomp_syscall_resolve_num_arch()
	 * to resolve the syscall's name, since it calls strdup()
	 * under the hood (wtf!).
	 */
	char buffer[128];
	(void)snprintf(buffer, lengthof(buffer),
			"---------------------------------------\n"
			"seccomp: bad syscall %d\n"
			"---------------------------------------\n",
			info->si_syscall);

	/*
	 * Instead of silently crashing the process with
	 * a fake SIGSYS caused by SCMP_ACT_KILL_PROCESS,
	 * we'd like to receive a real SIGSYS to print the
	 * message and *then* immediately exit.
	 */
	die(1, buffer);
}

#endif		/* HAVE_LIBSECCOMP */
