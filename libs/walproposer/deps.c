#ifndef DEPS_C
#define DEPS_C

/* src/include/pg_config_ext.h.  Generated from pg_config_ext.h.in by configure.  */
/*
 * src/include/pg_config_ext.h.in.  This is generated manually, not by
 * autoheader, since we want to limit which symbols get defined here.
 */

/* Define to the name of a signed 64-bit integer type. */
#define PG_INT64_TYPE long int

/*-------------------------------------------------------------------------
 *
 * postgres_ext.h
 *
 *	   This file contains declarations of things that are visible everywhere
 *	in PostgreSQL *and* are visible to clients of frontend interface libraries.
 *	For example, the Oid type is part of the API of libpq and other libraries.
 *
 *	   Declarations which are specific to a particular interface should
 *	go in the header file for that interface (such as libpq-fe.h).  This
 *	file is only for fundamental Postgres declarations.
 *
 *	   User-written C functions don't count as "external to Postgres."
 *	Those function much as local modifications to the backend itself, and
 *	use header files that are otherwise internal to Postgres to interface
 *	with the backend.
 *
 * src/include/postgres_ext.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef POSTGRES_EXT_H
#define POSTGRES_EXT_H

/*
 * Object ID is a fundamental type in Postgres.
 */
typedef unsigned int Oid;

#ifdef __cplusplus
#define InvalidOid		(Oid(0))
#else
#define InvalidOid		((Oid) 0)
#endif

#define OID_MAX  UINT_MAX
/* you will need to include <limits.h> to use the above #define */

#define atooid(x) ((Oid) strtoul((x), NULL, 10))
/* the above needs <stdlib.h> */


/* Define a signed 64-bit integer type for use in client API declarations. */
typedef PG_INT64_TYPE pg_int64;


/*
 * Identifiers of error message fields.  Kept here to keep common
 * between frontend and backend, and also to export them to libpq
 * applications.
 */
#define PG_DIAG_SEVERITY		'S'
#define PG_DIAG_SEVERITY_NONLOCALIZED 'V'
#define PG_DIAG_SQLSTATE		'C'
#define PG_DIAG_MESSAGE_PRIMARY 'M'
#define PG_DIAG_MESSAGE_DETAIL	'D'
#define PG_DIAG_MESSAGE_HINT	'H'
#define PG_DIAG_STATEMENT_POSITION 'P'
#define PG_DIAG_INTERNAL_POSITION 'p'
#define PG_DIAG_INTERNAL_QUERY	'q'
#define PG_DIAG_CONTEXT			'W'
#define PG_DIAG_SCHEMA_NAME		's'
#define PG_DIAG_TABLE_NAME		't'
#define PG_DIAG_COLUMN_NAME		'c'
#define PG_DIAG_DATATYPE_NAME	'd'
#define PG_DIAG_CONSTRAINT_NAME 'n'
#define PG_DIAG_SOURCE_FILE		'F'
#define PG_DIAG_SOURCE_LINE		'L'
#define PG_DIAG_SOURCE_FUNCTION 'R'

#endif							/* POSTGRES_EXT_H */


/*-------------------------------------------------------------------------
 *
 * c.h
 *	  Fundamental C definitions.  This is included by every .c file in
 *	  PostgreSQL (via either postgres.h or postgres_fe.h, as appropriate).
 *
 *	  Note that the definitions here are not intended to be exposed to clients
 *	  of the frontend interface libraries --- so we don't worry much about
 *	  polluting the namespace with lots of stuff...
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/c.h
 *
 *-------------------------------------------------------------------------
 */
/*
 *----------------------------------------------------------------
 *	 TABLE OF CONTENTS
 *
 *		When adding stuff to this file, please try to put stuff
 *		into the relevant section, or add new sections as appropriate.
 *
 *	  section	description
 *	  -------	------------------------------------------------
 *		0)		pg_config.h and standard system headers
 *		1)		compiler characteristics
 *		2)		bool, true, false
 *		3)		standard system types
 *		4)		IsValid macros for system types
 *		5)		offsetof, lengthof, alignment
 *		6)		assertions
 *		7)		widely useful macros
 *		8)		random stuff
 *		9)		system-specific hacks
 *
 * NOTE: since this file is included by both frontend and backend modules,
 * it's usually wrong to put an "extern" declaration here, unless it's
 * ifdef'd so that it's seen in only one case or the other.
 * typedefs and macros are the kind of thing that might go here.
 *
 *----------------------------------------------------------------
 */
#ifndef C_H
#define C_H

/* Must undef pg_config_ext.h symbols before including pg_config.h */
#undef PG_INT64_TYPE

/* src/include/pg_config.h.  Generated from pg_config.h.in by configure.  */
/* src/include/pg_config.h.in.  Generated from configure.ac by autoheader.  */

/* Define if building universal (internal helper macro) */
/* #undef AC_APPLE_UNIVERSAL_BUILD */

/* The normal alignment of `double', in bytes. */
#define ALIGNOF_DOUBLE 8

/* The normal alignment of `int', in bytes. */
#define ALIGNOF_INT 4

/* The normal alignment of `long', in bytes. */
#define ALIGNOF_LONG 8

/* The normal alignment of `long long int', in bytes. */
/* #undef ALIGNOF_LONG_LONG_INT */

/* The normal alignment of `PG_INT128_TYPE', in bytes. */
#define ALIGNOF_PG_INT128_TYPE 16

/* The normal alignment of `short', in bytes. */
#define ALIGNOF_SHORT 2

/* Size of a disk block --- this also limits the size of a tuple. You can set
   it bigger if you need bigger tuples (although TOAST should reduce the need
   to have large tuples, since fields can be spread across multiple tuples).
   BLCKSZ must be a power of 2. The maximum possible value of BLCKSZ is
   currently 2^15 (32768). This is determined by the 15-bit widths of the
   lp_off and lp_len fields in ItemIdData (see include/storage/itemid.h).
   Changing BLCKSZ requires an initdb. */
#define BLCKSZ 8192

/* Saved arguments from configure */
#define CONFIGURE_ARGS " 'CFLAGS=-O0 -g3 ' '--enable-debug' '--with-openssl' '--enable-cassert' '--enable-depend' '--with-includes=/usr/local/opt/openssl@3/include' '--with-libraries=/usr/local/opt/openssl@3/lib' 'INSTALL=/Users/arthur/zen/zenith//scripts/ninstall.sh -C' '--prefix=/Users/arthur/zen/zenith/pg_install/v15'"

/* Define to the default TCP port number on which the server listens and to
   which clients will try to connect. This can be overridden at run-time, but
   it's convenient if your clients have the right default compiled in.
   (--with-pgport=PORTNUM) */
#define DEF_PGPORT 5432

/* Define to the default TCP port number as a string constant. */
#define DEF_PGPORT_STR "5432"

/* Define to the file name extension of dynamically-loadable modules. */
#define DLSUFFIX ".so"

/* Define to build with GSSAPI support. (--with-gssapi) */
/* #undef ENABLE_GSS */

/* Define to 1 if you want National Language Support. (--enable-nls) */
/* #undef ENABLE_NLS */

/* Define to 1 to build client libraries as thread-safe code.
   (--enable-thread-safety) */
#define ENABLE_THREAD_SAFETY 1

/* Define to 1 if gettimeofday() takes only 1 argument. */
/* #undef GETTIMEOFDAY_1ARG */

#ifdef GETTIMEOFDAY_1ARG
# define gettimeofday(a,b) gettimeofday(a)
#endif

/* Define to 1 if you have the `append_history' function. */
/* #undef HAVE_APPEND_HISTORY */

/* Define to 1 if you have the `ASN1_STRING_get0_data' function. */
#define HAVE_ASN1_STRING_GET0_DATA 1

/* Define to 1 if you want to use atomics if available. */
#define HAVE_ATOMICS 1

/* Define to 1 if you have the <atomic.h> header file. */
/* #undef HAVE_ATOMIC_H */

/* Define to 1 if you have the `backtrace_symbols' function. */
#define HAVE_BACKTRACE_SYMBOLS 1

/* Define to 1 if you have the `BIO_get_data' function. */
#define HAVE_BIO_GET_DATA 1

/* Define to 1 if you have the `BIO_meth_new' function. */
#define HAVE_BIO_METH_NEW 1

/* Define to 1 if you have the `clock_gettime' function. */
#define HAVE_CLOCK_GETTIME 1

/* Define to 1 if your compiler handles computed gotos. */
#define HAVE_COMPUTED_GOTO 1

/* Define to 1 if you have the `copyfile' function. */
#define HAVE_COPYFILE 1

/* Define to 1 if you have the <copyfile.h> header file. */
#define HAVE_COPYFILE_H 1

/* Define to 1 if you have the <crtdefs.h> header file. */
/* #undef HAVE_CRTDEFS_H */

/* Define to 1 if you have the `CRYPTO_lock' function. */
/* #undef HAVE_CRYPTO_LOCK */

/* Define to 1 if you have the declaration of `fdatasync', and to 0 if you
   don't. */
#define HAVE_DECL_FDATASYNC 0

/* Define to 1 if you have the declaration of `F_FULLFSYNC', and to 0 if you
   don't. */
#define HAVE_DECL_F_FULLFSYNC 1

/* Define to 1 if you have the declaration of
   `LLVMCreateGDBRegistrationListener', and to 0 if you don't. */
/* #undef HAVE_DECL_LLVMCREATEGDBREGISTRATIONLISTENER */

/* Define to 1 if you have the declaration of
   `LLVMCreatePerfJITEventListener', and to 0 if you don't. */
/* #undef HAVE_DECL_LLVMCREATEPERFJITEVENTLISTENER */

/* Define to 1 if you have the declaration of `LLVMGetHostCPUFeatures', and to
   0 if you don't. */
/* #undef HAVE_DECL_LLVMGETHOSTCPUFEATURES */

/* Define to 1 if you have the declaration of `LLVMGetHostCPUName', and to 0
   if you don't. */
/* #undef HAVE_DECL_LLVMGETHOSTCPUNAME */

/* Define to 1 if you have the declaration of `LLVMOrcGetSymbolAddressIn', and
   to 0 if you don't. */
/* #undef HAVE_DECL_LLVMORCGETSYMBOLADDRESSIN */

/* Define to 1 if you have the declaration of `posix_fadvise', and to 0 if you
   don't. */
#define HAVE_DECL_POSIX_FADVISE 0

/* Define to 1 if you have the declaration of `preadv', and to 0 if you don't.
   */
#define HAVE_DECL_PREADV 1

/* Define to 1 if you have the declaration of `pwritev', and to 0 if you
   don't. */
#define HAVE_DECL_PWRITEV 1

/* Define to 1 if you have the declaration of `RTLD_GLOBAL', and to 0 if you
   don't. */
#define HAVE_DECL_RTLD_GLOBAL 1

/* Define to 1 if you have the declaration of `RTLD_NOW', and to 0 if you
   don't. */
#define HAVE_DECL_RTLD_NOW 1

/* Define to 1 if you have the declaration of `sigwait', and to 0 if you
   don't. */
#define HAVE_DECL_SIGWAIT 1

/* Define to 1 if you have the declaration of `strlcat', and to 0 if you
   don't. */
#define HAVE_DECL_STRLCAT 1

/* Define to 1 if you have the declaration of `strlcpy', and to 0 if you
   don't. */
#define HAVE_DECL_STRLCPY 1

/* Define to 1 if you have the declaration of `strnlen', and to 0 if you
   don't. */
#define HAVE_DECL_STRNLEN 1

/* Define to 1 if you have the declaration of `strtoll', and to 0 if you
   don't. */
#define HAVE_DECL_STRTOLL 1

/* Define to 1 if you have the declaration of `strtoull', and to 0 if you
   don't. */
#define HAVE_DECL_STRTOULL 1

/* Define to 1 if you have the `dlopen' function. */
#define HAVE_DLOPEN 1

/* Define to 1 if you have the <editline/history.h> header file. */
/* #undef HAVE_EDITLINE_HISTORY_H */

/* Define to 1 if you have the <editline/readline.h> header file. */
/* #undef HAVE_EDITLINE_READLINE_H */

/* Define to 1 if you have the <execinfo.h> header file. */
#define HAVE_EXECINFO_H 1

/* Define to 1 if you have the `explicit_bzero' function. */
/* #undef HAVE_EXPLICIT_BZERO */

/* Define to 1 if you have the `fdatasync' function. */
#define HAVE_FDATASYNC 1

/* Define to 1 if you have the `fls' function. */
#define HAVE_FLS 1

/* Define to 1 if fseeko (and presumably ftello) exists and is declared. */
#define HAVE_FSEEKO 1

/* Define to 1 if your compiler understands __func__. */
#define HAVE_FUNCNAME__FUNC 1

/* Define to 1 if your compiler understands __FUNCTION__. */
/* #undef HAVE_FUNCNAME__FUNCTION */

/* Define to 1 if you have __atomic_compare_exchange_n(int *, int *, int). */
#define HAVE_GCC__ATOMIC_INT32_CAS 1

/* Define to 1 if you have __atomic_compare_exchange_n(int64 *, int64 *,
   int64). */
#define HAVE_GCC__ATOMIC_INT64_CAS 1

/* Define to 1 if you have __sync_lock_test_and_set(char *) and friends. */
#define HAVE_GCC__SYNC_CHAR_TAS 1

/* Define to 1 if you have __sync_val_compare_and_swap(int *, int, int). */
#define HAVE_GCC__SYNC_INT32_CAS 1

/* Define to 1 if you have __sync_lock_test_and_set(int *) and friends. */
#define HAVE_GCC__SYNC_INT32_TAS 1

/* Define to 1 if you have __sync_val_compare_and_swap(int64 *, int64, int64).
   */
#define HAVE_GCC__SYNC_INT64_CAS 1

/* Define to 1 if you have the `getaddrinfo' function. */
#define HAVE_GETADDRINFO 1

/* Define to 1 if you have the `gethostbyname_r' function. */
/* #undef HAVE_GETHOSTBYNAME_R */

/* Define to 1 if you have the `getifaddrs' function. */
#define HAVE_GETIFADDRS 1

/* Define to 1 if you have the `getopt' function. */
#define HAVE_GETOPT 1

/* Define to 1 if you have the <getopt.h> header file. */
#define HAVE_GETOPT_H 1

/* Define to 1 if you have the `getopt_long' function. */
#define HAVE_GETOPT_LONG 1

/* Define to 1 if you have the `getpeereid' function. */
#define HAVE_GETPEEREID 1

/* Define to 1 if you have the `getpeerucred' function. */
/* #undef HAVE_GETPEERUCRED */

/* Define to 1 if you have the `getpwuid_r' function. */
#define HAVE_GETPWUID_R 1

/* Define to 1 if you have the `getrlimit' function. */
#define HAVE_GETRLIMIT 1

/* Define to 1 if you have the `getrusage' function. */
#define HAVE_GETRUSAGE 1

/* Define to 1 if you have the `gettimeofday' function. */
/* #undef HAVE_GETTIMEOFDAY */

/* Define to 1 if you have the <gssapi/gssapi.h> header file. */
/* #undef HAVE_GSSAPI_GSSAPI_H */

/* Define to 1 if you have the <gssapi.h> header file. */
/* #undef HAVE_GSSAPI_H */

/* Define to 1 if you have the <history.h> header file. */
/* #undef HAVE_HISTORY_H */

/* Define to 1 if you have the `history_truncate_file' function. */
#define HAVE_HISTORY_TRUNCATE_FILE 1

/* Define to 1 if you have the `HMAC_CTX_free' function. */
#define HAVE_HMAC_CTX_FREE 1

/* Define to 1 if you have the `HMAC_CTX_new' function. */
#define HAVE_HMAC_CTX_NEW 1

/* Define to 1 if you have the <ifaddrs.h> header file. */
#define HAVE_IFADDRS_H 1

/* Define to 1 if you have the `inet_aton' function. */
#define HAVE_INET_ATON 1

/* Define to 1 if you have the `inet_pton' function. */
#define HAVE_INET_PTON 1

/* Define to 1 if the system has the type `int64'. */
/* #undef HAVE_INT64 */

/* Define to 1 if the system has the type `int8'. */
/* #undef HAVE_INT8 */

/* Define to 1 if you have the <inttypes.h> header file. */
#define HAVE_INTTYPES_H 1

/* Define to 1 if you have the global variable 'int opterr'. */
#define HAVE_INT_OPTERR 1

/* Define to 1 if you have the global variable 'int optreset'. */
#define HAVE_INT_OPTRESET 1

/* Define to 1 if you have the global variable 'int timezone'. */
#define HAVE_INT_TIMEZONE 1

/* Define to 1 if you have support for IPv6. */
#define HAVE_IPV6 1

/* Define to 1 if __builtin_constant_p(x) implies "i"(x) acceptance. */
/* #undef HAVE_I_CONSTRAINT__BUILTIN_CONSTANT_P */

/* Define to 1 if you have the `kqueue' function. */
#define HAVE_KQUEUE 1

/* Define to 1 if you have the <langinfo.h> header file. */
#define HAVE_LANGINFO_H 1

/* Define to 1 if you have the <ldap.h> header file. */
/* #undef HAVE_LDAP_H */

/* Define to 1 if you have the `ldap_initialize' function. */
/* #undef HAVE_LDAP_INITIALIZE */

/* Define to 1 if you have the `crypto' library (-lcrypto). */
#define HAVE_LIBCRYPTO 1

/* Define to 1 if you have the `ldap' library (-lldap). */
/* #undef HAVE_LIBLDAP */

/* Define to 1 if you have the `lz4' library (-llz4). */
/* #undef HAVE_LIBLZ4 */

/* Define to 1 if you have the `m' library (-lm). */
#define HAVE_LIBM 1

/* Define to 1 if you have the `pam' library (-lpam). */
/* #undef HAVE_LIBPAM */

/* Define if you have a function readline library */
#define HAVE_LIBREADLINE 1

/* Define to 1 if you have the `seccomp' library (-lseccomp). */
/* #undef HAVE_LIBSECCOMP */

/* Define to 1 if you have the `selinux' library (-lselinux). */
/* #undef HAVE_LIBSELINUX */

/* Define to 1 if you have the `ssl' library (-lssl). */
#define HAVE_LIBSSL 1

/* Define to 1 if you have the `wldap32' library (-lwldap32). */
/* #undef HAVE_LIBWLDAP32 */

/* Define to 1 if you have the `xml2' library (-lxml2). */
/* #undef HAVE_LIBXML2 */

/* Define to 1 if you have the `xslt' library (-lxslt). */
/* #undef HAVE_LIBXSLT */

/* Define to 1 if you have the `z' library (-lz). */
#define HAVE_LIBZ 1

/* Define to 1 if you have the `zstd' library (-lzstd). */
/* #undef HAVE_LIBZSTD */

/* Define to 1 if you have the `link' function. */
#define HAVE_LINK 1

/* Define to 1 if the system has the type `locale_t'. */
#define HAVE_LOCALE_T 1

/* Define to 1 if `long int' works and is 64 bits. */
#define HAVE_LONG_INT_64 1

/* Define to 1 if `long long int' works and is 64 bits. */
/* #undef HAVE_LONG_LONG_INT_64 */

/* Define to 1 if you have the <mbarrier.h> header file. */
/* #undef HAVE_MBARRIER_H */

/* Define to 1 if you have the `mbstowcs_l' function. */
#define HAVE_MBSTOWCS_L 1

/* Define to 1 if you have the <memory.h> header file. */
#define HAVE_MEMORY_H 1

/* Define to 1 if you have the `memset_s' function. */
#define HAVE_MEMSET_S 1

/* Define to 1 if the system has the type `MINIDUMP_TYPE'. */
/* #undef HAVE_MINIDUMP_TYPE */

/* Define to 1 if you have the `mkdtemp' function. */
#define HAVE_MKDTEMP 1

/* Define to 1 if you have the <netinet/tcp.h> header file. */
#define HAVE_NETINET_TCP_H 1

/* Define to 1 if you have the <net/if.h> header file. */
#define HAVE_NET_IF_H 1

/* Define to 1 if you have the `OPENSSL_init_ssl' function. */
#define HAVE_OPENSSL_INIT_SSL 1

/* Define to 1 if you have the <ossp/uuid.h> header file. */
/* #undef HAVE_OSSP_UUID_H */

/* Define to 1 if you have the <pam/pam_appl.h> header file. */
/* #undef HAVE_PAM_PAM_APPL_H */

/* Define to 1 if you have the `poll' function. */
#define HAVE_POLL 1

/* Define to 1 if you have the <poll.h> header file. */
#define HAVE_POLL_H 1

/* Define to 1 if you have a POSIX-conforming sigwait declaration. */
#define HAVE_POSIX_DECL_SIGWAIT 1

/* Define to 1 if you have the `posix_fadvise' function. */
/* #undef HAVE_POSIX_FADVISE */

/* Define to 1 if you have the `posix_fallocate' function. */
/* #undef HAVE_POSIX_FALLOCATE */

/* Define to 1 if the assembler supports PPC's LWARX mutex hint bit. */
/* #undef HAVE_PPC_LWARX_MUTEX_HINT */

/* Define to 1 if you have the `ppoll' function. */
/* #undef HAVE_PPOLL */

/* Define to 1 if you have the `pread' function. */
#define HAVE_PREAD 1

/* Define to 1 if you have the `pstat' function. */
/* #undef HAVE_PSTAT */

/* Define to 1 if the PS_STRINGS thing exists. */
/* #undef HAVE_PS_STRINGS */

/* Define if you have POSIX threads libraries and header files. */
#define HAVE_PTHREAD 1

/* Define to 1 if you have the `pthread_barrier_wait' function. */
/* #undef HAVE_PTHREAD_BARRIER_WAIT */

/* Define to 1 if you have the `pthread_is_threaded_np' function. */
#define HAVE_PTHREAD_IS_THREADED_NP 1

/* Have PTHREAD_PRIO_INHERIT. */
#define HAVE_PTHREAD_PRIO_INHERIT 1

/* Define to 1 if you have the `pwrite' function. */
#define HAVE_PWRITE 1

/* Define to 1 if you have the <readline.h> header file. */
/* #undef HAVE_READLINE_H */

/* Define to 1 if you have the <readline/history.h> header file. */
#define HAVE_READLINE_HISTORY_H 1

/* Define to 1 if you have the <readline/readline.h> header file. */
#define HAVE_READLINE_READLINE_H 1

/* Define to 1 if you have the `readlink' function. */
#define HAVE_READLINK 1

/* Define to 1 if you have the `readv' function. */
#define HAVE_READV 1

/* Define to 1 if you have the `rl_completion_matches' function. */
#define HAVE_RL_COMPLETION_MATCHES 1

/* Define to 1 if you have the global variable 'rl_completion_suppress_quote'.
   */
/* #undef HAVE_RL_COMPLETION_SUPPRESS_QUOTE */

/* Define to 1 if you have the `rl_filename_completion_function' function. */
#define HAVE_RL_FILENAME_COMPLETION_FUNCTION 1

/* Define to 1 if you have the global variable 'rl_filename_quote_characters'.
   */
/* #undef HAVE_RL_FILENAME_QUOTE_CHARACTERS */

/* Define to 1 if you have the global variable 'rl_filename_quoting_function'.
   */
/* #undef HAVE_RL_FILENAME_QUOTING_FUNCTION */

/* Define to 1 if you have the `rl_reset_screen_size' function. */
/* #undef HAVE_RL_RESET_SCREEN_SIZE */

/* Define to 1 if you have the `rl_variable_bind' function. */
#define HAVE_RL_VARIABLE_BIND 1

/* Define to 1 if you have the <security/pam_appl.h> header file. */
/* #undef HAVE_SECURITY_PAM_APPL_H */

/* Define to 1 if you have the `setenv' function. */
#define HAVE_SETENV 1

/* Define to 1 if you have the `setproctitle' function. */
/* #undef HAVE_SETPROCTITLE */

/* Define to 1 if you have the `setproctitle_fast' function. */
/* #undef HAVE_SETPROCTITLE_FAST */

/* Define to 1 if you have the `setsid' function. */
#define HAVE_SETSID 1

/* Define to 1 if you have the `shm_open' function. */
#define HAVE_SHM_OPEN 1

/* Define to 1 if the system has the type `socklen_t'. */
#define HAVE_SOCKLEN_T 1

/* Define to 1 if you have spinlocks. */
#define HAVE_SPINLOCKS 1

/* Define to 1 if stdbool.h conforms to C99. */
#define HAVE_STDBOOL_H 1

/* Define to 1 if you have the <stdint.h> header file. */
#define HAVE_STDINT_H 1

/* Define to 1 if you have the <stdlib.h> header file. */
#define HAVE_STDLIB_H 1

/* Define to 1 if you have the `strchrnul' function. */
/* #undef HAVE_STRCHRNUL */

/* Define to 1 if you have the `strerror_r' function. */
#define HAVE_STRERROR_R 1

/* Define to 1 if you have the <strings.h> header file. */
#define HAVE_STRINGS_H 1

/* Define to 1 if you have the <string.h> header file. */
#define HAVE_STRING_H 1

/* Define to 1 if you have the `strlcat' function. */
#define HAVE_STRLCAT 1

/* Define to 1 if you have the `strlcpy' function. */
#define HAVE_STRLCPY 1

/* Define to 1 if you have the `strnlen' function. */
#define HAVE_STRNLEN 1

/* Define to 1 if you have the `strsignal' function. */
#define HAVE_STRSIGNAL 1

/* Define to 1 if you have the `strtof' function. */
#define HAVE_STRTOF 1

/* Define to 1 if you have the `strtoll' function. */
#define HAVE_STRTOLL 1

/* Define to 1 if you have the `strtoq' function. */
/* #undef HAVE_STRTOQ */

/* Define to 1 if you have the `strtoull' function. */
#define HAVE_STRTOULL 1

/* Define to 1 if you have the `strtouq' function. */
/* #undef HAVE_STRTOUQ */

/* Define to 1 if the system has the type `struct addrinfo'. */
#define HAVE_STRUCT_ADDRINFO 1

/* Define to 1 if the system has the type `struct cmsgcred'. */
/* #undef HAVE_STRUCT_CMSGCRED */

/* Define to 1 if the system has the type `struct option'. */
#define HAVE_STRUCT_OPTION 1

/* Define to 1 if `sa_len' is a member of `struct sockaddr'. */
#define HAVE_STRUCT_SOCKADDR_SA_LEN 1

/* Define to 1 if the system has the type `struct sockaddr_storage'. */
#define HAVE_STRUCT_SOCKADDR_STORAGE 1

/* Define to 1 if `ss_family' is a member of `struct sockaddr_storage'. */
#define HAVE_STRUCT_SOCKADDR_STORAGE_SS_FAMILY 1

/* Define to 1 if `ss_len' is a member of `struct sockaddr_storage'. */
#define HAVE_STRUCT_SOCKADDR_STORAGE_SS_LEN 1

/* Define to 1 if `__ss_family' is a member of `struct sockaddr_storage'. */
/* #undef HAVE_STRUCT_SOCKADDR_STORAGE___SS_FAMILY */

/* Define to 1 if `__ss_len' is a member of `struct sockaddr_storage'. */
/* #undef HAVE_STRUCT_SOCKADDR_STORAGE___SS_LEN */

/* Define to 1 if the system has the type `struct sockaddr_un'. */
#define HAVE_STRUCT_SOCKADDR_UN 1

/* Define to 1 if `tm_zone' is a member of `struct tm'. */
#define HAVE_STRUCT_TM_TM_ZONE 1

/* Define to 1 if you have the `symlink' function. */
#define HAVE_SYMLINK 1

/* Define to 1 if you have the `syncfs' function. */
/* #undef HAVE_SYNCFS */

/* Define to 1 if you have the `sync_file_range' function. */
/* #undef HAVE_SYNC_FILE_RANGE */

/* Define to 1 if you have the syslog interface. */
#define HAVE_SYSLOG 1

/* Define to 1 if you have the <sys/epoll.h> header file. */
/* #undef HAVE_SYS_EPOLL_H */

/* Define to 1 if you have the <sys/event.h> header file. */
#define HAVE_SYS_EVENT_H 1

/* Define to 1 if you have the <sys/ipc.h> header file. */
#define HAVE_SYS_IPC_H 1

/* Define to 1 if you have the <sys/personality.h> header file. */
/* #undef HAVE_SYS_PERSONALITY_H */

/* Define to 1 if you have the <sys/prctl.h> header file. */
/* #undef HAVE_SYS_PRCTL_H */

/* Define to 1 if you have the <sys/procctl.h> header file. */
/* #undef HAVE_SYS_PROCCTL_H */

/* Define to 1 if you have the <sys/pstat.h> header file. */
/* #undef HAVE_SYS_PSTAT_H */

/* Define to 1 if you have the <sys/resource.h> header file. */
#define HAVE_SYS_RESOURCE_H 1

/* Define to 1 if you have the <sys/select.h> header file. */
#define HAVE_SYS_SELECT_H 1

/* Define to 1 if you have the <sys/sem.h> header file. */
#define HAVE_SYS_SEM_H 1

/* Define to 1 if you have the <sys/shm.h> header file. */
#define HAVE_SYS_SHM_H 1

/* Define to 1 if you have the <sys/signalfd.h> header file. */
/* #undef HAVE_SYS_SIGNALFD_H */

/* Define to 1 if you have the <sys/sockio.h> header file. */
#define HAVE_SYS_SOCKIO_H 1

/* Define to 1 if you have the <sys/stat.h> header file. */
#define HAVE_SYS_STAT_H 1

/* Define to 1 if you have the <sys/tas.h> header file. */
/* #undef HAVE_SYS_TAS_H */

/* Define to 1 if you have the <sys/types.h> header file. */
#define HAVE_SYS_TYPES_H 1

/* Define to 1 if you have the <sys/ucred.h> header file. */
#define HAVE_SYS_UCRED_H 1

/* Define to 1 if you have the <sys/uio.h> header file. */
#define HAVE_SYS_UIO_H 1

/* Define to 1 if you have the <sys/un.h> header file. */
#define HAVE_SYS_UN_H 1

/* Define to 1 if you have the <termios.h> header file. */
#define HAVE_TERMIOS_H 1

/* Define to 1 if your compiler understands `typeof' or something similar. */
#define HAVE_TYPEOF 1

/* Define to 1 if you have the <ucred.h> header file. */
/* #undef HAVE_UCRED_H */

/* Define to 1 if the system has the type `uint64'. */
/* #undef HAVE_UINT64 */

/* Define to 1 if the system has the type `uint8'. */
/* #undef HAVE_UINT8 */

/* Define to 1 if the system has the type `union semun'. */
#define HAVE_UNION_SEMUN 1

/* Define to 1 if you have the <unistd.h> header file. */
#define HAVE_UNISTD_H 1

/* Define to 1 if you have the `unsetenv' function. */
#define HAVE_UNSETENV 1

/* Define to 1 if you have the `uselocale' function. */
#define HAVE_USELOCALE 1

/* Define to 1 if you have BSD UUID support. */
/* #undef HAVE_UUID_BSD */

/* Define to 1 if you have E2FS UUID support. */
/* #undef HAVE_UUID_E2FS */

/* Define to 1 if you have the <uuid.h> header file. */
/* #undef HAVE_UUID_H */

/* Define to 1 if you have OSSP UUID support. */
/* #undef HAVE_UUID_OSSP */

/* Define to 1 if you have the <uuid/uuid.h> header file. */
/* #undef HAVE_UUID_UUID_H */

/* Define to 1 if you have the `wcstombs_l' function. */
#define HAVE_WCSTOMBS_L 1

/* Define to 1 if you have the <wctype.h> header file. */
#define HAVE_WCTYPE_H 1

/* Define to 1 if you have the <winldap.h> header file. */
/* #undef HAVE_WINLDAP_H */

/* Define to 1 if you have the `writev' function. */
#define HAVE_WRITEV 1

/* Define to 1 if you have the `X509_get_signature_nid' function. */
#define HAVE_X509_GET_SIGNATURE_NID 1

/* Define to 1 if the assembler supports X86_64's POPCNTQ instruction. */
#define HAVE_X86_64_POPCNTQ 1

/* Define to 1 if the system has the type `_Bool'. */
#define HAVE__BOOL 1

/* Define to 1 if your compiler understands __builtin_bswap16. */
#define HAVE__BUILTIN_BSWAP16 1

/* Define to 1 if your compiler understands __builtin_bswap32. */
#define HAVE__BUILTIN_BSWAP32 1

/* Define to 1 if your compiler understands __builtin_bswap64. */
#define HAVE__BUILTIN_BSWAP64 1

/* Define to 1 if your compiler understands __builtin_clz. */
#define HAVE__BUILTIN_CLZ 1

/* Define to 1 if your compiler understands __builtin_constant_p. */
#define HAVE__BUILTIN_CONSTANT_P 1

/* Define to 1 if your compiler understands __builtin_ctz. */
#define HAVE__BUILTIN_CTZ 1

/* Define to 1 if your compiler understands __builtin_frame_address. */
#define HAVE__BUILTIN_FRAME_ADDRESS 1

/* Define to 1 if your compiler understands __builtin_$op_overflow. */
#define HAVE__BUILTIN_OP_OVERFLOW 1

/* Define to 1 if your compiler understands __builtin_popcount. */
#define HAVE__BUILTIN_POPCOUNT 1

/* Define to 1 if your compiler understands __builtin_types_compatible_p. */
#define HAVE__BUILTIN_TYPES_COMPATIBLE_P 1

/* Define to 1 if your compiler understands __builtin_unreachable. */
#define HAVE__BUILTIN_UNREACHABLE 1

/* Define to 1 if you have the `_configthreadlocale' function. */
/* #undef HAVE__CONFIGTHREADLOCALE */

/* Define to 1 if you have __cpuid. */
/* #undef HAVE__CPUID */

/* Define to 1 if you have __get_cpuid. */
#define HAVE__GET_CPUID 1

/* Define to 1 if your compiler understands _Static_assert. */
#define HAVE__STATIC_ASSERT 1

/* Define to 1 if you have the `__strtoll' function. */
/* #undef HAVE___STRTOLL */

/* Define to 1 if you have the `__strtoull' function. */
/* #undef HAVE___STRTOULL */

/* Define to the appropriate printf length modifier for 64-bit ints. */
#define INT64_MODIFIER "l"

/* Define to 1 if `locale_t' requires <xlocale.h>. */
#define LOCALE_T_IN_XLOCALE 1

/* Define as the maximum alignment requirement of any C data type. */
#define MAXIMUM_ALIGNOF 8

/* Define bytes to use libc memset(). */
#define MEMSET_LOOP_LIMIT 1024

/* Define to the OpenSSL API version in use. This avoids deprecation warnings
   from newer OpenSSL versions. */
#define OPENSSL_API_COMPAT 0x10001000L

/* Define to the address where bug reports for this package should be sent. */
#define PACKAGE_BUGREPORT "pgsql-bugs@lists.postgresql.org"

/* Define to the full name of this package. */
#define PACKAGE_NAME "PostgreSQL"

/* Define to the full name and version of this package. */
#define PACKAGE_STRING "PostgreSQL 15.2"

/* Define to the one symbol short name of this package. */
#define PACKAGE_TARNAME "postgresql"

/* Define to the home page for this package. */
#define PACKAGE_URL "https://www.postgresql.org/"

/* Define to the version of this package. */
#define PACKAGE_VERSION "15.2"

/* Define to the name of a signed 128-bit integer type. */
#define PG_INT128_TYPE __int128

/* Define to the name of a signed 64-bit integer type. */
#define PG_INT64_TYPE long int

/* Define to the name of the default PostgreSQL service principal in Kerberos
   (GSSAPI). (--with-krb-srvnam=NAME) */
#define PG_KRB_SRVNAM "postgres"

/* PostgreSQL major version as a string */
#define PG_MAJORVERSION "15"

/* PostgreSQL major version number */
#define PG_MAJORVERSION_NUM 15

/* PostgreSQL minor version number */
#define PG_MINORVERSION_NUM 2

/* Define to best printf format archetype, usually gnu_printf if available. */
#define PG_PRINTF_ATTRIBUTE printf

/* Define to 1 to use <stdbool.h> to define type bool. */
#define PG_USE_STDBOOL 1

/* PostgreSQL version as a string */
#define PG_VERSION "15.2"

/* PostgreSQL version as a number */
#define PG_VERSION_NUM 150002

/* A string containing the version number, platform, and C compiler */
#define PG_VERSION_STR "PostgreSQL 15.2 on x86_64-apple-darwin22.4.0, compiled by Apple clang version 13.0.0 (clang-1300.0.29.30), 64-bit"

/* Define to 1 to allow profiling output to be saved separately for each
   process. */
/* #undef PROFILE_PID_DIR */

/* Define to necessary symbol if this constant uses a non-standard name on
   your system. */
/* #undef PTHREAD_CREATE_JOINABLE */

/* RELSEG_SIZE is the maximum number of blocks allowed in one disk file. Thus,
   the maximum size of a single file is RELSEG_SIZE * BLCKSZ; relations bigger
   than that are divided into multiple files. RELSEG_SIZE * BLCKSZ must be
   less than your OS' limit on file size. This is often 2 GB or 4GB in a
   32-bit operating system, unless you have large file support enabled. By
   default, we make the limit 1 GB to avoid any possible integer-overflow
   problems within the OS. A limit smaller than necessary only means we divide
   a large relation into more chunks than necessary, so it seems best to err
   in the direction of a small limit. A power-of-2 value is recommended to
   save a few cycles in md.c, but is not absolutely required. Changing
   RELSEG_SIZE requires an initdb. */
#define RELSEG_SIZE 131072

/* The size of `bool', as computed by sizeof. */
#define SIZEOF_BOOL 1

/* The size of `long', as computed by sizeof. */
#define SIZEOF_LONG 8

/* The size of `off_t', as computed by sizeof. */
#define SIZEOF_OFF_T 8

/* The size of `size_t', as computed by sizeof. */
#define SIZEOF_SIZE_T 8

/* The size of `void *', as computed by sizeof. */
#define SIZEOF_VOID_P 8

/* Define to 1 if you have the ANSI C header files. */
#define STDC_HEADERS 1

/* Define to 1 if strerror_r() returns int. */
#define STRERROR_R_INT 1

/* Define to 1 to use ARMv8 CRC Extension. */
/* #undef USE_ARMV8_CRC32C */

/* Define to 1 to use ARMv8 CRC Extension with a runtime check. */
/* #undef USE_ARMV8_CRC32C_WITH_RUNTIME_CHECK */

/* Define to 1 to build with assertion checks. (--enable-cassert) */
#define USE_ASSERT_CHECKING 1

/* Define to 1 to build with Bonjour support. (--with-bonjour) */
/* #undef USE_BONJOUR */

/* Define to 1 to build with BSD Authentication support. (--with-bsd-auth) */
/* #undef USE_BSD_AUTH */

/* Define to build with ICU support. (--with-icu) */
/* #undef USE_ICU */

/* Define to 1 to build with LDAP support. (--with-ldap) */
/* #undef USE_LDAP */

/* Define to 1 to build with XML support. (--with-libxml) */
/* #undef USE_LIBXML */

/* Define to 1 to use XSLT support when building contrib/xml2.
   (--with-libxslt) */
/* #undef USE_LIBXSLT */

/* Define to 1 to build with LLVM based JIT support. (--with-llvm) */
/* #undef USE_LLVM */

/* Define to 1 to build with LZ4 support. (--with-lz4) */
/* #undef USE_LZ4 */

/* Define to select named POSIX semaphores. */
/* #undef USE_NAMED_POSIX_SEMAPHORES */

/* Define to 1 to build with OpenSSL support. (--with-ssl=openssl) */
#define USE_OPENSSL 1

/* Define to 1 to build with PAM support. (--with-pam) */
/* #undef USE_PAM */

/* Define to 1 to use software CRC-32C implementation (slicing-by-8). */
/* #undef USE_SLICING_BY_8_CRC32C */

/* Define to 1 use Intel SSE 4.2 CRC instructions. */
/* #undef USE_SSE42_CRC32C */

/* Define to 1 to use Intel SSE 4.2 CRC instructions with a runtime check. */
#define USE_SSE42_CRC32C_WITH_RUNTIME_CHECK 1

/* Define to build with systemd support. (--with-systemd) */
/* #undef USE_SYSTEMD */

/* Define to select SysV-style semaphores. */
#define USE_SYSV_SEMAPHORES 1

/* Define to select SysV-style shared memory. */
#define USE_SYSV_SHARED_MEMORY 1

/* Define to select unnamed POSIX semaphores. */
/* #undef USE_UNNAMED_POSIX_SEMAPHORES */

/* Define to select Win32-style semaphores. */
/* #undef USE_WIN32_SEMAPHORES */

/* Define to select Win32-style shared memory. */
/* #undef USE_WIN32_SHARED_MEMORY */

/* Define to 1 to build with ZSTD support. (--with-zstd) */
/* #undef USE_ZSTD */

/* Define to 1 if `wcstombs_l' requires <xlocale.h>. */
#define WCSTOMBS_L_IN_XLOCALE 1

/* Define WORDS_BIGENDIAN to 1 if your processor stores words with the most
   significant byte first (like Motorola and SPARC, unlike Intel). */
#if defined AC_APPLE_UNIVERSAL_BUILD
# if defined __BIG_ENDIAN__
#  define WORDS_BIGENDIAN 1
# endif
#else
# ifndef WORDS_BIGENDIAN
/* #  undef WORDS_BIGENDIAN */
# endif
#endif

/* Size of a WAL file block. This need have no particular relation to BLCKSZ.
   XLOG_BLCKSZ must be a power of 2, and if your system supports O_DIRECT I/O,
   XLOG_BLCKSZ must be a multiple of the alignment requirement for direct-I/O
   buffers, else direct I/O may fail. Changing XLOG_BLCKSZ requires an initdb.
   */
#define XLOG_BLCKSZ 8192



/* Number of bits in a file offset, on hosts where this is settable. */
/* #undef _FILE_OFFSET_BITS */

/* Define to 1 to make fseeko visible on some hosts (e.g. glibc 2.2). */
/* #undef _LARGEFILE_SOURCE */

/* Define for large files, on AIX-style hosts. */
/* #undef _LARGE_FILES */

/* Define to `__inline__' or `__inline' if that's what the C compiler
   calls it, or to nothing if 'inline' is not supported under any name.  */
#ifndef __cplusplus
/* #undef inline */
#endif

/* Define to keyword to use for C99 restrict support, or to nothing if not
   supported */
#define pg_restrict __restrict

/* Define to the equivalent of the C99 'restrict' keyword, or to
   nothing if this is not supported.  Do not define if restrict is
   supported directly.  */
#define restrict __restrict
/* Work around a bug in Sun C++: it does not support _Restrict or
   __restrict__, even though the corresponding Sun C compiler ends up with
   "#define restrict _Restrict" or "#define restrict __restrict__" in the
   previous line.  Perhaps some future version of Sun C++ will work with
   restrict; if so, hopefully it defines __RESTRICT like Sun C does.  */
#if defined __SUNPRO_CC && !defined __RESTRICT
# define _Restrict
# define __restrict__
#endif

/* Define to how the compiler spells `typeof'. */
/* #undef typeof */

/*------------------------------------------------------------------------
 * PostgreSQL manual configuration settings
 *
 * This file contains various configuration symbols and limits.  In
 * all cases, changing them is only useful in very rare situations or
 * for developers.  If you edit any of these, be sure to do a *full*
 * rebuild (and an initdb if noted).
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/pg_config_manual.h
 *------------------------------------------------------------------------
 */

/*
 * This is the default value for wal_segment_size to be used when initdb is run
 * without the --wal-segsize option.  It must be a valid segment size.
 */
#define DEFAULT_XLOG_SEG_SIZE	(16*1024*1024)

/*
 * Maximum length for identifiers (e.g. table names, column names,
 * function names).  Names actually are limited to one fewer byte than this,
 * because the length must include a trailing zero byte.
 *
 * Changing this requires an initdb.
 */
#define NAMEDATALEN 64

/*
 * Maximum number of arguments to a function.
 *
 * The minimum value is 8 (GIN indexes use 8-argument support functions).
 * The maximum possible value is around 600 (limited by index tuple size in
 * pg_proc's index; BLCKSZ larger than 8K would allow more).  Values larger
 * than needed will waste memory and processing time, but do not directly
 * cost disk space.
 *
 * Changing this does not require an initdb, but it does require a full
 * backend recompile (including any user-defined C functions).
 */
#define FUNC_MAX_ARGS		100

/*
 * When creating a product derived from PostgreSQL with changes that cause
 * incompatibilities for loadable modules, it is recommended to change this
 * string so that dfmgr.c can refuse to load incompatible modules with a clean
 * error message.  Typical examples that cause incompatibilities are any
 * changes to node tags or node structures.  (Note that dfmgr.c already
 * detects common sources of incompatibilities due to major version
 * differences and due to some changed compile-time constants.  This setting
 * is for catching anything that cannot be detected in a straightforward way.)
 *
 * There is no prescribed format for the string.  The suggestion is to include
 * product or company name, and optionally any internally-relevant ABI
 * version.  Example: "ACME Postgres/1.2".  Note that the string will appear
 * in a user-facing error message if an ABI mismatch is detected.
 */
#define FMGR_ABI_EXTRA		"Neon Postgres"

/*
 * Maximum number of columns in an index.  There is little point in making
 * this anything but a multiple of 32, because the main cost is associated
 * with index tuple header size (see access/itup.h).
 *
 * Changing this requires an initdb.
 */
#define INDEX_MAX_KEYS		32

/*
 * Maximum number of columns in a partition key
 */
#define PARTITION_MAX_KEYS	32

/*
 * Decide whether built-in 8-byte types, including float8, int8, and
 * timestamp, are passed by value.  This is on by default if sizeof(Datum) >=
 * 8 (that is, on 64-bit platforms).  If sizeof(Datum) < 8 (32-bit platforms),
 * this must be off.  We keep this here as an option so that it is easy to
 * test the pass-by-reference code paths on 64-bit platforms.
 *
 * Changing this requires an initdb.
 */
#if SIZEOF_VOID_P >= 8
#define USE_FLOAT8_BYVAL 1
#endif

/*
 * When we don't have native spinlocks, we use semaphores to simulate them.
 * Decreasing this value reduces consumption of OS resources; increasing it
 * may improve performance, but supplying a real spinlock implementation is
 * probably far better.
 */
#define NUM_SPINLOCK_SEMAPHORES		128

/*
 * When we have neither spinlocks nor atomic operations support we're
 * implementing atomic operations on top of spinlock on top of semaphores. To
 * be safe against atomic operations while holding a spinlock separate
 * semaphores have to be used.
 */
#define NUM_ATOMICS_SEMAPHORES		64

/*
 * MAXPGPATH: standard size of a pathname buffer in PostgreSQL (hence,
 * maximum usable pathname length is one less).
 *
 * We'd use a standard system header symbol for this, if there weren't
 * so many to choose from: MAXPATHLEN, MAX_PATH, PATH_MAX are all
 * defined by different "standards", and often have different values
 * on the same platform!  So we just punt and use a reasonably
 * generous setting here.
 */
#define MAXPGPATH		1024

/*
 * PG_SOMAXCONN: maximum accept-queue length limit passed to
 * listen(2).  You'd think we should use SOMAXCONN from
 * <sys/socket.h>, but on many systems that symbol is much smaller
 * than the kernel's actual limit.  In any case, this symbol need be
 * twiddled only if you have a kernel that refuses large limit values,
 * rather than silently reducing the value to what it can handle
 * (which is what most if not all Unixen do).
 */
#define PG_SOMAXCONN	10000

/*
 * You can try changing this if you have a machine with bytes of
 * another size, but no guarantee...
 */
#define BITS_PER_BYTE		8

/*
 * Preferred alignment for disk I/O buffers.  On some CPUs, copies between
 * user space and kernel space are significantly faster if the user buffer
 * is aligned on a larger-than-MAXALIGN boundary.  Ideally this should be
 * a platform-dependent value, but for now we just hard-wire it.
 */
#define ALIGNOF_BUFFER	32

/*
 * If EXEC_BACKEND is defined, the postmaster uses an alternative method for
 * starting subprocesses: Instead of simply using fork(), as is standard on
 * Unix platforms, it uses fork()+exec() or something equivalent on Windows,
 * as well as lots of extra code to bring the required global state to those
 * new processes.  This must be enabled on Windows (because there is no
 * fork()).  On other platforms, it's only useful for verifying those
 * otherwise Windows-specific code paths.
 */
#if defined(WIN32) && !defined(__CYGWIN__)
#define EXEC_BACKEND
#endif

/*
 * Define this if your operating system supports link()
 */
#if !defined(WIN32) && !defined(__CYGWIN__)
#define HAVE_WORKING_LINK 1
#endif

/*
 * USE_POSIX_FADVISE controls whether Postgres will attempt to use the
 * posix_fadvise() kernel call.  Usually the automatic configure tests are
 * sufficient, but some older Linux distributions had broken versions of
 * posix_fadvise().  If necessary you can remove the #define here.
 */
#if HAVE_DECL_POSIX_FADVISE && defined(HAVE_POSIX_FADVISE)
#define USE_POSIX_FADVISE
#endif

/*
 * USE_PREFETCH code should be compiled only if we have a way to implement
 * prefetching.  (This is decoupled from USE_POSIX_FADVISE because there
 * might in future be support for alternative low-level prefetch APIs.
 * If you change this, you probably need to adjust the error message in
 * check_effective_io_concurrency.)
 */
#ifdef USE_POSIX_FADVISE
#define USE_PREFETCH
#endif

/*
 * Default and maximum values for backend_flush_after, bgwriter_flush_after
 * and checkpoint_flush_after; measured in blocks.  Currently, these are
 * enabled by default if sync_file_range() exists, ie, only on Linux.  Perhaps
 * we could also enable by default if we have mmap and msync(MS_ASYNC)?
 */
#ifdef HAVE_SYNC_FILE_RANGE
#define DEFAULT_BACKEND_FLUSH_AFTER 0	/* never enabled by default */
#define DEFAULT_BGWRITER_FLUSH_AFTER 64
#define DEFAULT_CHECKPOINT_FLUSH_AFTER 32
#else
#define DEFAULT_BACKEND_FLUSH_AFTER 0
#define DEFAULT_BGWRITER_FLUSH_AFTER 0
#define DEFAULT_CHECKPOINT_FLUSH_AFTER 0
#endif
/* upper limit for all three variables */
#define WRITEBACK_MAX_PENDING_FLUSHES 256

/*
 * USE_SSL code should be compiled only when compiling with an SSL
 * implementation.
 */
#ifdef USE_OPENSSL
#define USE_SSL
#endif

/*
 * This is the default directory in which AF_UNIX socket files are
 * placed.  Caution: changing this risks breaking your existing client
 * applications, which are likely to continue to look in the old
 * directory.  But if you just hate the idea of sockets in /tmp,
 * here's where to twiddle it.  You can also override this at runtime
 * with the postmaster's -k switch.
 *
 * If set to an empty string, then AF_UNIX sockets are not used by default: A
 * server will not create an AF_UNIX socket unless the run-time configuration
 * is changed, a client will connect via TCP/IP by default and will only use
 * an AF_UNIX socket if one is explicitly specified.
 *
 * This is done by default on Windows because there is no good standard
 * location for AF_UNIX sockets and many installations on Windows don't
 * support them yet.
 */
#ifndef WIN32
#define DEFAULT_PGSOCKET_DIR  "/tmp"
#else
#define DEFAULT_PGSOCKET_DIR ""
#endif

/*
 * This is the default event source for Windows event log.
 */
#define DEFAULT_EVENT_SOURCE  "PostgreSQL"

/*
 * On PPC machines, decide whether to use the mutex hint bit in LWARX
 * instructions.  Setting the hint bit will slightly improve spinlock
 * performance on POWER6 and later machines, but does nothing before that,
 * and will result in illegal-instruction failures on some pre-POWER4
 * machines.  By default we use the hint bit when building for 64-bit PPC,
 * which should be safe in nearly all cases.  You might want to override
 * this if you are building 32-bit code for a known-recent PPC machine.
 */
#ifdef HAVE_PPC_LWARX_MUTEX_HINT	/* must have assembler support in any case */
#if defined(__ppc64__) || defined(__powerpc64__)
#define USE_PPC_LWARX_MUTEX_HINT
#endif
#endif

/*
 * On PPC machines, decide whether to use LWSYNC instructions in place of
 * ISYNC and SYNC.  This provides slightly better performance, but will
 * result in illegal-instruction failures on some pre-POWER4 machines.
 * By default we use LWSYNC when building for 64-bit PPC, which should be
 * safe in nearly all cases.
 */
#if defined(__ppc64__) || defined(__powerpc64__)
#define USE_PPC_LWSYNC
#endif

/*
 * Assumed cache line size. This doesn't affect correctness, but can be used
 * for low-level optimizations. Currently, this is used to pad some data
 * structures in xlog.c, to ensure that highly-contended fields are on
 * different cache lines. Too small a value can hurt performance due to false
 * sharing, while the only downside of too large a value is a few bytes of
 * wasted memory. The default is 128, which should be large enough for all
 * supported platforms.
 */
#define PG_CACHE_LINE_SIZE		128

/*
 *------------------------------------------------------------------------
 * The following symbols are for enabling debugging code, not for
 * controlling user-visible features or resource limits.
 *------------------------------------------------------------------------
 */

/*
 * Include Valgrind "client requests", mostly in the memory allocator, so
 * Valgrind understands PostgreSQL memory contexts.  This permits detecting
 * memory errors that Valgrind would not detect on a vanilla build.  It also
 * enables detection of buffer accesses that take place without holding a
 * buffer pin (or without holding a buffer lock in the case of index access
 * methods that superimpose their own custom client requests on top of the
 * generic bufmgr.c requests).
 *
 * "make installcheck" is significantly slower under Valgrind.  The client
 * requests fall in hot code paths, so USE_VALGRIND slows execution by a few
 * percentage points even when not run under Valgrind.
 *
 * Do not try to test the server under Valgrind without having built the
 * server with USE_VALGRIND; else you will get false positives from sinval
 * messaging (see comments in AddCatcacheInvalidationMessage).  It's also
 * important to use the suppression file src/tools/valgrind.supp to
 * exclude other known false positives.
 *
 * You should normally use MEMORY_CONTEXT_CHECKING with USE_VALGRIND;
 * instrumentation of repalloc() is inferior without it.
 */
/* #define USE_VALGRIND */

/*
 * Define this to cause pfree()'d memory to be cleared immediately, to
 * facilitate catching bugs that refer to already-freed values.
 * Right now, this gets defined automatically if --enable-cassert.
 */
#ifdef USE_ASSERT_CHECKING
#define CLOBBER_FREED_MEMORY
#endif

/*
 * Define this to check memory allocation errors (scribbling on more
 * bytes than were allocated).  Right now, this gets defined
 * automatically if --enable-cassert or USE_VALGRIND.
 */
#if defined(USE_ASSERT_CHECKING) || defined(USE_VALGRIND)
#define MEMORY_CONTEXT_CHECKING
#endif

/*
 * Define this to cause palloc()'d memory to be filled with random data, to
 * facilitate catching code that depends on the contents of uninitialized
 * memory.  Caution: this is horrendously expensive.
 */
/* #define RANDOMIZE_ALLOCATED_MEMORY */

/*
 * For cache-invalidation debugging, define DISCARD_CACHES_ENABLED to enable
 * use of the debug_discard_caches GUC to aggressively flush syscache/relcache
 * entries whenever it's possible to deliver invalidations.  See
 * AcceptInvalidationMessages() in src/backend/utils/cache/inval.c for
 * details.
 *
 * USE_ASSERT_CHECKING builds default to enabling this.  It's possible to use
 * DISCARD_CACHES_ENABLED without a cassert build and the implied
 * CLOBBER_FREED_MEMORY and MEMORY_CONTEXT_CHECKING options, but it's unlikely
 * to be as effective at identifying problems.
 */
/* #define DISCARD_CACHES_ENABLED */

#if defined(USE_ASSERT_CHECKING) && !defined(DISCARD_CACHES_ENABLED)
#define DISCARD_CACHES_ENABLED
#endif

/*
 * Backwards compatibility for the older compile-time-only clobber-cache
 * macros.
 */
#if !defined(DISCARD_CACHES_ENABLED) && (defined(CLOBBER_CACHE_ALWAYS) || defined(CLOBBER_CACHE_RECURSIVELY))
#define DISCARD_CACHES_ENABLED
#endif

/*
 * Recover memory used for relcache entries when invalidated.  See
 * RelationBuildDescr() in src/backend/utils/cache/relcache.c.
 *
 * This is active automatically for clobber-cache builds when clobbering is
 * active, but can be overridden here by explicitly defining
 * RECOVER_RELATION_BUILD_MEMORY.  Define to 1 to always free relation cache
 * memory even when clobber is off, or to 0 to never free relation cache
 * memory even when clobbering is on.
 */
 /* #define RECOVER_RELATION_BUILD_MEMORY 0 */	/* Force disable */
 /* #define RECOVER_RELATION_BUILD_MEMORY 1 */	/* Force enable */

/*
 * Define this to force all parse and plan trees to be passed through
 * copyObject(), to facilitate catching errors and omissions in
 * copyObject().
 */
/* #define COPY_PARSE_PLAN_TREES */

/*
 * Define this to force all parse and plan trees to be passed through
 * outfuncs.c/readfuncs.c, to facilitate catching errors and omissions in
 * those modules.
 */
/* #define WRITE_READ_PARSE_PLAN_TREES */

/*
 * Define this to force all raw parse trees for DML statements to be scanned
 * by raw_expression_tree_walker(), to facilitate catching errors and
 * omissions in that function.
 */
/* #define RAW_EXPRESSION_COVERAGE_TEST */

/*
 * Enable debugging print statements for lock-related operations.
 */
/* #define LOCK_DEBUG */

/*
 * Enable debugging print statements for WAL-related operations; see
 * also the wal_debug GUC var.
 */
/* #define WAL_DEBUG */

/*
 * Enable tracing of resource consumption during sort operations;
 * see also the trace_sort GUC var.  For 8.1 this is enabled by default.
 */
#define TRACE_SORT 1

/*
 * Enable tracing of syncscan operations (see also the trace_syncscan GUC var).
 */
/* #define TRACE_SYNCSCAN */

/* src/include/port/darwin.h */

#define __darwin__	1

#if HAVE_DECL_F_FULLFSYNC		/* not present before macOS 10.3 */
#define HAVE_FSYNC_WRITETHROUGH

#endif


/* System header files that should be available everywhere in Postgres */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include <stdarg.h>
#ifdef HAVE_STRINGS_H
#include <strings.h>
#endif
#include <stdint.h>
#include <sys/types.h>
#include <errno.h>
#if defined(WIN32) || defined(__CYGWIN__)
#include <fcntl.h>				/* ensure O_BINARY is available */
#endif
#include <locale.h>
#ifdef ENABLE_NLS
#include <libintl.h>
#endif


/* ----------------------------------------------------------------
 *				Section 1: compiler characteristics
 *
 * type prefixes (const, signed, volatile, inline) are handled in pg_config.h.
 * ----------------------------------------------------------------
 */

/*
 * Disable "inline" if PG_FORCE_DISABLE_INLINE is defined.
 * This is used to work around compiler bugs and might also be useful for
 * investigatory purposes.
 */
#ifdef PG_FORCE_DISABLE_INLINE
#undef inline
#define inline
#endif

/*
 * Attribute macros
 *
 * GCC: https://gcc.gnu.org/onlinedocs/gcc/Function-Attributes.html
 * GCC: https://gcc.gnu.org/onlinedocs/gcc/Type-Attributes.html
 * Clang: https://clang.llvm.org/docs/AttributeReference.html
 * Sunpro: https://docs.oracle.com/cd/E18659_01/html/821-1384/gjzke.html
 * XLC: https://www.ibm.com/support/knowledgecenter/SSGH2K_13.1.2/com.ibm.xlc131.aix.doc/language_ref/function_attributes.html
 * XLC: https://www.ibm.com/support/knowledgecenter/SSGH2K_13.1.2/com.ibm.xlc131.aix.doc/language_ref/type_attrib.html
 */

/*
 * For compilers which don't support __has_attribute, we just define
 * __has_attribute(x) to 0 so that we can define macros for various
 * __attribute__s more easily below.
 */
#ifndef __has_attribute
#define __has_attribute(attribute) 0
#endif

/* only GCC supports the unused attribute */
#ifdef __GNUC__
#define pg_attribute_unused() __attribute__((unused))
#else
#define pg_attribute_unused()
#endif

/*
 * pg_nodiscard means the compiler should warn if the result of a function
 * call is ignored.  The name "nodiscard" is chosen in alignment with
 * (possibly future) C and C++ standards.  For maximum compatibility, use it
 * as a function declaration specifier, so it goes before the return type.
 */
#ifdef __GNUC__
#define pg_nodiscard __attribute__((warn_unused_result))
#else
#define pg_nodiscard
#endif

/*
 * Place this macro before functions that should be allowed to make misaligned
 * accesses.  Think twice before using it on non-x86-specific code!
 * Testing can be done with "-fsanitize=alignment -fsanitize-trap=alignment"
 * on clang, or "-fsanitize=alignment -fno-sanitize-recover=alignment" on gcc.
 */
#if __clang_major__ >= 7 || __GNUC__ >= 8
#define pg_attribute_no_sanitize_alignment() __attribute__((no_sanitize("alignment")))
#else
#define pg_attribute_no_sanitize_alignment()
#endif

/*
 * Append PG_USED_FOR_ASSERTS_ONLY to definitions of variables that are only
 * used in assert-enabled builds, to avoid compiler warnings about unused
 * variables in assert-disabled builds.
 */
#ifdef USE_ASSERT_CHECKING
#define PG_USED_FOR_ASSERTS_ONLY
#else
#define PG_USED_FOR_ASSERTS_ONLY pg_attribute_unused()
#endif

/* GCC and XLC support format attributes */
#if defined(__GNUC__) || defined(__IBMC__)
#define pg_attribute_format_arg(a) __attribute__((format_arg(a)))
#define pg_attribute_printf(f,a) __attribute__((format(PG_PRINTF_ATTRIBUTE, f, a)))
#else
#define pg_attribute_format_arg(a)
#define pg_attribute_printf(f,a)
#endif

/* GCC, Sunpro and XLC support aligned, packed and noreturn */
#if defined(__GNUC__) || defined(__SUNPRO_C) || defined(__IBMC__)
#define pg_attribute_aligned(a) __attribute__((aligned(a)))
#define pg_attribute_noreturn() __attribute__((noreturn))
#define pg_attribute_packed() __attribute__((packed))
#define HAVE_PG_ATTRIBUTE_NORETURN 1
#else
/*
 * NB: aligned and packed are not given default definitions because they
 * affect code functionality; they *must* be implemented by the compiler
 * if they are to be used.
 */
#define pg_attribute_noreturn()
#endif

/*
 * Use "pg_attribute_always_inline" in place of "inline" for functions that
 * we wish to force inlining of, even when the compiler's heuristics would
 * choose not to.  But, if possible, don't force inlining in unoptimized
 * debug builds.
 */
#if (defined(__GNUC__) && __GNUC__ > 3 && defined(__OPTIMIZE__)) || defined(__SUNPRO_C) || defined(__IBMC__)
/* GCC > 3, Sunpro and XLC support always_inline via __attribute__ */
#define pg_attribute_always_inline __attribute__((always_inline)) inline
#elif defined(_MSC_VER)
/* MSVC has a special keyword for this */
#define pg_attribute_always_inline __forceinline
#else
/* Otherwise, the best we can do is to say "inline" */
#define pg_attribute_always_inline inline
#endif

/*
 * Forcing a function not to be inlined can be useful if it's the slow path of
 * a performance-critical function, or should be visible in profiles to allow
 * for proper cost attribution.  Note that unlike the pg_attribute_XXX macros
 * above, this should be placed before the function's return type and name.
 */
/* GCC, Sunpro and XLC support noinline via __attribute__ */
#if (defined(__GNUC__) && __GNUC__ > 2) || defined(__SUNPRO_C) || defined(__IBMC__)
#define pg_noinline __attribute__((noinline))
/* msvc via declspec */
#elif defined(_MSC_VER)
#define pg_noinline __declspec(noinline)
#else
#define pg_noinline
#endif

/*
 * For now, just define pg_attribute_cold and pg_attribute_hot to be empty
 * macros on minGW 8.1.  There appears to be a compiler bug that results in
 * compilation failure.  At this time, we still have at least one buildfarm
 * animal running that compiler, so this should make that green again. It's
 * likely this compiler is not popular enough to warrant keeping this code
 * around forever, so let's just remove it once the last buildfarm animal
 * upgrades.
 */
#if defined(__MINGW64__) && __GNUC__ == 8 && __GNUC_MINOR__ == 1

#define pg_attribute_cold
#define pg_attribute_hot

#else
/*
 * Marking certain functions as "hot" or "cold" can be useful to assist the
 * compiler in arranging the assembly code in a more efficient way.
 */
#if __has_attribute (cold)
#define pg_attribute_cold __attribute__((cold))
#else
#define pg_attribute_cold
#endif

#if __has_attribute (hot)
#define pg_attribute_hot __attribute__((hot))
#else
#define pg_attribute_hot
#endif

#endif							/* defined(__MINGW64__) && __GNUC__ == 8 &&
								 * __GNUC_MINOR__ == 1 */
/*
 * Mark a point as unreachable in a portable fashion.  This should preferably
 * be something that the compiler understands, to aid code generation.
 * In assert-enabled builds, we prefer abort() for debugging reasons.
 */
#if defined(HAVE__BUILTIN_UNREACHABLE) && !defined(USE_ASSERT_CHECKING)
#define pg_unreachable() __builtin_unreachable()
#elif defined(_MSC_VER) && !defined(USE_ASSERT_CHECKING)
#define pg_unreachable() __assume(0)
#else
#define pg_unreachable() abort()
#endif

/*
 * Hints to the compiler about the likelihood of a branch. Both likely() and
 * unlikely() return the boolean value of the contained expression.
 *
 * These should only be used sparingly, in very hot code paths. It's very easy
 * to mis-estimate likelihoods.
 */
#if __GNUC__ >= 3
#define likely(x)	__builtin_expect((x) != 0, 1)
#define unlikely(x) __builtin_expect((x) != 0, 0)
#else
#define likely(x)	((x) != 0)
#define unlikely(x) ((x) != 0)
#endif

/*
 * CppAsString
 *		Convert the argument to a string, using the C preprocessor.
 * CppAsString2
 *		Convert the argument to a string, after one round of macro expansion.
 * CppConcat
 *		Concatenate two arguments together, using the C preprocessor.
 *
 * Note: There used to be support here for pre-ANSI C compilers that didn't
 * support # and ##.  Nowadays, these macros are just for clarity and/or
 * backward compatibility with existing PostgreSQL code.
 */
#define CppAsString(identifier) #identifier
#define CppAsString2(x)			CppAsString(x)
#define CppConcat(x, y)			x##y

/*
 * VA_ARGS_NARGS
 *		Returns the number of macro arguments it is passed.
 *
 * An empty argument still counts as an argument, so effectively, this is
 * "one more than the number of commas in the argument list".
 *
 * This works for up to 63 arguments.  Internally, VA_ARGS_NARGS_() is passed
 * 64+N arguments, and the C99 standard only requires macros to allow up to
 * 127 arguments, so we can't portably go higher.  The implementation is
 * pretty trivial: VA_ARGS_NARGS_() returns its 64th argument, and we set up
 * the call so that that is the appropriate one of the list of constants.
 * This idea is due to Laurent Deniau.
 */
#define VA_ARGS_NARGS(...) \
	VA_ARGS_NARGS_(__VA_ARGS__, \
				   63,62,61,60,                   \
				   59,58,57,56,55,54,53,52,51,50, \
				   49,48,47,46,45,44,43,42,41,40, \
				   39,38,37,36,35,34,33,32,31,30, \
				   29,28,27,26,25,24,23,22,21,20, \
				   19,18,17,16,15,14,13,12,11,10, \
				   9, 8, 7, 6, 5, 4, 3, 2, 1, 0)
#define VA_ARGS_NARGS_( \
	_01,_02,_03,_04,_05,_06,_07,_08,_09,_10, \
	_11,_12,_13,_14,_15,_16,_17,_18,_19,_20, \
	_21,_22,_23,_24,_25,_26,_27,_28,_29,_30, \
	_31,_32,_33,_34,_35,_36,_37,_38,_39,_40, \
	_41,_42,_43,_44,_45,_46,_47,_48,_49,_50, \
	_51,_52,_53,_54,_55,_56,_57,_58,_59,_60, \
	_61,_62,_63,  N, ...) \
	(N)

/*
 * dummyret is used to set return values in macros that use ?: to make
 * assignments.  gcc wants these to be void, other compilers like char
 */
#ifdef __GNUC__					/* GNU cc */
#define dummyret	void
#else
#define dummyret	char
#endif

/*
 * Generic function pointer.  This can be used in the rare cases where it's
 * necessary to cast a function pointer to a seemingly incompatible function
 * pointer type while avoiding gcc's -Wcast-function-type warnings.
 */
typedef void (*pg_funcptr_t) (void);

/*
 * We require C99, hence the compiler should understand flexible array
 * members.  However, for documentation purposes we still consider it to be
 * project style to write "field[FLEXIBLE_ARRAY_MEMBER]" not just "field[]".
 * When computing the size of such an object, use "offsetof(struct s, f)"
 * for portability.  Don't use "offsetof(struct s, f[0])", as this doesn't
 * work with MSVC and with C++ compilers.
 */
#define FLEXIBLE_ARRAY_MEMBER	/* empty */

/* Which __func__ symbol do we have, if any? */
#ifdef HAVE_FUNCNAME__FUNC
#define PG_FUNCNAME_MACRO	__func__
#else
#ifdef HAVE_FUNCNAME__FUNCTION
#define PG_FUNCNAME_MACRO	__FUNCTION__
#else
#define PG_FUNCNAME_MACRO	NULL
#endif
#endif

/*
 * Does the compiler support #pragma GCC system_header? We optionally use it
 * to avoid warnings that we can't fix (e.g. in the perl headers).
 * See https://gcc.gnu.org/onlinedocs/cpp/System-Headers.html
 *
 * Headers for which we do not want to show compiler warnings can,
 * conditionally, use #pragma GCC system_header to avoid warnings. Obviously
 * this should only be used for external headers over which we do not have
 * control.
 *
 * Support for the pragma is tested here, instead of during configure, as gcc
 * also warns about the pragma being used in a .c file. It's surprisingly hard
 * to get autoconf to use .h as the file-ending. Looks like gcc has
 * implemented the pragma since the 2000, so this test should suffice.
 *
 *
 * Alternatively, we could add the include paths for problematic headers with
 * -isystem, but that is a larger hammer and is harder to search for.
 *
 * A more granular alternative would be to use #pragma GCC diagnostic
 * push/ignored/pop, but gcc warns about unknown warnings being ignored, so
 * every to-be-ignored-temporarily compiler warning would require its own
 * pg_config.h symbol and #ifdef.
 */
#ifdef __GNUC__
#define HAVE_PRAGMA_GCC_SYSTEM_HEADER	1
#endif


/* ----------------------------------------------------------------
 *				Section 2:	bool, true, false
 * ----------------------------------------------------------------
 */

/*
 * bool
 *		Boolean value, either true or false.
 *
 * We use stdbool.h if available and its bool has size 1.  That's useful for
 * better compiler and debugger output and for compatibility with third-party
 * libraries.  But PostgreSQL currently cannot deal with bool of other sizes;
 * there are static assertions around the code to prevent that.
 *
 * For C++ compilers, we assume the compiler has a compatible built-in
 * definition of bool.
 *
 * See also the version of this code in src/interfaces/ecpg/include/ecpglib.h.
 */

#ifndef __cplusplus

#ifdef PG_USE_STDBOOL
#include <stdbool.h>
#else

#ifndef bool
typedef unsigned char bool;
#endif

#ifndef true
#define true	((bool) 1)
#endif

#ifndef false
#define false	((bool) 0)
#endif

#endif							/* not PG_USE_STDBOOL */
#endif							/* not C++ */


/* ----------------------------------------------------------------
 *				Section 3:	standard system types
 * ----------------------------------------------------------------
 */

/*
 * Pointer
 *		Variable holding address of any memory resident object.
 *
 *		XXX Pointer arithmetic is done with this, so it can't be void *
 *		under "true" ANSI compilers.
 */
typedef char *Pointer;

/*
 * intN
 *		Signed integer, EXACTLY N BITS IN SIZE,
 *		used for numerical computations and the
 *		frontend/backend protocol.
 */
#ifndef HAVE_INT8
typedef signed char int8;		/* == 8 bits */
typedef signed short int16;		/* == 16 bits */
typedef signed int int32;		/* == 32 bits */
#endif							/* not HAVE_INT8 */

/*
 * uintN
 *		Unsigned integer, EXACTLY N BITS IN SIZE,
 *		used for numerical computations and the
 *		frontend/backend protocol.
 */
#ifndef HAVE_UINT8
typedef unsigned char uint8;	/* == 8 bits */
typedef unsigned short uint16;	/* == 16 bits */
typedef unsigned int uint32;	/* == 32 bits */
#endif							/* not HAVE_UINT8 */

/*
 * bitsN
 *		Unit of bitwise operation, AT LEAST N BITS IN SIZE.
 */
typedef uint8 bits8;			/* >= 8 bits */
typedef uint16 bits16;			/* >= 16 bits */
typedef uint32 bits32;			/* >= 32 bits */

/*
 * 64-bit integers
 */
#ifdef HAVE_LONG_INT_64
/* Plain "long int" fits, use it */

#ifndef HAVE_INT64
typedef long int int64;
#endif
#ifndef HAVE_UINT64
typedef unsigned long int uint64;
#endif
#define INT64CONST(x)  (x##L)
#define UINT64CONST(x) (x##UL)
#elif defined(HAVE_LONG_LONG_INT_64)
/* We have working support for "long long int", use that */

#ifndef HAVE_INT64
typedef long long int int64;
#endif
#ifndef HAVE_UINT64
typedef unsigned long long int uint64;
#endif
#define INT64CONST(x)  (x##LL)
#define UINT64CONST(x) (x##ULL)
#else
/* neither HAVE_LONG_INT_64 nor HAVE_LONG_LONG_INT_64 */
#error must have a working 64-bit integer datatype
#endif

/* snprintf format strings to use for 64-bit integers */
#define INT64_FORMAT "%" INT64_MODIFIER "d"
#define UINT64_FORMAT "%" INT64_MODIFIER "u"

/*
 * 128-bit signed and unsigned integers
 *		There currently is only limited support for such types.
 *		E.g. 128bit literals and snprintf are not supported; but math is.
 *		Also, because we exclude such types when choosing MAXIMUM_ALIGNOF,
 *		it must be possible to coerce the compiler to allocate them on no
 *		more than MAXALIGN boundaries.
 */
#if defined(PG_INT128_TYPE)
#if defined(pg_attribute_aligned) || ALIGNOF_PG_INT128_TYPE <= MAXIMUM_ALIGNOF
#define HAVE_INT128 1

typedef PG_INT128_TYPE int128
#if defined(pg_attribute_aligned)
			pg_attribute_aligned(MAXIMUM_ALIGNOF)
#endif
		   ;

typedef unsigned PG_INT128_TYPE uint128
#if defined(pg_attribute_aligned)
			pg_attribute_aligned(MAXIMUM_ALIGNOF)
#endif
		   ;

#endif
#endif

/*
 * stdint.h limits aren't guaranteed to have compatible types with our fixed
 * width types. So just define our own.
 */
#define PG_INT8_MIN		(-0x7F-1)
#define PG_INT8_MAX		(0x7F)
#define PG_UINT8_MAX	(0xFF)
#define PG_INT16_MIN	(-0x7FFF-1)
#define PG_INT16_MAX	(0x7FFF)
#define PG_UINT16_MAX	(0xFFFF)
#define PG_INT32_MIN	(-0x7FFFFFFF-1)
#define PG_INT32_MAX	(0x7FFFFFFF)
#define PG_UINT32_MAX	(0xFFFFFFFFU)
#define PG_INT64_MIN	(-INT64CONST(0x7FFFFFFFFFFFFFFF) - 1)
#define PG_INT64_MAX	INT64CONST(0x7FFFFFFFFFFFFFFF)
#define PG_UINT64_MAX	UINT64CONST(0xFFFFFFFFFFFFFFFF)

/*
 * We now always use int64 timestamps, but keep this symbol defined for the
 * benefit of external code that might test it.
 */
#define HAVE_INT64_TIMESTAMP

/*
 * Size
 *		Size of any memory resident object, as returned by sizeof.
 */
typedef size_t Size;

/*
 * Index
 *		Index into any memory resident array.
 *
 * Note:
 *		Indices are non negative.
 */
typedef unsigned int Index;

/*
 * Offset
 *		Offset into any memory resident array.
 *
 * Note:
 *		This differs from an Index in that an Index is always
 *		non negative, whereas Offset may be negative.
 */
typedef signed int Offset;

/*
 * Common Postgres datatype names (as used in the catalogs)
 */
typedef float float4;
typedef double float8;

#ifdef USE_FLOAT8_BYVAL
#define FLOAT8PASSBYVAL true
#else
#define FLOAT8PASSBYVAL false
#endif

/*
 * Oid, RegProcedure, TransactionId, SubTransactionId, MultiXactId,
 * CommandId
 */

/* typedef Oid is in postgres_ext.h */

/*
 * regproc is the type name used in the include/catalog headers, but
 * RegProcedure is the preferred name in C code.
 */
typedef Oid regproc;
typedef regproc RegProcedure;

typedef uint32 TransactionId;

typedef uint32 LocalTransactionId;

typedef uint32 SubTransactionId;

#define InvalidSubTransactionId		((SubTransactionId) 0)
#define TopSubTransactionId			((SubTransactionId) 1)

/* MultiXactId must be equivalent to TransactionId, to fit in t_xmax */
typedef TransactionId MultiXactId;

typedef uint32 MultiXactOffset;

typedef uint32 CommandId;

#define FirstCommandId	((CommandId) 0)
#define InvalidCommandId	(~(CommandId)0)


/* ----------------
 *		Variable-length datatypes all share the 'struct varlena' header.
 *
 * NOTE: for TOASTable types, this is an oversimplification, since the value
 * may be compressed or moved out-of-line.  However datatype-specific routines
 * are mostly content to deal with de-TOASTed values only, and of course
 * client-side routines should never see a TOASTed value.  But even in a
 * de-TOASTed value, beware of touching vl_len_ directly, as its
 * representation is no longer convenient.  It's recommended that code always
 * use macros VARDATA_ANY, VARSIZE_ANY, VARSIZE_ANY_EXHDR, VARDATA, VARSIZE,
 * and SET_VARSIZE instead of relying on direct mentions of the struct fields.
 * See postgres.h for details of the TOASTed form.
 * ----------------
 */
struct varlena
{
	char		vl_len_[4];		/* Do not touch this field directly! */
	char		vl_dat[FLEXIBLE_ARRAY_MEMBER];	/* Data content is here */
};

#define VARHDRSZ		((int32) sizeof(int32))

/*
 * These widely-used datatypes are just a varlena header and the data bytes.
 * There is no terminating null or anything like that --- the data length is
 * always VARSIZE_ANY_EXHDR(ptr).
 */
typedef struct varlena bytea;
typedef struct varlena text;
typedef struct varlena BpChar;	/* blank-padded char, ie SQL char(n) */
typedef struct varlena VarChar; /* var-length char, ie SQL varchar(n) */

/*
 * Specialized array types.  These are physically laid out just the same
 * as regular arrays (so that the regular array subscripting code works
 * with them).  They exist as distinct types mostly for historical reasons:
 * they have nonstandard I/O behavior which we don't want to change for fear
 * of breaking applications that look at the system catalogs.  There is also
 * an implementation issue for oidvector: it's part of the primary key for
 * pg_proc, and we can't use the normal btree array support routines for that
 * without circularity.
 */
typedef struct
{
	int32		vl_len_;		/* these fields must match ArrayType! */
	int			ndim;			/* always 1 for int2vector */
	int32		dataoffset;		/* always 0 for int2vector */
	Oid			elemtype;
	int			dim1;
	int			lbound1;
	int16		values[FLEXIBLE_ARRAY_MEMBER];
} int2vector;

typedef struct
{
	int32		vl_len_;		/* these fields must match ArrayType! */
	int			ndim;			/* always 1 for oidvector */
	int32		dataoffset;		/* always 0 for oidvector */
	Oid			elemtype;
	int			dim1;
	int			lbound1;
	Oid			values[FLEXIBLE_ARRAY_MEMBER];
} oidvector;

/*
 * Representation of a Name: effectively just a C string, but null-padded to
 * exactly NAMEDATALEN bytes.  The use of a struct is historical.
 */
typedef struct nameData
{
	char		data[NAMEDATALEN];
} NameData;
typedef NameData *Name;

#define NameStr(name)	((name).data)


/* ----------------------------------------------------------------
 *				Section 4:	IsValid macros for system types
 * ----------------------------------------------------------------
 */
/*
 * BoolIsValid
 *		True iff bool is valid.
 */
#define BoolIsValid(boolean)	((boolean) == false || (boolean) == true)

/*
 * PointerIsValid
 *		True iff pointer is valid.
 */
#define PointerIsValid(pointer) ((const void*)(pointer) != NULL)

/*
 * PointerIsAligned
 *		True iff pointer is properly aligned to point to the given type.
 */
#define PointerIsAligned(pointer, type) \
		(((uintptr_t)(pointer) % (sizeof (type))) == 0)

#define OffsetToPointer(base, offset) \
		((void *)((char *) base + offset))

#define OidIsValid(objectId)  ((bool) ((objectId) != InvalidOid))

#define RegProcedureIsValid(p)	OidIsValid(p)


/* ----------------------------------------------------------------
 *				Section 5:	offsetof, lengthof, alignment
 * ----------------------------------------------------------------
 */
/*
 * offsetof
 *		Offset of a structure/union field within that structure/union.
 *
 *		XXX This is supposed to be part of stddef.h, but isn't on
 *		some systems (like SunOS 4).
 */
#ifndef offsetof
#define offsetof(type, field)	((long) &((type *)0)->field)
#endif							/* offsetof */

/*
 * lengthof
 *		Number of elements in an array.
 */
#define lengthof(array) (sizeof (array) / sizeof ((array)[0]))

/* ----------------
 * Alignment macros: align a length or address appropriately for a given type.
 * The fooALIGN() macros round up to a multiple of the required alignment,
 * while the fooALIGN_DOWN() macros round down.  The latter are more useful
 * for problems like "how many X-sized structures will fit in a page?".
 *
 * NOTE: TYPEALIGN[_DOWN] will not work if ALIGNVAL is not a power of 2.
 * That case seems extremely unlikely to be needed in practice, however.
 *
 * NOTE: MAXIMUM_ALIGNOF, and hence MAXALIGN(), intentionally exclude any
 * larger-than-8-byte types the compiler might have.
 * ----------------
 */

#define TYPEALIGN(ALIGNVAL,LEN)  \
	(((uintptr_t) (LEN) + ((ALIGNVAL) - 1)) & ~((uintptr_t) ((ALIGNVAL) - 1)))

#define SHORTALIGN(LEN)			TYPEALIGN(ALIGNOF_SHORT, (LEN))
#define INTALIGN(LEN)			TYPEALIGN(ALIGNOF_INT, (LEN))
#define LONGALIGN(LEN)			TYPEALIGN(ALIGNOF_LONG, (LEN))
#define DOUBLEALIGN(LEN)		TYPEALIGN(ALIGNOF_DOUBLE, (LEN))
#define MAXALIGN(LEN)			TYPEALIGN(MAXIMUM_ALIGNOF, (LEN))
/* MAXALIGN covers only built-in types, not buffers */
#define BUFFERALIGN(LEN)		TYPEALIGN(ALIGNOF_BUFFER, (LEN))
#define CACHELINEALIGN(LEN)		TYPEALIGN(PG_CACHE_LINE_SIZE, (LEN))

#define TYPEALIGN_DOWN(ALIGNVAL,LEN)  \
	(((uintptr_t) (LEN)) & ~((uintptr_t) ((ALIGNVAL) - 1)))

#define SHORTALIGN_DOWN(LEN)	TYPEALIGN_DOWN(ALIGNOF_SHORT, (LEN))
#define INTALIGN_DOWN(LEN)		TYPEALIGN_DOWN(ALIGNOF_INT, (LEN))
#define LONGALIGN_DOWN(LEN)		TYPEALIGN_DOWN(ALIGNOF_LONG, (LEN))
#define DOUBLEALIGN_DOWN(LEN)	TYPEALIGN_DOWN(ALIGNOF_DOUBLE, (LEN))
#define MAXALIGN_DOWN(LEN)		TYPEALIGN_DOWN(MAXIMUM_ALIGNOF, (LEN))
#define BUFFERALIGN_DOWN(LEN)	TYPEALIGN_DOWN(ALIGNOF_BUFFER, (LEN))

/*
 * The above macros will not work with types wider than uintptr_t, like with
 * uint64 on 32-bit platforms.  That's not problem for the usual use where a
 * pointer or a length is aligned, but for the odd case that you need to
 * align something (potentially) wider, use TYPEALIGN64.
 */
#define TYPEALIGN64(ALIGNVAL,LEN)  \
	(((uint64) (LEN) + ((ALIGNVAL) - 1)) & ~((uint64) ((ALIGNVAL) - 1)))

/* we don't currently need wider versions of the other ALIGN macros */
#define MAXALIGN64(LEN)			TYPEALIGN64(MAXIMUM_ALIGNOF, (LEN))


/* ----------------------------------------------------------------
 *				Section 6:	assertions
 * ----------------------------------------------------------------
 */

/*
 * USE_ASSERT_CHECKING, if defined, turns on all the assertions.
 * - plai  9/5/90
 *
 * It should _NOT_ be defined in releases or in benchmark copies
 */

/*
 * Assert() can be used in both frontend and backend code. In frontend code it
 * just calls the standard assert, if it's available. If use of assertions is
 * not configured, it does nothing.
 */
#ifndef USE_ASSERT_CHECKING

#define Assert(condition)	((void)true)
#define AssertMacro(condition)	((void)true)
#define AssertArg(condition)	((void)true)
#define AssertState(condition)	((void)true)
#define AssertPointerAlignment(ptr, bndr)	((void)true)
#define Trap(condition, errorType)	((void)true)
#define TrapMacro(condition, errorType) (true)

#elif defined(FRONTEND)

#include <assert.h>
#define Assert(p) assert(p)
#define AssertMacro(p)	((void) assert(p))
#define AssertArg(condition) assert(condition)
#define AssertState(condition) assert(condition)
#define AssertPointerAlignment(ptr, bndr)	((void)true)

#else							/* USE_ASSERT_CHECKING && !FRONTEND */

/*
 * Trap
 *		Generates an exception if the given condition is true.
 */
#define Trap(condition, errorType) \
	do { \
		if (condition) \
			ExceptionalCondition(#condition, (errorType), \
								 __FILE__, __LINE__); \
	} while (0)

/*
 *	TrapMacro is the same as Trap but it's intended for use in macros:
 *
 *		#define foo(x) (AssertMacro(x != 0), bar(x))
 *
 *	Isn't CPP fun?
 */
#define TrapMacro(condition, errorType) \
	((bool) (! (condition) || \
			 (ExceptionalCondition(#condition, (errorType), \
								   __FILE__, __LINE__), 0)))

#define Assert(condition) \
	do { \
		if (!(condition)) \
			ExceptionalCondition(#condition, "FailedAssertion", \
								 __FILE__, __LINE__); \
	} while (0)

#define AssertMacro(condition) \
	((void) ((condition) || \
			 (ExceptionalCondition(#condition, "FailedAssertion", \
								   __FILE__, __LINE__), 0)))

#define AssertArg(condition) \
	do { \
		if (!(condition)) \
			ExceptionalCondition(#condition, "BadArgument", \
								 __FILE__, __LINE__); \
	} while (0)

#define AssertState(condition) \
	do { \
		if (!(condition)) \
			ExceptionalCondition(#condition, "BadState", \
								 __FILE__, __LINE__); \
	} while (0)

/*
 * Check that `ptr' is `bndr' aligned.
 */
#define AssertPointerAlignment(ptr, bndr) \
	Trap(TYPEALIGN(bndr, (uintptr_t)(ptr)) != (uintptr_t)(ptr), \
		 "UnalignedPointer")

#endif							/* USE_ASSERT_CHECKING && !FRONTEND */

/*
 * ExceptionalCondition is compiled into the backend whether or not
 * USE_ASSERT_CHECKING is defined, so as to support use of extensions
 * that are built with that #define with a backend that isn't.  Hence,
 * we should declare it as long as !FRONTEND.
 */
#ifndef FRONTEND
extern void ExceptionalCondition(const char *conditionName,
								 const char *errorType,
								 const char *fileName, int lineNumber) pg_attribute_noreturn();
#endif

/*
 * Macros to support compile-time assertion checks.
 *
 * If the "condition" (a compile-time-constant expression) evaluates to false,
 * throw a compile error using the "errmessage" (a string literal).
 *
 * gcc 4.6 and up supports _Static_assert(), but there are bizarre syntactic
 * placement restrictions.  Macros StaticAssertStmt() and StaticAssertExpr()
 * make it safe to use as a statement or in an expression, respectively.
 * The macro StaticAssertDecl() is suitable for use at file scope (outside of
 * any function).
 *
 * Otherwise we fall back on a kluge that assumes the compiler will complain
 * about a negative width for a struct bit-field.  This will not include a
 * helpful error message, but it beats not getting an error at all.
 */
#ifndef __cplusplus
#ifdef HAVE__STATIC_ASSERT
#define StaticAssertStmt(condition, errmessage) \
	do { _Static_assert(condition, errmessage); } while(0)
#define StaticAssertExpr(condition, errmessage) \
	((void) ({ StaticAssertStmt(condition, errmessage); true; }))
#define StaticAssertDecl(condition, errmessage) \
	_Static_assert(condition, errmessage)
#else							/* !HAVE__STATIC_ASSERT */
#define StaticAssertStmt(condition, errmessage) \
	((void) sizeof(struct { int static_assert_failure : (condition) ? 1 : -1; }))
#define StaticAssertExpr(condition, errmessage) \
	StaticAssertStmt(condition, errmessage)
#define StaticAssertDecl(condition, errmessage) \
	extern void static_assert_func(int static_assert_failure[(condition) ? 1 : -1])
#endif							/* HAVE__STATIC_ASSERT */
#else							/* C++ */
#if defined(__cpp_static_assert) && __cpp_static_assert >= 200410
#define StaticAssertStmt(condition, errmessage) \
	static_assert(condition, errmessage)
#define StaticAssertExpr(condition, errmessage) \
	({ static_assert(condition, errmessage); })
#define StaticAssertDecl(condition, errmessage) \
	static_assert(condition, errmessage)
#else							/* !__cpp_static_assert */
#define StaticAssertStmt(condition, errmessage) \
	do { struct static_assert_struct { int static_assert_failure : (condition) ? 1 : -1; }; } while(0)
#define StaticAssertExpr(condition, errmessage) \
	((void) ({ StaticAssertStmt(condition, errmessage); }))
#define StaticAssertDecl(condition, errmessage) \
	extern void static_assert_func(int static_assert_failure[(condition) ? 1 : -1])
#endif							/* __cpp_static_assert */
#endif							/* C++ */


/*
 * Compile-time checks that a variable (or expression) has the specified type.
 *
 * AssertVariableIsOfType() can be used as a statement.
 * AssertVariableIsOfTypeMacro() is intended for use in macros, eg
 *		#define foo(x) (AssertVariableIsOfTypeMacro(x, int), bar(x))
 *
 * If we don't have __builtin_types_compatible_p, we can still assert that
 * the types have the same size.  This is far from ideal (especially on 32-bit
 * platforms) but it provides at least some coverage.
 */
#ifdef HAVE__BUILTIN_TYPES_COMPATIBLE_P
#define AssertVariableIsOfType(varname, typename) \
	StaticAssertStmt(__builtin_types_compatible_p(__typeof__(varname), typename), \
	CppAsString(varname) " does not have type " CppAsString(typename))
#define AssertVariableIsOfTypeMacro(varname, typename) \
	(StaticAssertExpr(__builtin_types_compatible_p(__typeof__(varname), typename), \
	 CppAsString(varname) " does not have type " CppAsString(typename)))
#else							/* !HAVE__BUILTIN_TYPES_COMPATIBLE_P */
#define AssertVariableIsOfType(varname, typename) \
	StaticAssertStmt(sizeof(varname) == sizeof(typename), \
	CppAsString(varname) " does not have type " CppAsString(typename))
#define AssertVariableIsOfTypeMacro(varname, typename) \
	(StaticAssertExpr(sizeof(varname) == sizeof(typename), \
	 CppAsString(varname) " does not have type " CppAsString(typename)))
#endif							/* HAVE__BUILTIN_TYPES_COMPATIBLE_P */


/* ----------------------------------------------------------------
 *				Section 7:	widely useful macros
 * ----------------------------------------------------------------
 */
/*
 * Max
 *		Return the maximum of two numbers.
 */
#define Max(x, y)		((x) > (y) ? (x) : (y))

/*
 * Min
 *		Return the minimum of two numbers.
 */
#define Min(x, y)		((x) < (y) ? (x) : (y))

/*
 * Abs
 *		Return the absolute value of the argument.
 */
#define Abs(x)			((x) >= 0 ? (x) : -(x))


/* Get a bit mask of the bits set in non-long aligned addresses */
#define LONG_ALIGN_MASK (sizeof(long) - 1)

/*
 * MemSet
 *	Exactly the same as standard library function memset(), but considerably
 *	faster for zeroing small word-aligned structures (such as parsetree nodes).
 *	This has to be a macro because the main point is to avoid function-call
 *	overhead.   However, we have also found that the loop is faster than
 *	native libc memset() on some platforms, even those with assembler
 *	memset() functions.  More research needs to be done, perhaps with
 *	MEMSET_LOOP_LIMIT tests in configure.
 */
#define MemSet(start, val, len) \
	do \
	{ \
		/* must be void* because we don't know if it is integer aligned yet */ \
		void   *_vstart = (void *) (start); \
		int		_val = (val); \
		Size	_len = (len); \
\
		if ((((uintptr_t) _vstart) & LONG_ALIGN_MASK) == 0 && \
			(_len & LONG_ALIGN_MASK) == 0 && \
			_val == 0 && \
			_len <= MEMSET_LOOP_LIMIT && \
			/* \
			 *	If MEMSET_LOOP_LIMIT == 0, optimizer should find \
			 *	the whole "if" false at compile time. \
			 */ \
			MEMSET_LOOP_LIMIT != 0) \
		{ \
			long *_start = (long *) _vstart; \
			long *_stop = (long *) ((char *) _start + _len); \
			while (_start < _stop) \
				*_start++ = 0; \
		} \
		else \
			memset(_vstart, _val, _len); \
	} while (0)

/*
 * MemSetAligned is the same as MemSet except it omits the test to see if
 * "start" is word-aligned.  This is okay to use if the caller knows a-priori
 * that the pointer is suitably aligned (typically, because he just got it
 * from palloc(), which always delivers a max-aligned pointer).
 */
#define MemSetAligned(start, val, len) \
	do \
	{ \
		long   *_start = (long *) (start); \
		int		_val = (val); \
		Size	_len = (len); \
\
		if ((_len & LONG_ALIGN_MASK) == 0 && \
			_val == 0 && \
			_len <= MEMSET_LOOP_LIMIT && \
			MEMSET_LOOP_LIMIT != 0) \
		{ \
			long *_stop = (long *) ((char *) _start + _len); \
			while (_start < _stop) \
				*_start++ = 0; \
		} \
		else \
			memset(_start, _val, _len); \
	} while (0)


/*
 * MemSetTest/MemSetLoop are a variant version that allow all the tests in
 * MemSet to be done at compile time in cases where "val" and "len" are
 * constants *and* we know the "start" pointer must be word-aligned.
 * If MemSetTest succeeds, then it is okay to use MemSetLoop, otherwise use
 * MemSetAligned.  Beware of multiple evaluations of the arguments when using
 * this approach.
 */
#define MemSetTest(val, len) \
	( ((len) & LONG_ALIGN_MASK) == 0 && \
	(len) <= MEMSET_LOOP_LIMIT && \
	MEMSET_LOOP_LIMIT != 0 && \
	(val) == 0 )

#define MemSetLoop(start, val, len) \
	do \
	{ \
		long * _start = (long *) (start); \
		long * _stop = (long *) ((char *) _start + (Size) (len)); \
	\
		while (_start < _stop) \
			*_start++ = 0; \
	} while (0)

/*
 * Macros for range-checking float values before converting to integer.
 * We must be careful here that the boundary values are expressed exactly
 * in the float domain.  PG_INTnn_MIN is an exact power of 2, so it will
 * be represented exactly; but PG_INTnn_MAX isn't, and might get rounded
 * off, so avoid using that.
 * The input must be rounded to an integer beforehand, typically with rint(),
 * else we might draw the wrong conclusion about close-to-the-limit values.
 * These macros will do the right thing for Inf, but not necessarily for NaN,
 * so check isnan(num) first if that's a possibility.
 */
#define FLOAT4_FITS_IN_INT16(num) \
	((num) >= (float4) PG_INT16_MIN && (num) < -((float4) PG_INT16_MIN))
#define FLOAT4_FITS_IN_INT32(num) \
	((num) >= (float4) PG_INT32_MIN && (num) < -((float4) PG_INT32_MIN))
#define FLOAT4_FITS_IN_INT64(num) \
	((num) >= (float4) PG_INT64_MIN && (num) < -((float4) PG_INT64_MIN))
#define FLOAT8_FITS_IN_INT16(num) \
	((num) >= (float8) PG_INT16_MIN && (num) < -((float8) PG_INT16_MIN))
#define FLOAT8_FITS_IN_INT32(num) \
	((num) >= (float8) PG_INT32_MIN && (num) < -((float8) PG_INT32_MIN))
#define FLOAT8_FITS_IN_INT64(num) \
	((num) >= (float8) PG_INT64_MIN && (num) < -((float8) PG_INT64_MIN))


/* ----------------------------------------------------------------
 *				Section 8:	random stuff
 * ----------------------------------------------------------------
 */

#ifdef HAVE_STRUCT_SOCKADDR_UN
#define HAVE_UNIX_SOCKETS 1
#endif

/*
 * Invert the sign of a qsort-style comparison result, ie, exchange negative
 * and positive integer values, being careful not to get the wrong answer
 * for INT_MIN.  The argument should be an integral variable.
 */
#define INVERT_COMPARE_RESULT(var) \
	((var) = ((var) < 0) ? 1 : -(var))

/*
 * Use this, not "char buf[BLCKSZ]", to declare a field or local variable
 * holding a page buffer, if that page might be accessed as a page and not
 * just a string of bytes.  Otherwise the variable might be under-aligned,
 * causing problems on alignment-picky hardware.  (In some places, we use
 * this to declare buffers even though we only pass them to read() and
 * write(), because copying to/from aligned buffers is usually faster than
 * using unaligned buffers.)  We include both "double" and "int64" in the
 * union to ensure that the compiler knows the value must be MAXALIGN'ed
 * (cf. configure's computation of MAXIMUM_ALIGNOF).
 */
typedef union PGAlignedBlock
{
	char		data[BLCKSZ];
	double		force_align_d;
	int64		force_align_i64;
} PGAlignedBlock;

/* Same, but for an XLOG_BLCKSZ-sized buffer */
typedef union PGAlignedXLogBlock
{
	char		data[XLOG_BLCKSZ];
	double		force_align_d;
	int64		force_align_i64;
} PGAlignedXLogBlock;

/* msb for char */
#define HIGHBIT					(0x80)
#define IS_HIGHBIT_SET(ch)		((unsigned char)(ch) & HIGHBIT)

/*
 * Support macros for escaping strings.  escape_backslash should be true
 * if generating a non-standard-conforming string.  Prefixing a string
 * with ESCAPE_STRING_SYNTAX guarantees it is non-standard-conforming.
 * Beware of multiple evaluation of the "ch" argument!
 */
#define SQL_STR_DOUBLE(ch, escape_backslash)	\
	((ch) == '\'' || ((ch) == '\\' && (escape_backslash)))

#define ESCAPE_STRING_SYNTAX	'E'


#define STATUS_OK				(0)
#define STATUS_ERROR			(-1)
#define STATUS_EOF				(-2)

/*
 * gettext support
 */

#ifndef ENABLE_NLS
/* stuff we'd otherwise get from <libintl.h> */
#define gettext(x) (x)
#define dgettext(d,x) (x)
#define ngettext(s,p,n) ((n) == 1 ? (s) : (p))
#define dngettext(d,s,p,n) ((n) == 1 ? (s) : (p))
#endif

#define _(x) gettext(x)

/*
 *	Use this to mark string constants as needing translation at some later
 *	time, rather than immediately.  This is useful for cases where you need
 *	access to the original string and translated string, and for cases where
 *	immediate translation is not possible, like when initializing global
 *	variables.
 *
 *	https://www.gnu.org/software/gettext/manual/html_node/Special-cases.html
 */
#define gettext_noop(x) (x)

/*
 * To better support parallel installations of major PostgreSQL
 * versions as well as parallel installations of major library soname
 * versions, we mangle the gettext domain name by appending those
 * version numbers.  The coding rule ought to be that wherever the
 * domain name is mentioned as a literal, it must be wrapped into
 * PG_TEXTDOMAIN().  The macros below do not work on non-literals; but
 * that is somewhat intentional because it avoids having to worry
 * about multiple states of premangling and postmangling as the values
 * are being passed around.
 *
 * Make sure this matches the installation rules in nls-global.mk.
 */
#ifdef SO_MAJOR_VERSION
#define PG_TEXTDOMAIN(domain) (domain CppAsString2(SO_MAJOR_VERSION) "-" PG_MAJORVERSION)
#else
#define PG_TEXTDOMAIN(domain) (domain "-" PG_MAJORVERSION)
#endif

/*
 * Macro that allows to cast constness and volatile away from an expression, but doesn't
 * allow changing the underlying type.  Enforcement of the latter
 * currently only works for gcc like compilers.
 *
 * Please note IT IS NOT SAFE to cast constness away if the result will ever
 * be modified (it would be undefined behaviour). Doing so anyway can cause
 * compiler misoptimizations or runtime crashes (modifying readonly memory).
 * It is only safe to use when the result will not be modified, but API
 * design or language restrictions prevent you from declaring that
 * (e.g. because a function returns both const and non-const variables).
 *
 * Note that this only works in function scope, not for global variables (it'd
 * be nice, but not trivial, to improve that).
 */
#if defined(HAVE__BUILTIN_TYPES_COMPATIBLE_P)
#define unconstify(underlying_type, expr) \
	(StaticAssertExpr(__builtin_types_compatible_p(__typeof(expr), const underlying_type), \
					  "wrong cast"), \
	 (underlying_type) (expr))
#define unvolatize(underlying_type, expr) \
	(StaticAssertExpr(__builtin_types_compatible_p(__typeof(expr), volatile underlying_type), \
					  "wrong cast"), \
	 (underlying_type) (expr))
#else
#define unconstify(underlying_type, expr) \
	((underlying_type) (expr))
#define unvolatize(underlying_type, expr) \
	((underlying_type) (expr))
#endif

/* ----------------------------------------------------------------
 *				Section 9: system-specific hacks
 *
 *		This should be limited to things that absolutely have to be
 *		included in every source file.  The port-specific header file
 *		is usually a better place for this sort of thing.
 * ----------------------------------------------------------------
 */

/*
 *	NOTE:  this is also used for opening text files.
 *	WIN32 treats Control-Z as EOF in files opened in text mode.
 *	Therefore, we open files in binary mode on Win32 so we can read
 *	literal control-Z.  The other affect is that we see CRLF, but
 *	that is OK because we can already handle those cleanly.
 */
#if defined(WIN32) || defined(__CYGWIN__)
#define PG_BINARY	O_BINARY
#define PG_BINARY_A "ab"
#define PG_BINARY_R "rb"
#define PG_BINARY_W "wb"
#else
#define PG_BINARY	0
#define PG_BINARY_A "a"
#define PG_BINARY_R "r"
#define PG_BINARY_W "w"
#endif

/*
 * Provide prototypes for routines not present in a particular machine's
 * standard C library.
 */

#if defined(HAVE_FDATASYNC) && !HAVE_DECL_FDATASYNC
extern int	fdatasync(int fildes);
#endif

/* Older platforms may provide strto[u]ll functionality under other names */
#if !defined(HAVE_STRTOLL) && defined(HAVE___STRTOLL)
#define strtoll __strtoll
#define HAVE_STRTOLL 1
#endif

#if !defined(HAVE_STRTOLL) && defined(HAVE_STRTOQ)
#define strtoll strtoq
#define HAVE_STRTOLL 1
#endif

#if !defined(HAVE_STRTOULL) && defined(HAVE___STRTOULL)
#define strtoull __strtoull
#define HAVE_STRTOULL 1
#endif

#if !defined(HAVE_STRTOULL) && defined(HAVE_STRTOUQ)
#define strtoull strtouq
#define HAVE_STRTOULL 1
#endif

#if defined(HAVE_STRTOLL) && !HAVE_DECL_STRTOLL
extern long long strtoll(const char *str, char **endptr, int base);
#endif

#if defined(HAVE_STRTOULL) && !HAVE_DECL_STRTOULL
extern unsigned long long strtoull(const char *str, char **endptr, int base);
#endif

/*
 * Thin wrappers that convert strings to exactly 64-bit integers, matching our
 * definition of int64.  (For the naming, compare that POSIX has
 * strtoimax()/strtoumax() which return intmax_t/uintmax_t.)
 */
#ifdef HAVE_LONG_INT_64
#define strtoi64(str, endptr, base) ((int64) strtol(str, endptr, base))
#define strtou64(str, endptr, base) ((uint64) strtoul(str, endptr, base))
#else
#define strtoi64(str, endptr, base) ((int64) strtoll(str, endptr, base))
#define strtou64(str, endptr, base) ((uint64) strtoull(str, endptr, base))
#endif

/*
 * Use "extern PGDLLIMPORT ..." to declare variables that are defined
 * in the core backend and need to be accessible by loadable modules.
 * No special marking is required on most ports.
 */
#ifndef PGDLLIMPORT
#define PGDLLIMPORT
#endif

/*
 * Use "extern PGDLLEXPORT ..." to declare functions that are defined in
 * loadable modules and need to be callable by the core backend.  (Usually,
 * this is not necessary because our build process automatically exports
 * such symbols, but sometimes manual marking is required.)
 * No special marking is required on most ports.
 */
#ifndef PGDLLEXPORT
#define PGDLLEXPORT
#endif

/*
 * The following is used as the arg list for signal handlers.  Any ports
 * that take something other than an int argument should override this in
 * their pg_config_os.h file.  Note that variable names are required
 * because it is used in both the prototypes as well as the definitions.
 * Note also the long name.  We expect that this won't collide with
 * other names causing compiler warnings.
 */

#ifndef SIGNAL_ARGS
#define SIGNAL_ARGS  int postgres_signal_arg
#endif

/*
 * When there is no sigsetjmp, its functionality is provided by plain
 * setjmp.  We now support the case only on Windows.  However, it seems
 * that MinGW-64 has some longstanding issues in its setjmp support,
 * so on that toolchain we cheat and use gcc's builtins.
 */
#ifdef WIN32
#ifdef __MINGW64__
typedef intptr_t sigjmp_buf[5];
#define sigsetjmp(x,y) __builtin_setjmp(x)
#define siglongjmp __builtin_longjmp
#else							/* !__MINGW64__ */
#define sigjmp_buf jmp_buf
#define sigsetjmp(x,y) setjmp(x)
#define siglongjmp longjmp
#endif							/* __MINGW64__ */
#endif							/* WIN32 */

/* EXEC_BACKEND defines */
#ifdef EXEC_BACKEND
#define NON_EXEC_STATIC
#else
#define NON_EXEC_STATIC static
#endif

/* /port compatibility functions */
/*-------------------------------------------------------------------------
 *
 * port.h
 *	  Header for src/port/ compatibility functions.
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/port.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_PORT_H
#define PG_PORT_H

#include <ctype.h>

/*
 * Windows has enough specialized port stuff that we push most of it off
 * into another file.
 * Note: Some CYGWIN includes might #define WIN32.
 */
#if defined(WIN32) && !defined(__CYGWIN__)
// #include "port/win32_port.h"
#endif

/* socket has a different definition on WIN32 */
#ifndef WIN32
typedef int pgsocket;

#define PGINVALID_SOCKET (-1)
#else
typedef SOCKET pgsocket;

#define PGINVALID_SOCKET INVALID_SOCKET
#endif

/* if platform lacks socklen_t, we assume this will work */
#ifndef HAVE_SOCKLEN_T
typedef unsigned int socklen_t;
#endif

/* non-blocking */
extern bool pg_set_noblock(pgsocket sock);
extern bool pg_set_block(pgsocket sock);

/* Portable path handling for Unix/Win32 (in path.c) */

extern bool has_drive_prefix(const char *filename);
extern char *first_dir_separator(const char *filename);
extern char *last_dir_separator(const char *filename);
extern char *first_path_var_separator(const char *pathlist);
extern void join_path_components(char *ret_path,
								 const char *head, const char *tail);
extern void canonicalize_path(char *path);
extern void make_native_path(char *path);
extern void cleanup_path(char *path);
extern bool path_contains_parent_reference(const char *path);
extern bool path_is_relative_and_below_cwd(const char *path);
extern bool path_is_prefix_of_path(const char *path1, const char *path2);
extern char *make_absolute_path(const char *path);
extern const char *get_progname(const char *argv0);
extern void get_share_path(const char *my_exec_path, char *ret_path);
extern void get_etc_path(const char *my_exec_path, char *ret_path);
extern void get_include_path(const char *my_exec_path, char *ret_path);
extern void get_pkginclude_path(const char *my_exec_path, char *ret_path);
extern void get_includeserver_path(const char *my_exec_path, char *ret_path);
extern void get_lib_path(const char *my_exec_path, char *ret_path);
extern void get_pkglib_path(const char *my_exec_path, char *ret_path);
extern void get_locale_path(const char *my_exec_path, char *ret_path);
extern void get_doc_path(const char *my_exec_path, char *ret_path);
extern void get_html_path(const char *my_exec_path, char *ret_path);
extern void get_man_path(const char *my_exec_path, char *ret_path);
extern bool get_home_path(char *ret_path);
extern void get_parent_directory(char *path);

/* common/pgfnames.c */
extern char **pgfnames(const char *path);
extern void pgfnames_cleanup(char **filenames);

#define IS_NONWINDOWS_DIR_SEP(ch)	((ch) == '/')
#define is_nonwindows_absolute_path(filename) \
( \
	IS_NONWINDOWS_DIR_SEP((filename)[0]) \
)

#define IS_WINDOWS_DIR_SEP(ch)	((ch) == '/' || (ch) == '\\')
/* See path_is_relative_and_below_cwd() for how we handle 'E:abc'. */
#define is_windows_absolute_path(filename) \
( \
	IS_WINDOWS_DIR_SEP((filename)[0]) || \
	(isalpha((unsigned char) ((filename)[0])) && (filename)[1] == ':' && \
	 IS_WINDOWS_DIR_SEP((filename)[2])) \
)

/*
 *	is_absolute_path and IS_DIR_SEP
 *
 *	By using macros here we avoid needing to include path.c in libpq.
 */
#ifndef WIN32
#define IS_DIR_SEP(ch) IS_NONWINDOWS_DIR_SEP(ch)
#define is_absolute_path(filename) is_nonwindows_absolute_path(filename)
#else
#define IS_DIR_SEP(ch) IS_WINDOWS_DIR_SEP(ch)
#define is_absolute_path(filename) is_windows_absolute_path(filename)
#endif

/*
 * This macro provides a centralized list of all errnos that identify
 * hard failure of a previously-established network connection.
 * The macro is intended to be used in a switch statement, in the form
 * "case ALL_CONNECTION_FAILURE_ERRNOS:".
 *
 * Note: this groups EPIPE and ECONNRESET, which we take to indicate a
 * probable server crash, with other errors that indicate loss of network
 * connectivity without proving much about the server's state.  Places that
 * are actually reporting errors typically single out EPIPE and ECONNRESET,
 * while allowing the network failures to be reported generically.
 */
#define ALL_CONNECTION_FAILURE_ERRNOS \
	EPIPE: \
	case ECONNRESET: \
	case ECONNABORTED: \
	case EHOSTDOWN: \
	case EHOSTUNREACH: \
	case ENETDOWN: \
	case ENETRESET: \
	case ENETUNREACH: \
	case ETIMEDOUT

/* Portable locale initialization (in exec.c) */
extern void set_pglocale_pgservice(const char *argv0, const char *app);

/* Portable way to find and execute binaries (in exec.c) */
extern int	validate_exec(const char *path);
extern int	find_my_exec(const char *argv0, char *retpath);
extern int	find_other_exec(const char *argv0, const char *target,
							const char *versionstr, char *retpath);
extern char *pipe_read_line(char *cmd, char *line, int maxsize);

/* Doesn't belong here, but this is used with find_other_exec(), so... */
#define PG_BACKEND_VERSIONSTR "postgres (PostgreSQL) " PG_VERSION "\n"

#ifdef EXEC_BACKEND
/* Disable ASLR before exec, for developer builds only (in exec.c) */
extern int	pg_disable_aslr(void);
#endif


#if defined(WIN32) || defined(__CYGWIN__)
#define EXE ".exe"
#else
#define EXE ""
#endif

#if defined(WIN32) && !defined(__CYGWIN__)
#define DEVNULL "nul"
#else
#define DEVNULL "/dev/null"
#endif

/* Portable delay handling */
extern void pg_usleep(long microsec);

/* Portable SQL-like case-independent comparisons and conversions */
extern int	pg_strcasecmp(const char *s1, const char *s2);
extern int	pg_strncasecmp(const char *s1, const char *s2, size_t n);
extern unsigned char pg_toupper(unsigned char ch);
extern unsigned char pg_tolower(unsigned char ch);
extern unsigned char pg_ascii_toupper(unsigned char ch);
extern unsigned char pg_ascii_tolower(unsigned char ch);

/*
 * Beginning in v12, we always replace snprintf() and friends with our own
 * implementation.  This symbol is no longer consulted by the core code,
 * but keep it defined anyway in case any extensions are looking at it.
 */
#define USE_REPL_SNPRINTF 1

/*
 * Versions of libintl >= 0.13 try to replace printf() and friends with
 * macros to their own versions that understand the %$ format.  We do the
 * same, so disable their macros, if they exist.
 */
#ifdef vsnprintf
#undef vsnprintf
#endif
#ifdef snprintf
#undef snprintf
#endif
#ifdef vsprintf
#undef vsprintf
#endif
#ifdef sprintf
#undef sprintf
#endif
#ifdef vfprintf
#undef vfprintf
#endif
#ifdef fprintf
#undef fprintf
#endif
#ifdef vprintf
#undef vprintf
#endif
#ifdef printf
#undef printf
#endif

extern int	pg_vsnprintf(char *str, size_t count, const char *fmt, va_list args);
extern int	pg_snprintf(char *str, size_t count, const char *fmt,...) pg_attribute_printf(3, 4);
extern int	pg_vsprintf(char *str, const char *fmt, va_list args);
extern int	pg_sprintf(char *str, const char *fmt,...) pg_attribute_printf(2, 3);
extern int	pg_vfprintf(FILE *stream, const char *fmt, va_list args);
extern int	pg_fprintf(FILE *stream, const char *fmt,...) pg_attribute_printf(2, 3);
extern int	pg_vprintf(const char *fmt, va_list args);
extern int	pg_printf(const char *fmt,...) pg_attribute_printf(1, 2);

/*
 * We use __VA_ARGS__ for printf to prevent replacing references to
 * the "printf" format archetype in format() attribute declarations.
 * That unfortunately means that taking a function pointer to printf
 * will not do what we'd wish.  (If you need to do that, you must name
 * pg_printf explicitly.)  For printf's sibling functions, use
 * parameterless macros so that function pointers will work unsurprisingly.
 */
#define vsnprintf		pg_vsnprintf
#define snprintf		pg_snprintf
#define vsprintf		pg_vsprintf
#define sprintf			pg_sprintf
#define vfprintf		pg_vfprintf
#define fprintf			pg_fprintf
#define vprintf			pg_vprintf
#define printf(...)		pg_printf(__VA_ARGS__)

/* This is also provided by snprintf.c */
extern int	pg_strfromd(char *str, size_t count, int precision, double value);

/* Replace strerror() with our own, somewhat more robust wrapper */
extern char *pg_strerror(int errnum);
#define strerror pg_strerror

/* Likewise for strerror_r(); note we prefer the GNU API for that */
extern char *pg_strerror_r(int errnum, char *buf, size_t buflen);
#define strerror_r pg_strerror_r
#define PG_STRERROR_R_BUFLEN 256	/* Recommended buffer size for strerror_r */

/* Wrap strsignal(), or provide our own version if necessary */
extern const char *pg_strsignal(int signum);

extern int	pclose_check(FILE *stream);

/* Global variable holding time zone information. */
#if defined(WIN32) || defined(__CYGWIN__)
#define TIMEZONE_GLOBAL _timezone
#define TZNAME_GLOBAL _tzname
#else
#define TIMEZONE_GLOBAL timezone
#define TZNAME_GLOBAL tzname
#endif

#if defined(WIN32) || defined(__CYGWIN__)
/*
 *	Win32 doesn't have reliable rename/unlink during concurrent access.
 */
extern int	pgrename(const char *from, const char *to);
extern int	pgunlink(const char *path);

/* Include this first so later includes don't see these defines */
#ifdef _MSC_VER
#include <io.h>
#endif

#define rename(from, to)		pgrename(from, to)
#define unlink(path)			pgunlink(path)
#endif							/* defined(WIN32) || defined(__CYGWIN__) */

/*
 *	Win32 also doesn't have symlinks, but we can emulate them with
 *	junction points on newer Win32 versions.
 *
 *	Cygwin has its own symlinks which work on Win95/98/ME where
 *	junction points don't, so use those instead.  We have no way of
 *	knowing what type of system Cygwin binaries will be run on.
 *		Note: Some CYGWIN includes might #define WIN32.
 */
#if defined(WIN32) && !defined(__CYGWIN__)
extern int	pgsymlink(const char *oldpath, const char *newpath);
extern int	pgreadlink(const char *path, char *buf, size_t size);
extern bool pgwin32_is_junction(const char *path);

#define symlink(oldpath, newpath)	pgsymlink(oldpath, newpath)
#define readlink(path, buf, size)	pgreadlink(path, buf, size)
#endif

extern bool rmtree(const char *path, bool rmtopdir);

#if defined(WIN32) && !defined(__CYGWIN__)

/*
 * open() and fopen() replacements to allow deletion of open files and
 * passing of other special options.
 */
#define		O_DIRECT	0x80000000
extern HANDLE pgwin32_open_handle(const char *, int, bool);
extern int	pgwin32_open(const char *, int,...);
extern FILE *pgwin32_fopen(const char *, const char *);
#define		open(a,b,c) pgwin32_open(a,b,c)
#define		fopen(a,b) pgwin32_fopen(a,b)

/*
 * Mingw-w64 headers #define popen and pclose to _popen and _pclose.  We want
 * to use our popen wrapper, rather than plain _popen, so override that.  For
 * consistency, use our version of pclose, too.
 */
#ifdef popen
#undef popen
#endif
#ifdef pclose
#undef pclose
#endif

/*
 * system() and popen() replacements to enclose the command in an extra
 * pair of quotes.
 */
extern int	pgwin32_system(const char *command);
extern FILE *pgwin32_popen(const char *command, const char *type);

#define system(a) pgwin32_system(a)
#define popen(a,b) pgwin32_popen(a,b)
#define pclose(a) _pclose(a)

/* New versions of MingW have gettimeofday, old mingw and msvc don't */
#ifndef HAVE_GETTIMEOFDAY
/* Last parameter not used */
extern int	gettimeofday(struct timeval *tp, struct timezone *tzp);
#endif
#else							/* !WIN32 */

/*
 *	Win32 requires a special close for sockets and pipes, while on Unix
 *	close() does them all.
 */
#define closesocket close
#endif							/* WIN32 */

/*
 * On Windows, setvbuf() does not support _IOLBF mode, and interprets that
 * as _IOFBF.  To add insult to injury, setvbuf(file, NULL, _IOFBF, 0)
 * crashes outright if "parameter validation" is enabled.  Therefore, in
 * places where we'd like to select line-buffered mode, we fall back to
 * unbuffered mode instead on Windows.  Always use PG_IOLBF not _IOLBF
 * directly in order to implement this behavior.
 */
#ifndef WIN32
#define PG_IOLBF	_IOLBF
#else
#define PG_IOLBF	_IONBF
#endif

/*
 * Default "extern" declarations or macro substitutes for library routines.
 * When necessary, these routines are provided by files in src/port/.
 */

/* Type to use with fseeko/ftello */
#ifndef WIN32					/* WIN32 is handled in port/win32_port.h */
#define pgoff_t off_t
#endif

#ifndef HAVE_FLS
extern int	fls(int mask);
#endif

#ifndef HAVE_GETPEEREID
/* On Windows, Perl might have incompatible definitions of uid_t and gid_t. */
#ifndef PLPERL_HAVE_UID_GID
extern int	getpeereid(int sock, uid_t *uid, gid_t *gid);
#endif
#endif

/*
 * Glibc doesn't use the builtin for clang due to a *gcc* bug in a version
 * newer than the gcc compatibility clang claims to have. This would cause a
 * *lot* of superfluous function calls, therefore revert when using clang. In
 * C++ there's issues with libc++ (not libstdc++), so disable as well.
 */
#if defined(__clang__) && !defined(__cplusplus)
/* needs to be separate to not confuse other compilers */
#if __has_builtin(__builtin_isinf)
/* need to include before, to avoid getting overwritten */
#include <math.h>
#undef isinf
#define isinf __builtin_isinf
#endif							/* __has_builtin(isinf) */
#endif							/* __clang__ && !__cplusplus */

#ifndef HAVE_EXPLICIT_BZERO
extern void explicit_bzero(void *buf, size_t len);
#endif

#ifndef HAVE_STRTOF
extern float strtof(const char *nptr, char **endptr);
#endif

#ifdef HAVE_BUGGY_STRTOF
extern float pg_strtof(const char *nptr, char **endptr);
#define strtof(a,b) (pg_strtof((a),(b)))
#endif

#ifndef HAVE_LINK
extern int	link(const char *src, const char *dst);
#endif

#ifndef HAVE_MKDTEMP
extern char *mkdtemp(char *path);
#endif

#ifndef HAVE_INET_ATON
#include <netinet/in.h>
#include <arpa/inet.h>
extern int	inet_aton(const char *cp, struct in_addr *addr);
#endif

/*
 * Windows and older Unix don't have pread(2) and pwrite(2).  We have
 * replacement functions, but they have slightly different semantics so we'll
 * use a name with a pg_ prefix to avoid confusion.
 */
#ifdef HAVE_PREAD
#define pg_pread pread
#else
extern ssize_t pg_pread(int fd, void *buf, size_t nbyte, off_t offset);
#endif

#ifdef HAVE_PWRITE
#define pg_pwrite pwrite
#else
extern ssize_t pg_pwrite(int fd, const void *buf, size_t nbyte, off_t offset);
#endif

/* For pg_pwritev() and pg_preadv(), see port/pg_iovec.h. */

#if !HAVE_DECL_STRLCAT
extern size_t strlcat(char *dst, const char *src, size_t siz);
#endif

#if !HAVE_DECL_STRLCPY
extern size_t strlcpy(char *dst, const char *src, size_t siz);
#endif

#if !HAVE_DECL_STRNLEN
extern size_t strnlen(const char *str, size_t maxlen);
#endif

#ifndef HAVE_SETENV
extern int	setenv(const char *name, const char *value, int overwrite);
#endif

#ifndef HAVE_UNSETENV
extern int	unsetenv(const char *name);
#endif

#ifndef HAVE_DLOPEN
extern void *dlopen(const char *file, int mode);
extern void *dlsym(void *handle, const char *symbol);
extern int	dlclose(void *handle);
extern char *dlerror(void);
#endif

/*
 * In some older systems, the RTLD_NOW flag isn't defined and the mode
 * argument to dlopen must always be 1.
 */
#if !HAVE_DECL_RTLD_NOW
#define RTLD_NOW 1
#endif

/*
 * The RTLD_GLOBAL flag is wanted if available, but it doesn't exist
 * everywhere.  If it doesn't exist, set it to 0 so it has no effect.
 */
#if !HAVE_DECL_RTLD_GLOBAL
#define RTLD_GLOBAL 0
#endif

/* thread.c */
#ifndef WIN32
extern bool pg_get_user_name(uid_t user_id, char *buffer, size_t buflen);
extern bool pg_get_user_home_dir(uid_t user_id, char *buffer, size_t buflen);
#endif

extern void pg_qsort(void *base, size_t nel, size_t elsize,
					 int (*cmp) (const void *, const void *));
extern int	pg_qsort_strcmp(const void *a, const void *b);

#define qsort(a,b,c,d) pg_qsort(a,b,c,d)

typedef int (*qsort_arg_comparator) (const void *a, const void *b, void *arg);

extern void qsort_arg(void *base, size_t nel, size_t elsize,
					  qsort_arg_comparator cmp, void *arg);

extern void qsort_interruptible(void *base, size_t nel, size_t elsize,
								qsort_arg_comparator cmp, void *arg);

extern void *bsearch_arg(const void *key, const void *base,
						 size_t nmemb, size_t size,
						 int (*compar) (const void *, const void *, void *),
						 void *arg);

/* port/chklocale.c */
extern int	pg_get_encoding_from_locale(const char *ctype, bool write_message);

#if defined(WIN32) && !defined(FRONTEND)
extern int	pg_codepage_to_encoding(UINT cp);
#endif

/* port/inet_net_ntop.c */
extern char *pg_inet_net_ntop(int af, const void *src, int bits,
							  char *dst, size_t size);

/* port/pg_strong_random.c */
extern void pg_strong_random_init(void);
extern bool pg_strong_random(void *buf, size_t len);

/*
 * pg_backend_random used to be a wrapper for pg_strong_random before
 * Postgres 12 for the backend code.
 */
#define pg_backend_random pg_strong_random

/* port/pgcheckdir.c */
extern int	pg_check_dir(const char *dir);

/* port/pgmkdirp.c */
extern int	pg_mkdir_p(char *path, int omode);

/* port/pqsignal.c */
typedef void (*pqsigfunc) (int signo);
extern pqsigfunc pqsignal(int signo, pqsigfunc func);

/* port/quotes.c */
extern char *escape_single_quotes_ascii(const char *src);

/* common/wait_error.c */
extern char *wait_result_to_str(int exit_status);
extern bool wait_result_is_signal(int exit_status, int signum);
extern bool wait_result_is_any_signal(int exit_status, bool include_command_not_found);

#endif							/* PG_PORT_H */


#endif							/* C_H */




/*-------------------------------------------------------------------------
 *
 * postgres.h
 *	  Primary include file for PostgreSQL server .c files
 *
 * This should be the first file included by PostgreSQL backend modules.
 * Client-side code should include postgres_fe.h instead.
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1995, Regents of the University of California
 *
 * src/include/postgres.h
 *
 *-------------------------------------------------------------------------
 */
/*
 *----------------------------------------------------------------
 *	 TABLE OF CONTENTS
 *
 *		When adding stuff to this file, please try to put stuff
 *		into the relevant section, or add new sections as appropriate.
 *
 *	  section	description
 *	  -------	------------------------------------------------
 *		1)		variable-length datatypes (TOAST support)
 *		2)		Datum type + support macros
 *
 *	 NOTES
 *
 *	In general, this file should contain declarations that are widely needed
 *	in the backend environment, but are of no interest outside the backend.
 *
 *	Simple type definitions live in c.h, where they are shared with
 *	postgres_fe.h.  We do that since those type definitions are needed by
 *	frontend modules that want to deal with binary data transmission to or
 *	from the backend.  Type definitions in this file should be for
 *	representations that never escape the backend, such as Datum or
 *	TOASTed varlena objects.
 *
 *----------------------------------------------------------------
 */
#ifndef POSTGRES_H
#define POSTGRES_H

/*-------------------------------------------------------------------------
 *
 * elog.h
 *	  POSTGRES error reporting/logging definitions.
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/elog.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ELOG_H
#define ELOG_H

#include <setjmp.h>

/* Error level codes */
#define DEBUG5		10			/* Debugging messages, in categories of
								 * decreasing detail. */
#define DEBUG4		11
#define DEBUG3		12
#define DEBUG2		13
#define DEBUG1		14			/* used by GUC debug_* variables */
#define LOG			15			/* Server operational messages; sent only to
								 * server log by default. */
#define LOG_SERVER_ONLY 16		/* Same as LOG for server reporting, but never
								 * sent to client. */
#define COMMERROR	LOG_SERVER_ONLY /* Client communication problems; same as
									 * LOG for server reporting, but never
									 * sent to client. */
#define INFO		17			/* Messages specifically requested by user (eg
								 * VACUUM VERBOSE output); always sent to
								 * client regardless of client_min_messages,
								 * but by default not sent to server log. */
#define NOTICE		18			/* Helpful messages to users about query
								 * operation; sent to client and not to server
								 * log by default. */
#define WARNING		19			/* Warnings.  NOTICE is for expected messages
								 * like implicit sequence creation by SERIAL.
								 * WARNING is for unexpected messages. */
#define PGWARNING	19			/* Must equal WARNING; see NOTE below. */
#define WARNING_CLIENT_ONLY	20	/* Warnings to be sent to client as usual, but
								 * never to the server log. */
#define ERROR		21			/* user error - abort transaction; return to
								 * known state */
#define PGERROR		21			/* Must equal ERROR; see NOTE below. */
#define FATAL		22			/* fatal error - abort process */
#define PANIC		23			/* take down the other backends with me */

/*
 * NOTE: the alternate names PGWARNING and PGERROR are useful for dealing
 * with third-party headers that make other definitions of WARNING and/or
 * ERROR.  One can, for example, re-define ERROR as PGERROR after including
 * such a header.
 */


/* macros for representing SQLSTATE strings compactly */
#define PGSIXBIT(ch)	(((ch) - '0') & 0x3F)
#define PGUNSIXBIT(val) (((val) & 0x3F) + '0')

#define MAKE_SQLSTATE(ch1,ch2,ch3,ch4,ch5)	\
	(PGSIXBIT(ch1) + (PGSIXBIT(ch2) << 6) + (PGSIXBIT(ch3) << 12) + \
	 (PGSIXBIT(ch4) << 18) + (PGSIXBIT(ch5) << 24))

/* These macros depend on the fact that '0' becomes a zero in PGSIXBIT */
#define ERRCODE_TO_CATEGORY(ec)  ((ec) & ((1 << 12) - 1))
#define ERRCODE_IS_CATEGORY(ec)  (((ec) & ~((1 << 12) - 1)) == 0)

/* SQLSTATE codes for errors are defined in a separate file */
// #include "utils/errcodes.h"

/*
 * Provide a way to prevent "errno" from being accidentally used inside an
 * elog() or ereport() invocation.  Since we know that some operating systems
 * define errno as something involving a function call, we'll put a local
 * variable of the same name as that function in the local scope to force a
 * compile error.  On platforms that don't define errno in that way, nothing
 * happens, so we get no warning ... but we can live with that as long as it
 * happens on some popular platforms.
 */
#if defined(errno) && defined(__linux__)
#define pg_prevent_errno_in_scope() int __errno_location pg_attribute_unused()
#elif defined(errno) && (defined(__darwin__) || defined(__freebsd__))
#define pg_prevent_errno_in_scope() int __error pg_attribute_unused()
#else
#define pg_prevent_errno_in_scope()
#endif


/*----------
 * New-style error reporting API: to be used in this way:
 *		ereport(ERROR,
 *				errcode(ERRCODE_UNDEFINED_CURSOR),
 *				errmsg("portal \"%s\" not found", stmt->portalname),
 *				... other errxxx() fields as needed ...);
 *
 * The error level is required, and so is a primary error message (errmsg
 * or errmsg_internal).  All else is optional.  errcode() defaults to
 * ERRCODE_INTERNAL_ERROR if elevel is ERROR or more, ERRCODE_WARNING
 * if elevel is WARNING, or ERRCODE_SUCCESSFUL_COMPLETION if elevel is
 * NOTICE or below.
 *
 * Before Postgres v12, extra parentheses were required around the
 * list of auxiliary function calls; that's now optional.
 *
 * ereport_domain() allows a message domain to be specified, for modules that
 * wish to use a different message catalog from the backend's.  To avoid having
 * one copy of the default text domain per .o file, we define it as NULL here
 * and have errstart insert the default text domain.  Modules can either use
 * ereport_domain() directly, or preferably they can override the TEXTDOMAIN
 * macro.
 *
 * When __builtin_constant_p is available and elevel >= ERROR we make a call
 * to errstart_cold() instead of errstart().  This version of the function is
 * marked with pg_attribute_cold which will coax supporting compilers into
 * generating code which is more optimized towards non-ERROR cases.  Because
 * we use __builtin_constant_p() in the condition, when elevel is not a
 * compile-time constant, or if it is, but it's < ERROR, the compiler has no
 * need to generate any code for this branch.  It can simply call errstart()
 * unconditionally.
 *
 * If elevel >= ERROR, the call will not return; we try to inform the compiler
 * of that via pg_unreachable().  However, no useful optimization effect is
 * obtained unless the compiler sees elevel as a compile-time constant, else
 * we're just adding code bloat.  So, if __builtin_constant_p is available,
 * use that to cause the second if() to vanish completely for non-constant
 * cases.  We avoid using a local variable because it's not necessary and
 * prevents gcc from making the unreachability deduction at optlevel -O0.
 *----------
 */
#ifdef HAVE__BUILTIN_CONSTANT_P
#define ereport_domain(elevel, domain, ...)	\
	do { \
		pg_prevent_errno_in_scope(); \
		if (__builtin_constant_p(elevel) && (elevel) >= ERROR ? \
			errstart_cold(elevel, domain) : \
			errstart(elevel, domain)) \
			__VA_ARGS__, errfinish(__FILE__, __LINE__, PG_FUNCNAME_MACRO); \
		if (__builtin_constant_p(elevel) && (elevel) >= ERROR) \
			pg_unreachable(); \
	} while(0)
#else							/* !HAVE__BUILTIN_CONSTANT_P */
#define ereport_domain(elevel, domain, ...)	\
	do { \
		const int elevel_ = (elevel); \
		pg_prevent_errno_in_scope(); \
		if (errstart(elevel_, domain)) \
			__VA_ARGS__, errfinish(__FILE__, __LINE__, PG_FUNCNAME_MACRO); \
		if (elevel_ >= ERROR) \
			pg_unreachable(); \
	} while(0)
#endif							/* HAVE__BUILTIN_CONSTANT_P */

#define ereport(elevel, ...)	\
	ereport_domain(elevel, TEXTDOMAIN, __VA_ARGS__)

#define TEXTDOMAIN NULL

extern bool message_level_is_interesting(int elevel);

extern bool errstart(int elevel, const char *domain);
extern pg_attribute_cold bool errstart_cold(int elevel, const char *domain);
extern void errfinish(const char *filename, int lineno, const char *funcname);

extern int	errcode(int sqlerrcode);

extern int	errcode_for_file_access(void);
extern int	errcode_for_socket_access(void);

extern int	errmsg(const char *fmt,...) pg_attribute_printf(1, 2);
extern int	errmsg_internal(const char *fmt,...) pg_attribute_printf(1, 2);

extern int	errmsg_plural(const char *fmt_singular, const char *fmt_plural,
						  unsigned long n,...) pg_attribute_printf(1, 4) pg_attribute_printf(2, 4);

extern int	errdetail(const char *fmt,...) pg_attribute_printf(1, 2);
extern int	errdetail_internal(const char *fmt,...) pg_attribute_printf(1, 2);

extern int	errdetail_log(const char *fmt,...) pg_attribute_printf(1, 2);

extern int	errdetail_log_plural(const char *fmt_singular,
								 const char *fmt_plural,
								 unsigned long n,...) pg_attribute_printf(1, 4) pg_attribute_printf(2, 4);

extern int	errdetail_plural(const char *fmt_singular, const char *fmt_plural,
							 unsigned long n,...) pg_attribute_printf(1, 4) pg_attribute_printf(2, 4);

extern int	errhint(const char *fmt,...) pg_attribute_printf(1, 2);

extern int	errhint_plural(const char *fmt_singular, const char *fmt_plural,
						   unsigned long n,...) pg_attribute_printf(1, 4) pg_attribute_printf(2, 4);

/*
 * errcontext() is typically called in error context callback functions, not
 * within an ereport() invocation. The callback function can be in a different
 * module than the ereport() call, so the message domain passed in errstart()
 * is not usually the correct domain for translating the context message.
 * set_errcontext_domain() first sets the domain to be used, and
 * errcontext_msg() passes the actual message.
 */
#define errcontext	set_errcontext_domain(TEXTDOMAIN),	errcontext_msg

extern int	set_errcontext_domain(const char *domain);

extern int	errcontext_msg(const char *fmt,...) pg_attribute_printf(1, 2);

extern int	errhidestmt(bool hide_stmt);
extern int	errhidecontext(bool hide_ctx);

extern int	errbacktrace(void);

extern int	errposition(int cursorpos);

extern int	internalerrposition(int cursorpos);
extern int	internalerrquery(const char *query);

extern int	err_generic_string(int field, const char *str);

extern int	geterrcode(void);
extern int	geterrposition(void);
extern int	getinternalerrposition(void);


/*----------
 * Old-style error reporting API: to be used in this way:
 *		elog(ERROR, "portal \"%s\" not found", stmt->portalname);
 *----------
 */
#define elog(elevel, ...)  \
	ereport(elevel, errmsg_internal(__VA_ARGS__))


/* Support for constructing error strings separately from ereport() calls */

extern void pre_format_elog_string(int errnumber, const char *domain);
extern char *format_elog_string(const char *fmt,...) pg_attribute_printf(1, 2);


/* Support for attaching context information to error reports */

typedef struct ErrorContextCallback
{
	struct ErrorContextCallback *previous;
	void		(*callback) (void *arg);
	void	   *arg;
} ErrorContextCallback;

extern PGDLLIMPORT ErrorContextCallback *error_context_stack;


/*----------
 * API for catching ereport(ERROR) exits.  Use these macros like so:
 *
 *		PG_TRY();
 *		{
 *			... code that might throw ereport(ERROR) ...
 *		}
 *		PG_CATCH();
 *		{
 *			... error recovery code ...
 *		}
 *		PG_END_TRY();
 *
 * (The braces are not actually necessary, but are recommended so that
 * pgindent will indent the construct nicely.)  The error recovery code
 * can either do PG_RE_THROW to propagate the error outwards, or do a
 * (sub)transaction abort. Failure to do so may leave the system in an
 * inconsistent state for further processing.
 *
 * For the common case that the error recovery code and the cleanup in the
 * normal code path are identical, the following can be used instead:
 *
 *		PG_TRY();
 *		{
 *			... code that might throw ereport(ERROR) ...
 *		}
 *		PG_FINALLY();
 *		{
 *			... cleanup code ...
 *		}
 *      PG_END_TRY();
 *
 * The cleanup code will be run in either case, and any error will be rethrown
 * afterwards.
 *
 * You cannot use both PG_CATCH() and PG_FINALLY() in the same
 * PG_TRY()/PG_END_TRY() block.
 *
 * Note: while the system will correctly propagate any new ereport(ERROR)
 * occurring in the recovery section, there is a small limit on the number
 * of levels this will work for.  It's best to keep the error recovery
 * section simple enough that it can't generate any new errors, at least
 * not before popping the error stack.
 *
 * Note: an ereport(FATAL) will not be caught by this construct; control will
 * exit straight through proc_exit().  Therefore, do NOT put any cleanup
 * of non-process-local resources into the error recovery section, at least
 * not without taking thought for what will happen during ereport(FATAL).
 * The PG_ENSURE_ERROR_CLEANUP macros provided by storage/ipc.h may be
 * helpful in such cases.
 *
 * Note: if a local variable of the function containing PG_TRY is modified
 * in the PG_TRY section and used in the PG_CATCH section, that variable
 * must be declared "volatile" for POSIX compliance.  This is not mere
 * pedantry; we have seen bugs from compilers improperly optimizing code
 * away when such a variable was not marked.  Beware that gcc's -Wclobbered
 * warnings are just about entirely useless for catching such oversights.
 *----------
 */
#define PG_TRY()  \
	do { \
		sigjmp_buf *_save_exception_stack = PG_exception_stack; \
		ErrorContextCallback *_save_context_stack = error_context_stack; \
		sigjmp_buf _local_sigjmp_buf; \
		bool _do_rethrow = false; \
		if (sigsetjmp(_local_sigjmp_buf, 0) == 0) \
		{ \
			PG_exception_stack = &_local_sigjmp_buf

#define PG_CATCH()	\
		} \
		else \
		{ \
			PG_exception_stack = _save_exception_stack; \
			error_context_stack = _save_context_stack

#define PG_FINALLY() \
		} \
		else \
			_do_rethrow = true; \
		{ \
			PG_exception_stack = _save_exception_stack; \
			error_context_stack = _save_context_stack

#define PG_END_TRY()  \
		} \
		if (_do_rethrow) \
				PG_RE_THROW(); \
		PG_exception_stack = _save_exception_stack; \
		error_context_stack = _save_context_stack; \
	} while (0)

/*
 * Some compilers understand pg_attribute_noreturn(); for other compilers,
 * insert pg_unreachable() so that the compiler gets the point.
 */
#ifdef HAVE_PG_ATTRIBUTE_NORETURN
#define PG_RE_THROW()  \
	pg_re_throw()
#else
#define PG_RE_THROW()  \
	(pg_re_throw(), pg_unreachable())
#endif

extern PGDLLIMPORT sigjmp_buf *PG_exception_stack;


/* Stuff that error handlers might want to use */

/*
 * ErrorData holds the data accumulated during any one ereport() cycle.
 * Any non-NULL pointers must point to palloc'd data.
 * (The const pointers are an exception; we assume they point at non-freeable
 * constant strings.)
 */
typedef struct ErrorData
{
	int			elevel;			/* error level */
	bool		output_to_server;	/* will report to server log? */
	bool		output_to_client;	/* will report to client? */
	bool		hide_stmt;		/* true to prevent STATEMENT: inclusion */
	bool		hide_ctx;		/* true to prevent CONTEXT: inclusion */
	const char *filename;		/* __FILE__ of ereport() call */
	int			lineno;			/* __LINE__ of ereport() call */
	const char *funcname;		/* __func__ of ereport() call */
	const char *domain;			/* message domain */
	const char *context_domain; /* message domain for context message */
	int			sqlerrcode;		/* encoded ERRSTATE */
	char	   *message;		/* primary error message (translated) */
	char	   *detail;			/* detail error message */
	char	   *detail_log;		/* detail error message for server log only */
	char	   *hint;			/* hint message */
	char	   *context;		/* context message */
	char	   *backtrace;		/* backtrace */
	const char *message_id;		/* primary message's id (original string) */
	char	   *schema_name;	/* name of schema */
	char	   *table_name;		/* name of table */
	char	   *column_name;	/* name of column */
	char	   *datatype_name;	/* name of datatype */
	char	   *constraint_name;	/* name of constraint */
	int			cursorpos;		/* cursor index into query string */
	int			internalpos;	/* cursor index into internalquery */
	char	   *internalquery;	/* text of internally-generated query */
	int			saved_errno;	/* errno at entry */

	/* context containing associated non-constant strings */
	struct MemoryContextData *assoc_context;
} ErrorData;

extern void EmitErrorReport(void);
extern ErrorData *CopyErrorData(void);
extern void FreeErrorData(ErrorData *edata);
extern void FlushErrorState(void);
extern void ReThrowError(ErrorData *edata) pg_attribute_noreturn();
extern void ThrowErrorData(ErrorData *edata);
extern void pg_re_throw(void) pg_attribute_noreturn();

extern char *GetErrorContextStack(void);

/* Hook for intercepting messages before they are sent to the server log */
typedef void (*emit_log_hook_type) (ErrorData *edata);
extern PGDLLIMPORT emit_log_hook_type emit_log_hook;


/* GUC-configurable parameters */

typedef enum
{
	PGERROR_TERSE,				/* single-line error messages */
	PGERROR_DEFAULT,			/* recommended style */
	PGERROR_VERBOSE				/* all the facts, ma'am */
}			PGErrorVerbosity;

extern PGDLLIMPORT int Log_error_verbosity;
extern PGDLLIMPORT char *Log_line_prefix;
extern PGDLLIMPORT int Log_destination;
extern PGDLLIMPORT char *Log_destination_string;
extern PGDLLIMPORT bool syslog_sequence_numbers;
extern PGDLLIMPORT bool syslog_split_messages;

/* Log destination bitmap */
#define LOG_DESTINATION_STDERR	 1
#define LOG_DESTINATION_SYSLOG	 2
#define LOG_DESTINATION_EVENTLOG 4
#define LOG_DESTINATION_CSVLOG	 8
#define LOG_DESTINATION_JSONLOG	16

/* Other exported functions */
extern void DebugFileOpen(void);
extern char *unpack_sql_state(int sql_state);
extern bool in_error_recursion_trouble(void);

/* Common functions shared across destinations */
extern void reset_formatted_start_time(void);
extern char *get_formatted_start_time(void);
extern char *get_formatted_log_time(void);
extern const char *get_backend_type_for_log(void);
extern bool check_log_of_query(ErrorData *edata);
extern const char *error_severity(int elevel);
extern void write_pipe_chunks(char *data, int len, int dest);

/* Destination-specific functions */
extern void write_csvlog(ErrorData *edata);
extern void write_jsonlog(ErrorData *edata);

#ifdef HAVE_SYSLOG
extern void set_syslog_parameters(const char *ident, int facility);
#endif

/*
 * Write errors to stderr (or by equal means when stderr is
 * not available). Used before ereport/elog can be used
 * safely (memory context, GUC load etc)
 */
extern void write_stderr(const char *fmt,...) pg_attribute_printf(1, 2);

#endif							/* ELOG_H */

/*-------------------------------------------------------------------------
 *
 * elog.c
 *	  error logging and reporting
 *
 * Because of the extremely high rate at which log messages can be generated,
 * we need to be mindful of the performance cost of obtaining any information
 * that may be logged.  Also, it's important to keep in mind that this code may
 * get called from within an aborted transaction, in which case operations
 * such as syscache lookups are unsafe.
 *
 * Some notes about recursion and errors during error processing:
 *
 * We need to be robust about recursive-error scenarios --- for example,
 * if we run out of memory, it's important to be able to report that fact.
 * There are a number of considerations that go into this.
 *
 * First, distinguish between re-entrant use and actual recursion.  It
 * is possible for an error or warning message to be emitted while the
 * parameters for an error message are being computed.  In this case
 * errstart has been called for the outer message, and some field values
 * may have already been saved, but we are not actually recursing.  We handle
 * this by providing a (small) stack of ErrorData records.  The inner message
 * can be computed and sent without disturbing the state of the outer message.
 * (If the inner message is actually an error, this isn't very interesting
 * because control won't come back to the outer message generator ... but
 * if the inner message is only debug or log data, this is critical.)
 *
 * Second, actual recursion will occur if an error is reported by one of
 * the elog.c routines or something they call.  By far the most probable
 * scenario of this sort is "out of memory"; and it's also the nastiest
 * to handle because we'd likely also run out of memory while trying to
 * report this error!  Our escape hatch for this case is to reset the
 * ErrorContext to empty before trying to process the inner error.  Since
 * ErrorContext is guaranteed to have at least 8K of space in it (see mcxt.c),
 * we should be able to process an "out of memory" message successfully.
 * Since we lose the prior error state due to the reset, we won't be able
 * to return to processing the original error, but we wouldn't have anyway.
 * (NOTE: the escape hatch is not used for recursive situations where the
 * inner message is of less than ERROR severity; in that case we just
 * try to process it and return normally.  Usually this will work, but if
 * it ends up in infinite recursion, we will PANIC due to error stack
 * overflow.)
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/error/elog.c
 *
 *-------------------------------------------------------------------------
 */
// #include "postgres.h"

#include <fcntl.h>
#include <time.h>
#include <unistd.h>
#include <signal.h>
#include <ctype.h>
#ifdef HAVE_SYSLOG
#include <syslog.h>
#endif
#ifdef HAVE_EXECINFO_H
#include <execinfo.h>
#endif

// #include "access/transam.h"
// #include "access/xact.h"
// #include "libpq/libpq.h"
// #include "libpq/pqformat.h"
// #include "mb/pg_wchar.h"
// #include "miscadmin.h"
// #include "pgstat.h"
// #include "postmaster/bgworker.h"
// #include "postmaster/postmaster.h"
// #include "postmaster/syslogger.h"
// #include "storage/ipc.h"
// #include "storage/proc.h"
// #include "tcop/tcopprot.h"
// #include "utils/guc.h"
// #include "utils/memutils.h"
// #include "utils/ps_status.h"


/* In this module, access gettext() via err_gettext() */
#undef _
#define _(x) err_gettext(x)


/* Global variables */
ErrorContextCallback *error_context_stack = NULL;

sigjmp_buf *PG_exception_stack = NULL;

extern bool redirection_done;

/*
 * Hook for intercepting messages before they are sent to the server log.
 * Note that the hook will not get called for messages that are suppressed
 * by log_min_messages.  Also note that logging hooks implemented in preload
 * libraries will miss any log messages that are generated before the
 * library is loaded.
 */
emit_log_hook_type emit_log_hook = NULL;

/* GUC parameters */
int			Log_error_verbosity = PGERROR_VERBOSE;
char	   *Log_line_prefix = NULL; /* format for extra log line info */
int			Log_destination = LOG_DESTINATION_STDERR;
char	   *Log_destination_string = NULL;
bool		syslog_sequence_numbers = true;
bool		syslog_split_messages = true;

#ifdef HAVE_SYSLOG

/*
 * Max string length to send to syslog().  Note that this doesn't count the
 * sequence-number prefix we add, and of course it doesn't count the prefix
 * added by syslog itself.  Solaris and sysklogd truncate the final message
 * at 1024 bytes, so this value leaves 124 bytes for those prefixes.  (Most
 * other syslog implementations seem to have limits of 2KB or so.)
 */
#ifndef PG_SYSLOG_LIMIT
#define PG_SYSLOG_LIMIT 900
#endif

static bool openlog_done = false;
static char *syslog_ident = NULL;
static int	syslog_facility = LOG_LOCAL0;

static void write_syslog(int level, const char *line);
#endif

#ifdef WIN32
extern char *event_source;

static void write_eventlog(int level, const char *line, int len);
#endif

/* We provide a small stack of ErrorData records for re-entrant cases */
#define ERRORDATA_STACK_SIZE  5

static ErrorData errordata[ERRORDATA_STACK_SIZE];

static int	errordata_stack_depth = -1; /* index of topmost active frame */

static int	recursion_depth = 0;	/* to detect actual recursion */

/*
 * Saved timeval and buffers for formatted timestamps that might be used by
 * both log_line_prefix and csv logs.
 */
static struct timeval saved_timeval;
static bool saved_timeval_set = false;

#define FORMATTED_TS_LEN 128
static char formatted_start_time[FORMATTED_TS_LEN];
static char formatted_log_time[FORMATTED_TS_LEN];


/* Macro for checking errordata_stack_depth is reasonable */
#define CHECK_STACK_DEPTH() \
	do { \
		if (errordata_stack_depth < 0) \
		{ \
			errordata_stack_depth = -1; \
			ereport(ERROR, (errmsg_internal("errstart was not called"))); \
		} \
	} while (0)


static const char *err_gettext(const char *str) pg_attribute_format_arg(1);
static pg_noinline void set_backtrace(ErrorData *edata, int num_skip);
// static void set_errdata_field(MemoryContextData *cxt, char **ptr, const char *str);
static void write_console(const char *line, int len);
static void setup_formatted_log_time(void);
static void setup_formatted_start_time(void);
static const char *process_log_prefix_padding(const char *p, int *padding);
static void log_line_prefix(StringInfo buf, ErrorData *edata);
static void write_csvlog(ErrorData *edata);
static void send_message_to_server_log(ErrorData *edata);
static void write_pipe_chunks(char *data, int len, int dest);
static void send_message_to_frontend(ErrorData *edata);
static const char *error_severity(int elevel);
static void append_with_tabs(StringInfo buf, const char *str);


/*
 * is_log_level_output -- is elevel logically >= log_min_level?
 *
 * We use this for tests that should consider LOG to sort out-of-order,
 * between ERROR and FATAL.  Generally this is the right thing for testing
 * whether a message should go to the postmaster log, whereas a simple >=
 * test is correct for testing whether the message should go to the client.
 */
static inline bool
is_log_level_output(int elevel, int log_min_level)
{
	if (elevel == LOG || elevel == LOG_SERVER_ONLY)
	{
		if (log_min_level == LOG || log_min_level <= ERROR)
			return true;
	}
	else if (elevel == WARNING_CLIENT_ONLY)
	{
		/* never sent to log, regardless of log_min_level */
		return false;
	}
	else if (log_min_level == LOG)
	{
		/* elevel != LOG */
		if (elevel >= FATAL)
			return true;
	}
	/* Neither is LOG */
	else if (elevel >= log_min_level)
		return true;

	return false;
}

/*
 * Policy-setting subroutines.  These are fairly simple, but it seems wise
 * to have the code in just one place.
 */

/*
 * should_output_to_server --- should message of given elevel go to the log?
 */
static inline bool
should_output_to_server(int elevel)
{
	return is_log_level_output(elevel, log_min_messages);
}

/*
 * should_output_to_client --- should message of given elevel go to the client?
 */
static inline bool
should_output_to_client(int elevel)
{
	if (whereToSendOutput == DestRemote && elevel != LOG_SERVER_ONLY)
	{
		/*
		 * client_min_messages is honored only after we complete the
		 * authentication handshake.  This is required both for security
		 * reasons and because many clients can't handle NOTICE messages
		 * during authentication.
		 */
		if (ClientAuthInProgress)
			return (elevel >= ERROR);
		else
			return (elevel >= client_min_messages || elevel == INFO);
	}
	return false;
}


/*
 * message_level_is_interesting --- would ereport/elog do anything?
 *
 * Returns true if ereport/elog with this elevel will not be a no-op.
 * This is useful to short-circuit any expensive preparatory work that
 * might be needed for a logging message.  There is no point in
 * prepending this to a bare ereport/elog call, however.
 */
bool
message_level_is_interesting(int elevel)
{
	/*
	 * Keep this in sync with the decision-making in errstart().
	 */
	if (elevel >= ERROR ||
		should_output_to_server(elevel) ||
		should_output_to_client(elevel))
		return true;
	return false;
}


/*
 * in_error_recursion_trouble --- are we at risk of infinite error recursion?
 *
 * This function exists to provide common control of various fallback steps
 * that we take if we think we are facing infinite error recursion.  See the
 * callers for details.
 */
bool
in_error_recursion_trouble(void)
{
	/* Pull the plug if recurse more than once */
	return (recursion_depth > 2);
}

/*
 * One of those fallback steps is to stop trying to localize the error
 * message, since there's a significant probability that that's exactly
 * what's causing the recursion.
 */
static inline const char *
err_gettext(const char *str)
{
#ifdef ENABLE_NLS
	if (in_error_recursion_trouble())
		return str;
	else
		return gettext(str);
#else
	return str;
#endif
}

/*
 * errstart_cold
 *		A simple wrapper around errstart, but hinted to be "cold".  Supporting
 *		compilers are more likely to move code for branches containing this
 *		function into an area away from the calling function's code.  This can
 *		result in more commonly executed code being more compact and fitting
 *		on fewer cache lines.
 */
pg_attribute_cold bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}

/*
 * errstart --- begin an error-reporting cycle
 *
 * Create and initialize error stack entry.  Subsequently, errmsg() and
 * perhaps other routines will be called to further populate the stack entry.
 * Finally, errfinish() will be called to actually process the error report.
 *
 * Returns true in normal case.  Returns false to short-circuit the error
 * report (if it's a warning or lower and not to be reported anywhere).
 */
bool
errstart(int elevel, const char *domain)
{
	ErrorData  *edata;
	bool		output_to_server;
	bool		output_to_client = false;
	int			i;

	/*
	 * Check some cases in which we want to promote an error into a more
	 * severe error.  None of this logic applies for non-error messages.
	 */
	if (elevel >= ERROR)
	{
		/*
		 * If we are inside a critical section, all errors become PANIC
		 * errors.  See miscadmin.h.
		 */
		if (CritSectionCount > 0)
			elevel = PANIC;

		/*
		 * Check reasons for treating ERROR as FATAL:
		 *
		 * 1. we have no handler to pass the error to (implies we are in the
		 * postmaster or in backend startup).
		 *
		 * 2. ExitOnAnyError mode switch is set (initdb uses this).
		 *
		 * 3. the error occurred after proc_exit has begun to run.  (It's
		 * proc_exit's responsibility to see that this doesn't turn into
		 * infinite recursion!)
		 */
		if (elevel == ERROR)
		{
			if (PG_exception_stack == NULL ||
				ExitOnAnyError ||
				proc_exit_inprogress)
				elevel = FATAL;
		}

		/*
		 * If the error level is ERROR or more, errfinish is not going to
		 * return to caller; therefore, if there is any stacked error already
		 * in progress it will be lost.  This is more or less okay, except we
		 * do not want to have a FATAL or PANIC error downgraded because the
		 * reporting process was interrupted by a lower-grade error.  So check
		 * the stack and make sure we panic if panic is warranted.
		 */
		for (i = 0; i <= errordata_stack_depth; i++)
			elevel = Max(elevel, errordata[i].elevel);
	}

	/*
	 * Now decide whether we need to process this report at all; if it's
	 * warning or less and not enabled for logging, just return false without
	 * starting up any error logging machinery.
	 */
	output_to_server = should_output_to_server(elevel);
	output_to_client = should_output_to_client(elevel);
	if (elevel < ERROR && !output_to_server && !output_to_client)
		return false;

	/*
	 * We need to do some actual work.  Make sure that memory context
	 * initialization has finished, else we can't do anything useful.
	 */
	if (ErrorContext == NULL)
	{
		/* Oops, hard crash time; very little we can do safely here */
		write_stderr("error occurred before error message processing is available\n");
		exit(2);
	}

	/*
	 * Okay, crank up a stack entry to store the info in.
	 */

	if (recursion_depth++ > 0 && elevel >= ERROR)
	{
		/*
		 * Oops, error during error processing.  Clear ErrorContext as
		 * discussed at top of file.  We will not return to the original
		 * error's reporter or handler, so we don't need it.
		 */
		MemoryContextReset(ErrorContext);

		/*
		 * Infinite error recursion might be due to something broken in a
		 * context traceback routine.  Abandon them too.  We also abandon
		 * attempting to print the error statement (which, if long, could
		 * itself be the source of the recursive failure).
		 */
		if (in_error_recursion_trouble())
		{
			error_context_stack = NULL;
			debug_query_string = NULL;
		}
	}
	if (++errordata_stack_depth >= ERRORDATA_STACK_SIZE)
	{
		/*
		 * Wups, stack not big enough.  We treat this as a PANIC condition
		 * because it suggests an infinite loop of errors during error
		 * recovery.
		 */
		errordata_stack_depth = -1; /* make room on stack */
		ereport(PANIC, (errmsg_internal("ERRORDATA_STACK_SIZE exceeded")));
	}

	/* Initialize data for this error frame */
	edata = &errordata[errordata_stack_depth];
	MemSet(edata, 0, sizeof(ErrorData));
	edata->elevel = elevel;
	edata->output_to_server = output_to_server;
	edata->output_to_client = output_to_client;
	/* the default text domain is the backend's */
	edata->domain = domain ? domain : PG_TEXTDOMAIN("postgres");
	/* initialize context_domain the same way (see set_errcontext_domain()) */
	edata->context_domain = edata->domain;
	/* Select default errcode based on elevel */
	if (elevel >= ERROR)
		edata->sqlerrcode = ERRCODE_INTERNAL_ERROR;
	else if (elevel >= WARNING)
		edata->sqlerrcode = ERRCODE_WARNING;
	else
		edata->sqlerrcode = ERRCODE_SUCCESSFUL_COMPLETION;
	/* errno is saved here so that error parameter eval can't change it */
	edata->saved_errno = errno;

	/*
	 * Any allocations for this error state level should go into ErrorContext
	 */
	edata->assoc_context = ErrorContext;

	recursion_depth--;
	return true;
}

/*
 * Checks whether the given funcname matches backtrace_functions; see
 * check_backtrace_functions.
 */
static bool
matches_backtrace_functions(const char *funcname)
{
	char	   *p;

	if (!backtrace_symbol_list || funcname == NULL || funcname[0] == '\0')
		return false;

	p = backtrace_symbol_list;
	for (;;)
	{
		if (*p == '\0')			/* end of backtrace_symbol_list */
			break;

		if (strcmp(funcname, p) == 0)
			return true;
		p += strlen(p) + 1;
	}

	return false;
}

/*
 * errfinish --- end an error-reporting cycle
 *
 * Produce the appropriate error report(s) and pop the error stack.
 *
 * If elevel, as passed to errstart(), is ERROR or worse, control does not
 * return to the caller.  See elog.h for the error level definitions.
 */
void
errfinish(const char *filename, int lineno, const char *funcname)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];
	int			elevel;
	MemoryContext oldcontext;
	ErrorContextCallback *econtext;

	recursion_depth++;
	CHECK_STACK_DEPTH();

	/* Save the last few bits of error state into the stack entry */
	if (filename)
	{
		const char *slash;

		/* keep only base name, useful especially for vpath builds */
		slash = strrchr(filename, '/');
		if (slash)
			filename = slash + 1;
		/* Some Windows compilers use backslashes in __FILE__ strings */
		slash = strrchr(filename, '\\');
		if (slash)
			filename = slash + 1;
	}

	edata->filename = filename;
	edata->lineno = lineno;
	edata->funcname = funcname;

	elevel = edata->elevel;

	/*
	 * Do processing in ErrorContext, which we hope has enough reserved space
	 * to report an error.
	 */
	oldcontext = MemoryContextSwitchTo(ErrorContext);

	if (!edata->backtrace &&
		edata->funcname &&
		backtrace_functions &&
		matches_backtrace_functions(edata->funcname))
		set_backtrace(edata, 2);

	/*
	 * Call any context callback functions.  Errors occurring in callback
	 * functions will be treated as recursive errors --- this ensures we will
	 * avoid infinite recursion (see errstart).
	 */
	for (econtext = error_context_stack;
		 econtext != NULL;
		 econtext = econtext->previous)
		econtext->callback(econtext->arg);

	/*
	 * If ERROR (not more nor less) we pass it off to the current handler.
	 * Printing it and popping the stack is the responsibility of the handler.
	 */
	if (elevel == ERROR)
	{
		/*
		 * We do some minimal cleanup before longjmp'ing so that handlers can
		 * execute in a reasonably sane state.
		 *
		 * Reset InterruptHoldoffCount in case we ereport'd from inside an
		 * interrupt holdoff section.  (We assume here that no handler will
		 * itself be inside a holdoff section.  If necessary, such a handler
		 * could save and restore InterruptHoldoffCount for itself, but this
		 * should make life easier for most.)
		 */
		InterruptHoldoffCount = 0;
		QueryCancelHoldoffCount = 0;

		CritSectionCount = 0;	/* should be unnecessary, but... */

		/*
		 * Note that we leave CurrentMemoryContext set to ErrorContext. The
		 * handler should reset it to something else soon.
		 */

		recursion_depth--;
		PG_RE_THROW();
	}

	/* Emit the message to the right places */
	EmitErrorReport();

	/* Now free up subsidiary data attached to stack entry, and release it */
	if (edata->message)
		pfree(edata->message);
	if (edata->detail)
		pfree(edata->detail);
	if (edata->detail_log)
		pfree(edata->detail_log);
	if (edata->hint)
		pfree(edata->hint);
	if (edata->context)
		pfree(edata->context);
	if (edata->backtrace)
		pfree(edata->backtrace);
	if (edata->schema_name)
		pfree(edata->schema_name);
	if (edata->table_name)
		pfree(edata->table_name);
	if (edata->column_name)
		pfree(edata->column_name);
	if (edata->datatype_name)
		pfree(edata->datatype_name);
	if (edata->constraint_name)
		pfree(edata->constraint_name);
	if (edata->internalquery)
		pfree(edata->internalquery);

	errordata_stack_depth--;

	/* Exit error-handling context */
	MemoryContextSwitchTo(oldcontext);
	recursion_depth--;

	/*
	 * Perform error recovery action as specified by elevel.
	 */
	if (elevel == FATAL)
	{
		/*
		 * For a FATAL error, we let proc_exit clean up and exit.
		 *
		 * If we just reported a startup failure, the client will disconnect
		 * on receiving it, so don't send any more to the client.
		 */
		if (PG_exception_stack == NULL && whereToSendOutput == DestRemote)
			whereToSendOutput = DestNone;

		/*
		 * fflush here is just to improve the odds that we get to see the
		 * error message, in case things are so hosed that proc_exit crashes.
		 * Any other code you might be tempted to add here should probably be
		 * in an on_proc_exit or on_shmem_exit callback instead.
		 */
		fflush(stdout);
		fflush(stderr);

		/*
		 * Let the statistics collector know. Only mark the session as
		 * terminated by fatal error if there is no other known cause.
		 */
		if (pgStatSessionEndCause == DISCONNECT_NORMAL)
			pgStatSessionEndCause = DISCONNECT_FATAL;

		/*
		 * Do normal process-exit cleanup, then return exit code 1 to indicate
		 * FATAL termination.  The postmaster may or may not consider this
		 * worthy of panic, depending on which subprocess returns it.
		 */
		proc_exit(1);
	}

	if (elevel >= PANIC)
	{
		/*
		 * Serious crash time. Postmaster will observe SIGABRT process exit
		 * status and kill the other backends too.
		 *
		 * XXX: what if we are *in* the postmaster?  abort() won't kill our
		 * children...
		 */
		fflush(stdout);
		fflush(stderr);
		abort();
	}

	/*
	 * Check for cancel/die interrupt first --- this is so that the user can
	 * stop a query emitting tons of notice or warning messages, even if it's
	 * in a loop that otherwise fails to check for interrupts.
	 */
	CHECK_FOR_INTERRUPTS();
}


/*
 * errcode --- add SQLSTATE error code to the current error
 *
 * The code is expected to be represented as per MAKE_SQLSTATE().
 */
int
errcode(int sqlerrcode)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];

	/* we don't bother incrementing recursion_depth */
	CHECK_STACK_DEPTH();

	edata->sqlerrcode = sqlerrcode;

	return 0;					/* return value does not matter */
}


/*
 * errcode_for_file_access --- add SQLSTATE error code to the current error
 *
 * The SQLSTATE code is chosen based on the saved errno value.  We assume
 * that the failing operation was some type of disk file access.
 *
 * NOTE: the primary error message string should generally include %m
 * when this is used.
 */
int
errcode_for_file_access(void)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];

	/* we don't bother incrementing recursion_depth */
	CHECK_STACK_DEPTH();

	switch (edata->saved_errno)
	{
			/* Permission-denied failures */
		case EPERM:				/* Not super-user */
		case EACCES:			/* Permission denied */
#ifdef EROFS
		case EROFS:				/* Read only file system */
#endif
			edata->sqlerrcode = ERRCODE_INSUFFICIENT_PRIVILEGE;
			break;

			/* File not found */
		case ENOENT:			/* No such file or directory */
			edata->sqlerrcode = ERRCODE_UNDEFINED_FILE;
			break;

			/* Duplicate file */
		case EEXIST:			/* File exists */
			edata->sqlerrcode = ERRCODE_DUPLICATE_FILE;
			break;

			/* Wrong object type or state */
		case ENOTDIR:			/* Not a directory */
		case EISDIR:			/* Is a directory */
#if defined(ENOTEMPTY) && (ENOTEMPTY != EEXIST) /* same code on AIX */
		case ENOTEMPTY:			/* Directory not empty */
#endif
			edata->sqlerrcode = ERRCODE_WRONG_OBJECT_TYPE;
			break;

			/* Insufficient resources */
		case ENOSPC:			/* No space left on device */
			edata->sqlerrcode = ERRCODE_DISK_FULL;
			break;

		case ENFILE:			/* File table overflow */
		case EMFILE:			/* Too many open files */
			edata->sqlerrcode = ERRCODE_INSUFFICIENT_RESOURCES;
			break;

			/* Hardware failure */
		case EIO:				/* I/O error */
			edata->sqlerrcode = ERRCODE_IO_ERROR;
			break;

			/* All else is classified as internal errors */
		default:
			edata->sqlerrcode = ERRCODE_INTERNAL_ERROR;
			break;
	}

	return 0;					/* return value does not matter */
}

/*
 * errcode_for_socket_access --- add SQLSTATE error code to the current error
 *
 * The SQLSTATE code is chosen based on the saved errno value.  We assume
 * that the failing operation was some type of socket access.
 *
 * NOTE: the primary error message string should generally include %m
 * when this is used.
 */
int
errcode_for_socket_access(void)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];

	/* we don't bother incrementing recursion_depth */
	CHECK_STACK_DEPTH();

	switch (edata->saved_errno)
	{
			/* Loss of connection */
		case ALL_CONNECTION_FAILURE_ERRNOS:
			edata->sqlerrcode = ERRCODE_CONNECTION_FAILURE;
			break;

			/* All else is classified as internal errors */
		default:
			edata->sqlerrcode = ERRCODE_INTERNAL_ERROR;
			break;
	}

	return 0;					/* return value does not matter */
}


/*
 * This macro handles expansion of a format string and associated parameters;
 * it's common code for errmsg(), errdetail(), etc.  Must be called inside
 * a routine that is declared like "const char *fmt, ..." and has an edata
 * pointer set up.  The message is assigned to edata->targetfield, or
 * appended to it if appendval is true.  The message is subject to translation
 * if translateit is true.
 *
 * Note: we pstrdup the buffer rather than just transferring its storage
 * to the edata field because the buffer might be considerably larger than
 * really necessary.
 */
#define EVALUATE_MESSAGE(domain, targetfield, appendval, translateit)	\
	{ \
		StringInfoData	buf; \
		/* Internationalize the error format string */ \
		if ((translateit) && !in_error_recursion_trouble()) \
			fmt = dgettext((domain), fmt);				  \
		initStringInfo(&buf); \
		if ((appendval) && edata->targetfield) { \
			appendStringInfoString(&buf, edata->targetfield); \
			appendStringInfoChar(&buf, '\n'); \
		} \
		/* Generate actual output --- have to use appendStringInfoVA */ \
		for (;;) \
		{ \
			va_list		args; \
			int			needed; \
			errno = edata->saved_errno; \
			va_start(args, fmt); \
			needed = appendStringInfoVA(&buf, fmt, args); \
			va_end(args); \
			if (needed == 0) \
				break; \
			enlargeStringInfo(&buf, needed); \
		} \
		/* Save the completed message into the stack item */ \
		if (edata->targetfield) \
			pfree(edata->targetfield); \
		edata->targetfield = pstrdup(buf.data); \
		pfree(buf.data); \
	}

/*
 * Same as above, except for pluralized error messages.  The calling routine
 * must be declared like "const char *fmt_singular, const char *fmt_plural,
 * unsigned long n, ...".  Translation is assumed always wanted.
 */
#define EVALUATE_MESSAGE_PLURAL(domain, targetfield, appendval)  \
	{ \
		const char	   *fmt; \
		StringInfoData	buf; \
		/* Internationalize the error format string */ \
		if (!in_error_recursion_trouble()) \
			fmt = dngettext((domain), fmt_singular, fmt_plural, n); \
		else \
			fmt = (n == 1 ? fmt_singular : fmt_plural); \
		initStringInfo(&buf); \
		if ((appendval) && edata->targetfield) { \
			appendStringInfoString(&buf, edata->targetfield); \
			appendStringInfoChar(&buf, '\n'); \
		} \
		/* Generate actual output --- have to use appendStringInfoVA */ \
		for (;;) \
		{ \
			va_list		args; \
			int			needed; \
			errno = edata->saved_errno; \
			va_start(args, n); \
			needed = appendStringInfoVA(&buf, fmt, args); \
			va_end(args); \
			if (needed == 0) \
				break; \
			enlargeStringInfo(&buf, needed); \
		} \
		/* Save the completed message into the stack item */ \
		if (edata->targetfield) \
			pfree(edata->targetfield); \
		edata->targetfield = pstrdup(buf.data); \
		pfree(buf.data); \
	}


/*
 * errmsg --- add a primary error message text to the current error
 *
 * In addition to the usual %-escapes recognized by printf, "%m" in
 * fmt is replaced by the error message for the caller's value of errno.
 *
 * Note: no newline is needed at the end of the fmt string, since
 * ereport will provide one for the output methods that need it.
 */
int
errmsg(const char *fmt,...)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];
	MemoryContext oldcontext;

	recursion_depth++;
	CHECK_STACK_DEPTH();
	oldcontext = MemoryContextSwitchTo(edata->assoc_context);

	edata->message_id = fmt;
	EVALUATE_MESSAGE(edata->domain, message, false, true);

	MemoryContextSwitchTo(oldcontext);
	recursion_depth--;
	return 0;					/* return value does not matter */
}

/*
 * Add a backtrace to the containing ereport() call.  This is intended to be
 * added temporarily during debugging.
 */
int
errbacktrace(void)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];
	MemoryContext oldcontext;

	recursion_depth++;
	CHECK_STACK_DEPTH();
	oldcontext = MemoryContextSwitchTo(edata->assoc_context);

	set_backtrace(edata, 1);

	MemoryContextSwitchTo(oldcontext);
	recursion_depth--;

	return 0;
}

/*
 * Compute backtrace data and add it to the supplied ErrorData.  num_skip
 * specifies how many inner frames to skip.  Use this to avoid showing the
 * internal backtrace support functions in the backtrace.  This requires that
 * this and related functions are not inlined.
 */
static void
set_backtrace(ErrorData *edata, int num_skip)
{
	StringInfoData errtrace;

	initStringInfo(&errtrace);

#ifdef HAVE_BACKTRACE_SYMBOLS
	{
		void	   *buf[100];
		int			nframes;
		char	  **strfrms;

		nframes = backtrace(buf, lengthof(buf));
		strfrms = backtrace_symbols(buf, nframes);
		if (strfrms == NULL)
			return;

		for (int i = num_skip; i < nframes; i++)
			appendStringInfo(&errtrace, "\n%s", strfrms[i]);
		free(strfrms);
	}
#else
	appendStringInfoString(&errtrace,
						   "backtrace generation is not supported by this installation");
#endif

	edata->backtrace = errtrace.data;
}

/*
 * errmsg_internal --- add a primary error message text to the current error
 *
 * This is exactly like errmsg() except that strings passed to errmsg_internal
 * are not translated, and are customarily left out of the
 * internationalization message dictionary.  This should be used for "can't
 * happen" cases that are probably not worth spending translation effort on.
 * We also use this for certain cases where we *must* not try to translate
 * the message because the translation would fail and result in infinite
 * error recursion.
 */
int
errmsg_internal(const char *fmt,...)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];
	MemoryContext oldcontext;

	recursion_depth++;
	CHECK_STACK_DEPTH();
	oldcontext = MemoryContextSwitchTo(edata->assoc_context);

	edata->message_id = fmt;
	EVALUATE_MESSAGE(edata->domain, message, false, false);

	MemoryContextSwitchTo(oldcontext);
	recursion_depth--;
	return 0;					/* return value does not matter */
}


/*
 * errmsg_plural --- add a primary error message text to the current error,
 * with support for pluralization of the message text
 */
int
errmsg_plural(const char *fmt_singular, const char *fmt_plural,
			  unsigned long n,...)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];
	MemoryContext oldcontext;

	recursion_depth++;
	CHECK_STACK_DEPTH();
	oldcontext = MemoryContextSwitchTo(edata->assoc_context);

	edata->message_id = fmt_singular;
	EVALUATE_MESSAGE_PLURAL(edata->domain, message, false);

	MemoryContextSwitchTo(oldcontext);
	recursion_depth--;
	return 0;					/* return value does not matter */
}


/*
 * errdetail --- add a detail error message text to the current error
 */
int
errdetail(const char *fmt,...)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];
	MemoryContext oldcontext;

	recursion_depth++;
	CHECK_STACK_DEPTH();
	oldcontext = MemoryContextSwitchTo(edata->assoc_context);

	EVALUATE_MESSAGE(edata->domain, detail, false, true);

	MemoryContextSwitchTo(oldcontext);
	recursion_depth--;
	return 0;					/* return value does not matter */
}


/*
 * errdetail_internal --- add a detail error message text to the current error
 *
 * This is exactly like errdetail() except that strings passed to
 * errdetail_internal are not translated, and are customarily left out of the
 * internationalization message dictionary.  This should be used for detail
 * messages that seem not worth translating for one reason or another
 * (typically, that they don't seem to be useful to average users).
 */
int
errdetail_internal(const char *fmt,...)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];
	MemoryContext oldcontext;

	recursion_depth++;
	CHECK_STACK_DEPTH();
	oldcontext = MemoryContextSwitchTo(edata->assoc_context);

	EVALUATE_MESSAGE(edata->domain, detail, false, false);

	MemoryContextSwitchTo(oldcontext);
	recursion_depth--;
	return 0;					/* return value does not matter */
}


/*
 * errdetail_log --- add a detail_log error message text to the current error
 */
int
errdetail_log(const char *fmt,...)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];
	MemoryContext oldcontext;

	recursion_depth++;
	CHECK_STACK_DEPTH();
	oldcontext = MemoryContextSwitchTo(edata->assoc_context);

	EVALUATE_MESSAGE(edata->domain, detail_log, false, true);

	MemoryContextSwitchTo(oldcontext);
	recursion_depth--;
	return 0;					/* return value does not matter */
}

/*
 * errdetail_log_plural --- add a detail_log error message text to the current error
 * with support for pluralization of the message text
 */
int
errdetail_log_plural(const char *fmt_singular, const char *fmt_plural,
					 unsigned long n,...)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];
	MemoryContext oldcontext;

	recursion_depth++;
	CHECK_STACK_DEPTH();
	oldcontext = MemoryContextSwitchTo(edata->assoc_context);

	EVALUATE_MESSAGE_PLURAL(edata->domain, detail_log, false);

	MemoryContextSwitchTo(oldcontext);
	recursion_depth--;
	return 0;					/* return value does not matter */
}


/*
 * errdetail_plural --- add a detail error message text to the current error,
 * with support for pluralization of the message text
 */
int
errdetail_plural(const char *fmt_singular, const char *fmt_plural,
				 unsigned long n,...)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];
	MemoryContext oldcontext;

	recursion_depth++;
	CHECK_STACK_DEPTH();
	oldcontext = MemoryContextSwitchTo(edata->assoc_context);

	EVALUATE_MESSAGE_PLURAL(edata->domain, detail, false);

	MemoryContextSwitchTo(oldcontext);
	recursion_depth--;
	return 0;					/* return value does not matter */
}


/*
 * errhint --- add a hint error message text to the current error
 */
int
errhint(const char *fmt,...)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];
	MemoryContext oldcontext;

	recursion_depth++;
	CHECK_STACK_DEPTH();
	oldcontext = MemoryContextSwitchTo(edata->assoc_context);

	EVALUATE_MESSAGE(edata->domain, hint, false, true);

	MemoryContextSwitchTo(oldcontext);
	recursion_depth--;
	return 0;					/* return value does not matter */
}


/*
 * errhint_plural --- add a hint error message text to the current error,
 * with support for pluralization of the message text
 */
int
errhint_plural(const char *fmt_singular, const char *fmt_plural,
			   unsigned long n,...)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];
	MemoryContext oldcontext;

	recursion_depth++;
	CHECK_STACK_DEPTH();
	oldcontext = MemoryContextSwitchTo(edata->assoc_context);

	EVALUATE_MESSAGE_PLURAL(edata->domain, hint, false);

	MemoryContextSwitchTo(oldcontext);
	recursion_depth--;
	return 0;					/* return value does not matter */
}


/*
 * errcontext_msg --- add a context error message text to the current error
 *
 * Unlike other cases, multiple calls are allowed to build up a stack of
 * context information.  We assume earlier calls represent more-closely-nested
 * states.
 */
int
errcontext_msg(const char *fmt,...)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];
	MemoryContext oldcontext;

	recursion_depth++;
	CHECK_STACK_DEPTH();
	oldcontext = MemoryContextSwitchTo(edata->assoc_context);

	EVALUATE_MESSAGE(edata->context_domain, context, true, true);

	MemoryContextSwitchTo(oldcontext);
	recursion_depth--;
	return 0;					/* return value does not matter */
}

/*
 * set_errcontext_domain --- set message domain to be used by errcontext()
 *
 * errcontext_msg() can be called from a different module than the original
 * ereport(), so we cannot use the message domain passed in errstart() to
 * translate it.  Instead, each errcontext_msg() call should be preceded by
 * a set_errcontext_domain() call to specify the domain.  This is usually
 * done transparently by the errcontext() macro.
 */
int
set_errcontext_domain(const char *domain)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];

	/* we don't bother incrementing recursion_depth */
	CHECK_STACK_DEPTH();

	/* the default text domain is the backend's */
	edata->context_domain = domain ? domain : PG_TEXTDOMAIN("postgres");

	return 0;					/* return value does not matter */
}


/*
 * errhidestmt --- optionally suppress STATEMENT: field of log entry
 *
 * This should be called if the message text already includes the statement.
 */
int
errhidestmt(bool hide_stmt)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];

	/* we don't bother incrementing recursion_depth */
	CHECK_STACK_DEPTH();

	edata->hide_stmt = hide_stmt;

	return 0;					/* return value does not matter */
}

/*
 * errhidecontext --- optionally suppress CONTEXT: field of log entry
 *
 * This should only be used for verbose debugging messages where the repeated
 * inclusion of context would bloat the log volume too much.
 */
int
errhidecontext(bool hide_ctx)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];

	/* we don't bother incrementing recursion_depth */
	CHECK_STACK_DEPTH();

	edata->hide_ctx = hide_ctx;

	return 0;					/* return value does not matter */
}

/*
 * errposition --- add cursor position to the current error
 */
int
errposition(int cursorpos)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];

	/* we don't bother incrementing recursion_depth */
	CHECK_STACK_DEPTH();

	edata->cursorpos = cursorpos;

	return 0;					/* return value does not matter */
}

/*
 * internalerrposition --- add internal cursor position to the current error
 */
int
internalerrposition(int cursorpos)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];

	/* we don't bother incrementing recursion_depth */
	CHECK_STACK_DEPTH();

	edata->internalpos = cursorpos;

	return 0;					/* return value does not matter */
}

/*
 * internalerrquery --- add internal query text to the current error
 *
 * Can also pass NULL to drop the internal query text entry.  This case
 * is intended for use in error callback subroutines that are editorializing
 * on the layout of the error report.
 */
int
internalerrquery(const char *query)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];

	/* we don't bother incrementing recursion_depth */
	CHECK_STACK_DEPTH();

	if (edata->internalquery)
	{
		pfree(edata->internalquery);
		edata->internalquery = NULL;
	}

	if (query)
		edata->internalquery = MemoryContextStrdup(edata->assoc_context, query);

	return 0;					/* return value does not matter */
}

/*
 * err_generic_string -- used to set individual ErrorData string fields
 * identified by PG_DIAG_xxx codes.
 *
 * This intentionally only supports fields that don't use localized strings,
 * so that there are no translation considerations.
 *
 * Most potential callers should not use this directly, but instead prefer
 * higher-level abstractions, such as errtablecol() (see relcache.c).
 */
int
err_generic_string(int field, const char *str)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];

	/* we don't bother incrementing recursion_depth */
	CHECK_STACK_DEPTH();

	switch (field)
	{
		case PG_DIAG_SCHEMA_NAME:
			set_errdata_field(edata->assoc_context, &edata->schema_name, str);
			break;
		case PG_DIAG_TABLE_NAME:
			set_errdata_field(edata->assoc_context, &edata->table_name, str);
			break;
		case PG_DIAG_COLUMN_NAME:
			set_errdata_field(edata->assoc_context, &edata->column_name, str);
			break;
		case PG_DIAG_DATATYPE_NAME:
			set_errdata_field(edata->assoc_context, &edata->datatype_name, str);
			break;
		case PG_DIAG_CONSTRAINT_NAME:
			set_errdata_field(edata->assoc_context, &edata->constraint_name, str);
			break;
		default:
			elog(ERROR, "unsupported ErrorData field id: %d", field);
			break;
	}

	return 0;					/* return value does not matter */
}

/*
 * set_errdata_field --- set an ErrorData string field
 */
static void
set_errdata_field(MemoryContextData *cxt, char **ptr, const char *str)
{
	Assert(*ptr == NULL);
	*ptr = MemoryContextStrdup(cxt, str);
}

/*
 * geterrcode --- return the currently set SQLSTATE error code
 *
 * This is only intended for use in error callback subroutines, since there
 * is no other place outside elog.c where the concept is meaningful.
 */
int
geterrcode(void)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];

	/* we don't bother incrementing recursion_depth */
	CHECK_STACK_DEPTH();

	return edata->sqlerrcode;
}

/*
 * geterrposition --- return the currently set error position (0 if none)
 *
 * This is only intended for use in error callback subroutines, since there
 * is no other place outside elog.c where the concept is meaningful.
 */
int
geterrposition(void)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];

	/* we don't bother incrementing recursion_depth */
	CHECK_STACK_DEPTH();

	return edata->cursorpos;
}

/*
 * getinternalerrposition --- same for internal error position
 *
 * This is only intended for use in error callback subroutines, since there
 * is no other place outside elog.c where the concept is meaningful.
 */
int
getinternalerrposition(void)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];

	/* we don't bother incrementing recursion_depth */
	CHECK_STACK_DEPTH();

	return edata->internalpos;
}


/*
 * Functions to allow construction of error message strings separately from
 * the ereport() call itself.
 *
 * The expected calling convention is
 *
 *	pre_format_elog_string(errno, domain), var = format_elog_string(format,...)
 *
 * which can be hidden behind a macro such as GUC_check_errdetail().  We
 * assume that any functions called in the arguments of format_elog_string()
 * cannot result in re-entrant use of these functions --- otherwise the wrong
 * text domain might be used, or the wrong errno substituted for %m.  This is
 * okay for the current usage with GUC check hooks, but might need further
 * effort someday.
 *
 * The result of format_elog_string() is stored in ErrorContext, and will
 * therefore survive until FlushErrorState() is called.
 */
static int	save_format_errnumber;
static const char *save_format_domain;

void
pre_format_elog_string(int errnumber, const char *domain)
{
	/* Save errno before evaluation of argument functions can change it */
	save_format_errnumber = errnumber;
	/* Save caller's text domain */
	save_format_domain = domain;
}

char *
format_elog_string(const char *fmt,...)
{
	ErrorData	errdata;
	ErrorData  *edata;
	MemoryContext oldcontext;

	/* Initialize a mostly-dummy error frame */
	edata = &errdata;
	MemSet(edata, 0, sizeof(ErrorData));
	/* the default text domain is the backend's */
	edata->domain = save_format_domain ? save_format_domain : PG_TEXTDOMAIN("postgres");
	/* set the errno to be used to interpret %m */
	edata->saved_errno = save_format_errnumber;

	oldcontext = MemoryContextSwitchTo(ErrorContext);

	edata->message_id = fmt;
	EVALUATE_MESSAGE(edata->domain, message, false, true);

	MemoryContextSwitchTo(oldcontext);

	return edata->message;
}


/*
 * Actual output of the top-of-stack error message
 *
 * In the ereport(ERROR) case this is called from PostgresMain (or not at all,
 * if the error is caught by somebody).  For all other severity levels this
 * is called by errfinish.
 */
void
EmitErrorReport(void)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];
	MemoryContext oldcontext;

	recursion_depth++;
	CHECK_STACK_DEPTH();
	oldcontext = MemoryContextSwitchTo(edata->assoc_context);

	/*
	 * Call hook before sending message to log.  The hook function is allowed
	 * to turn off edata->output_to_server, so we must recheck that afterward.
	 * Making any other change in the content of edata is not considered
	 * supported.
	 *
	 * Note: the reason why the hook can only turn off output_to_server, and
	 * not turn it on, is that it'd be unreliable: we will never get here at
	 * all if errstart() deems the message uninteresting.  A hook that could
	 * make decisions in that direction would have to hook into errstart(),
	 * where it would have much less information available.  emit_log_hook is
	 * intended for custom log filtering and custom log message transmission
	 * mechanisms.
	 *
	 * The log hook has access to both the translated and original English
	 * error message text, which is passed through to allow it to be used as a
	 * message identifier. Note that the original text is not available for
	 * detail, detail_log, hint and context text elements.
	 */
	if (edata->output_to_server && emit_log_hook)
		(*emit_log_hook) (edata);

	/* Send to server log, if enabled */
	if (edata->output_to_server)
		send_message_to_server_log(edata);

	/* Send to client, if enabled */
	if (edata->output_to_client)
		send_message_to_frontend(edata);

	MemoryContextSwitchTo(oldcontext);
	recursion_depth--;
}

/*
 * CopyErrorData --- obtain a copy of the topmost error stack entry
 *
 * This is only for use in error handler code.  The data is copied into the
 * current memory context, so callers should always switch away from
 * ErrorContext first; otherwise it will be lost when FlushErrorState is done.
 */
ErrorData *
CopyErrorData(void)
{
	ErrorData  *edata = &errordata[errordata_stack_depth];
	ErrorData  *newedata;

	/*
	 * we don't increment recursion_depth because out-of-memory here does not
	 * indicate a problem within the error subsystem.
	 */
	CHECK_STACK_DEPTH();

	Assert(CurrentMemoryContext != ErrorContext);

	/* Copy the struct itself */
	newedata = (ErrorData *) palloc(sizeof(ErrorData));
	memcpy(newedata, edata, sizeof(ErrorData));

	/* Make copies of separately-allocated fields */
	if (newedata->message)
		newedata->message = pstrdup(newedata->message);
	if (newedata->detail)
		newedata->detail = pstrdup(newedata->detail);
	if (newedata->detail_log)
		newedata->detail_log = pstrdup(newedata->detail_log);
	if (newedata->hint)
		newedata->hint = pstrdup(newedata->hint);
	if (newedata->context)
		newedata->context = pstrdup(newedata->context);
	if (newedata->backtrace)
		newedata->backtrace = pstrdup(newedata->backtrace);
	if (newedata->schema_name)
		newedata->schema_name = pstrdup(newedata->schema_name);
	if (newedata->table_name)
		newedata->table_name = pstrdup(newedata->table_name);
	if (newedata->column_name)
		newedata->column_name = pstrdup(newedata->column_name);
	if (newedata->datatype_name)
		newedata->datatype_name = pstrdup(newedata->datatype_name);
	if (newedata->constraint_name)
		newedata->constraint_name = pstrdup(newedata->constraint_name);
	if (newedata->internalquery)
		newedata->internalquery = pstrdup(newedata->internalquery);

	/* Use the calling context for string allocation */
	newedata->assoc_context = CurrentMemoryContext;

	return newedata;
}

/*
 * FreeErrorData --- free the structure returned by CopyErrorData.
 *
 * Error handlers should use this in preference to assuming they know all
 * the separately-allocated fields.
 */
void
FreeErrorData(ErrorData *edata)
{
	if (edata->message)
		pfree(edata->message);
	if (edata->detail)
		pfree(edata->detail);
	if (edata->detail_log)
		pfree(edata->detail_log);
	if (edata->hint)
		pfree(edata->hint);
	if (edata->context)
		pfree(edata->context);
	if (edata->backtrace)
		pfree(edata->backtrace);
	if (edata->schema_name)
		pfree(edata->schema_name);
	if (edata->table_name)
		pfree(edata->table_name);
	if (edata->column_name)
		pfree(edata->column_name);
	if (edata->datatype_name)
		pfree(edata->datatype_name);
	if (edata->constraint_name)
		pfree(edata->constraint_name);
	if (edata->internalquery)
		pfree(edata->internalquery);
	pfree(edata);
}

/*
 * FlushErrorState --- flush the error state after error recovery
 *
 * This should be called by an error handler after it's done processing
 * the error; or as soon as it's done CopyErrorData, if it intends to
 * do stuff that is likely to provoke another error.  You are not "out" of
 * the error subsystem until you have done this.
 */
void
FlushErrorState(void)
{
	/*
	 * Reset stack to empty.  The only case where it would be more than one
	 * deep is if we serviced an error that interrupted construction of
	 * another message.  We assume control escaped out of that message
	 * construction and won't ever go back.
	 */
	errordata_stack_depth = -1;
	recursion_depth = 0;
	/* Delete all data in ErrorContext */
	MemoryContextResetAndDeleteChildren(ErrorContext);
}

/*
 * ThrowErrorData --- report an error described by an ErrorData structure
 *
 * This is somewhat like ReThrowError, but it allows elevels besides ERROR,
 * and the boolean flags such as output_to_server are computed via the
 * default rules rather than being copied from the given ErrorData.
 * This is primarily used to re-report errors originally reported by
 * background worker processes and then propagated (with or without
 * modification) to the backend responsible for them.
 */
void
ThrowErrorData(ErrorData *edata)
{
	ErrorData  *newedata;
	MemoryContext oldcontext;

	if (!errstart(edata->elevel, edata->domain))
		return;					/* error is not to be reported at all */

	newedata = &errordata[errordata_stack_depth];
	recursion_depth++;
	oldcontext = MemoryContextSwitchTo(newedata->assoc_context);

	/* Copy the supplied fields to the error stack entry. */
	if (edata->sqlerrcode != 0)
		newedata->sqlerrcode = edata->sqlerrcode;
	if (edata->message)
		newedata->message = pstrdup(edata->message);
	if (edata->detail)
		newedata->detail = pstrdup(edata->detail);
	if (edata->detail_log)
		newedata->detail_log = pstrdup(edata->detail_log);
	if (edata->hint)
		newedata->hint = pstrdup(edata->hint);
	if (edata->context)
		newedata->context = pstrdup(edata->context);
	if (edata->backtrace)
		newedata->backtrace = pstrdup(edata->backtrace);
	/* assume message_id is not available */
	if (edata->schema_name)
		newedata->schema_name = pstrdup(edata->schema_name);
	if (edata->table_name)
		newedata->table_name = pstrdup(edata->table_name);
	if (edata->column_name)
		newedata->column_name = pstrdup(edata->column_name);
	if (edata->datatype_name)
		newedata->datatype_name = pstrdup(edata->datatype_name);
	if (edata->constraint_name)
		newedata->constraint_name = pstrdup(edata->constraint_name);
	newedata->cursorpos = edata->cursorpos;
	newedata->internalpos = edata->internalpos;
	if (edata->internalquery)
		newedata->internalquery = pstrdup(edata->internalquery);

	MemoryContextSwitchTo(oldcontext);
	recursion_depth--;

	/* Process the error. */
	errfinish(edata->filename, edata->lineno, edata->funcname);
}

/*
 * ReThrowError --- re-throw a previously copied error
 *
 * A handler can do CopyErrorData/FlushErrorState to get out of the error
 * subsystem, then do some processing, and finally ReThrowError to re-throw
 * the original error.  This is slower than just PG_RE_THROW() but should
 * be used if the "some processing" is likely to incur another error.
 */
void
ReThrowError(ErrorData *edata)
{
	ErrorData  *newedata;

	Assert(edata->elevel == ERROR);

	/* Push the data back into the error context */
	recursion_depth++;
	MemoryContextSwitchTo(ErrorContext);

	if (++errordata_stack_depth >= ERRORDATA_STACK_SIZE)
	{
		/*
		 * Wups, stack not big enough.  We treat this as a PANIC condition
		 * because it suggests an infinite loop of errors during error
		 * recovery.
		 */
		errordata_stack_depth = -1; /* make room on stack */
		ereport(PANIC, (errmsg_internal("ERRORDATA_STACK_SIZE exceeded")));
	}

	newedata = &errordata[errordata_stack_depth];
	memcpy(newedata, edata, sizeof(ErrorData));

	/* Make copies of separately-allocated fields */
	if (newedata->message)
		newedata->message = pstrdup(newedata->message);
	if (newedata->detail)
		newedata->detail = pstrdup(newedata->detail);
	if (newedata->detail_log)
		newedata->detail_log = pstrdup(newedata->detail_log);
	if (newedata->hint)
		newedata->hint = pstrdup(newedata->hint);
	if (newedata->context)
		newedata->context = pstrdup(newedata->context);
	if (newedata->backtrace)
		newedata->backtrace = pstrdup(newedata->backtrace);
	if (newedata->schema_name)
		newedata->schema_name = pstrdup(newedata->schema_name);
	if (newedata->table_name)
		newedata->table_name = pstrdup(newedata->table_name);
	if (newedata->column_name)
		newedata->column_name = pstrdup(newedata->column_name);
	if (newedata->datatype_name)
		newedata->datatype_name = pstrdup(newedata->datatype_name);
	if (newedata->constraint_name)
		newedata->constraint_name = pstrdup(newedata->constraint_name);
	if (newedata->internalquery)
		newedata->internalquery = pstrdup(newedata->internalquery);

	/* Reset the assoc_context to be ErrorContext */
	newedata->assoc_context = ErrorContext;

	recursion_depth--;
	PG_RE_THROW();
}

/*
 * pg_re_throw --- out-of-line implementation of PG_RE_THROW() macro
 */
void
pg_re_throw(void)
{
	/* If possible, throw the error to the next outer setjmp handler */
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	else
	{
		/*
		 * If we get here, elog(ERROR) was thrown inside a PG_TRY block, which
		 * we have now exited only to discover that there is no outer setjmp
		 * handler to pass the error to.  Had the error been thrown outside
		 * the block to begin with, we'd have promoted the error to FATAL, so
		 * the correct behavior is to make it FATAL now; that is, emit it and
		 * then call proc_exit.
		 */
		ErrorData  *edata = &errordata[errordata_stack_depth];

		Assert(errordata_stack_depth >= 0);
		Assert(edata->elevel == ERROR);
		edata->elevel = FATAL;

		/*
		 * At least in principle, the increase in severity could have changed
		 * where-to-output decisions, so recalculate.
		 */
		edata->output_to_server = should_output_to_server(FATAL);
		edata->output_to_client = should_output_to_client(FATAL);

		/*
		 * We can use errfinish() for the rest, but we don't want it to call
		 * any error context routines a second time.  Since we know we are
		 * about to exit, it should be OK to just clear the context stack.
		 */
		error_context_stack = NULL;

		errfinish(edata->filename, edata->lineno, edata->funcname);
	}

	/* Doesn't return ... */
	ExceptionalCondition("pg_re_throw tried to return", "FailedAssertion",
						 __FILE__, __LINE__);
}


/*
 * GetErrorContextStack - Return the context stack, for display/diags
 *
 * Returns a pstrdup'd string in the caller's context which includes the PG
 * error call stack.  It is the caller's responsibility to ensure this string
 * is pfree'd (or its context cleaned up) when done.
 *
 * This information is collected by traversing the error contexts and calling
 * each context's callback function, each of which is expected to call
 * errcontext() to return a string which can be presented to the user.
 */
char *
GetErrorContextStack(void)
{
	ErrorData  *edata;
	ErrorContextCallback *econtext;

	/*
	 * Okay, crank up a stack entry to store the info in.
	 */
	recursion_depth++;

	if (++errordata_stack_depth >= ERRORDATA_STACK_SIZE)
	{
		/*
		 * Wups, stack not big enough.  We treat this as a PANIC condition
		 * because it suggests an infinite loop of errors during error
		 * recovery.
		 */
		errordata_stack_depth = -1; /* make room on stack */
		ereport(PANIC, (errmsg_internal("ERRORDATA_STACK_SIZE exceeded")));
	}

	/*
	 * Things look good so far, so initialize our error frame
	 */
	edata = &errordata[errordata_stack_depth];
	MemSet(edata, 0, sizeof(ErrorData));

	/*
	 * Set up assoc_context to be the caller's context, so any allocations
	 * done (which will include edata->context) will use their context.
	 */
	edata->assoc_context = CurrentMemoryContext;

	/*
	 * Call any context callback functions to collect the context information
	 * into edata->context.
	 *
	 * Errors occurring in callback functions should go through the regular
	 * error handling code which should handle any recursive errors, though we
	 * double-check above, just in case.
	 */
	for (econtext = error_context_stack;
		 econtext != NULL;
		 econtext = econtext->previous)
		econtext->callback(econtext->arg);

	/*
	 * Clean ourselves off the stack, any allocations done should have been
	 * using edata->assoc_context, which we set up earlier to be the caller's
	 * context, so we're free to just remove our entry off the stack and
	 * decrement recursion depth and exit.
	 */
	errordata_stack_depth--;
	recursion_depth--;

	/*
	 * Return a pointer to the string the caller asked for, which should have
	 * been allocated in their context.
	 */
	return edata->context;
}


/*
 * Initialization of error output file
 */
void
DebugFileOpen(void)
{
	int			fd,
				istty;

	if (OutputFileName[0])
	{
		/*
		 * A debug-output file name was given.
		 *
		 * Make sure we can write the file, and find out if it's a tty.
		 */
		if ((fd = open(OutputFileName, O_CREAT | O_APPEND | O_WRONLY,
					   0666)) < 0)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\": %m", OutputFileName)));
		istty = isatty(fd);
		close(fd);

		/*
		 * Redirect our stderr to the debug output file.
		 */
		if (!freopen(OutputFileName, "a", stderr))
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not reopen file \"%s\" as stderr: %m",
							OutputFileName)));

		/*
		 * If the file is a tty and we're running under the postmaster, try to
		 * send stdout there as well (if it isn't a tty then stderr will block
		 * out stdout, so we may as well let stdout go wherever it was going
		 * before).
		 */
		if (istty && IsUnderPostmaster)
			if (!freopen(OutputFileName, "a", stdout))
				ereport(FATAL,
						(errcode_for_file_access(),
						 errmsg("could not reopen file \"%s\" as stdout: %m",
								OutputFileName)));
	}
}


#ifdef HAVE_SYSLOG

/*
 * Set or update the parameters for syslog logging
 */
void
set_syslog_parameters(const char *ident, int facility)
{
	/*
	 * guc.c is likely to call us repeatedly with same parameters, so don't
	 * thrash the syslog connection unnecessarily.  Also, we do not re-open
	 * the connection until needed, since this routine will get called whether
	 * or not Log_destination actually mentions syslog.
	 *
	 * Note that we make our own copy of the ident string rather than relying
	 * on guc.c's.  This may be overly paranoid, but it ensures that we cannot
	 * accidentally free a string that syslog is still using.
	 */
	if (syslog_ident == NULL || strcmp(syslog_ident, ident) != 0 ||
		syslog_facility != facility)
	{
		if (openlog_done)
		{
			closelog();
			openlog_done = false;
		}
		if (syslog_ident)
			free(syslog_ident);
		syslog_ident = strdup(ident);
		/* if the strdup fails, we will cope in write_syslog() */
		syslog_facility = facility;
	}
}


/*
 * Write a message line to syslog
 */
static void
write_syslog(int level, const char *line)
{
	static unsigned long seq = 0;

	int			len;
	const char *nlpos;

	/* Open syslog connection if not done yet */
	if (!openlog_done)
	{
		openlog(syslog_ident ? syslog_ident : "postgres",
				LOG_PID | LOG_NDELAY | LOG_NOWAIT,
				syslog_facility);
		openlog_done = true;
	}

	/*
	 * We add a sequence number to each log message to suppress "same"
	 * messages.
	 */
	seq++;

	/*
	 * Our problem here is that many syslog implementations don't handle long
	 * messages in an acceptable manner. While this function doesn't help that
	 * fact, it does work around by splitting up messages into smaller pieces.
	 *
	 * We divide into multiple syslog() calls if message is too long or if the
	 * message contains embedded newline(s).
	 */
	len = strlen(line);
	nlpos = strchr(line, '\n');
	if (syslog_split_messages && (len > PG_SYSLOG_LIMIT || nlpos != NULL))
	{
		int			chunk_nr = 0;

		while (len > 0)
		{
			char		buf[PG_SYSLOG_LIMIT + 1];
			int			buflen;
			int			i;

			/* if we start at a newline, move ahead one char */
			if (line[0] == '\n')
			{
				line++;
				len--;
				/* we need to recompute the next newline's position, too */
				nlpos = strchr(line, '\n');
				continue;
			}

			/* copy one line, or as much as will fit, to buf */
			if (nlpos != NULL)
				buflen = nlpos - line;
			else
				buflen = len;
			buflen = Min(buflen, PG_SYSLOG_LIMIT);
			memcpy(buf, line, buflen);
			buf[buflen] = '\0';

			/* trim to multibyte letter boundary */
			buflen = pg_mbcliplen(buf, buflen, buflen);
			if (buflen <= 0)
				return;
			buf[buflen] = '\0';

			/* already word boundary? */
			if (line[buflen] != '\0' &&
				!isspace((unsigned char) line[buflen]))
			{
				/* try to divide at word boundary */
				i = buflen - 1;
				while (i > 0 && !isspace((unsigned char) buf[i]))
					i--;

				if (i > 0)		/* else couldn't divide word boundary */
				{
					buflen = i;
					buf[i] = '\0';
				}
			}

			chunk_nr++;

			if (syslog_sequence_numbers)
				syslog(level, "[%lu-%d] %s", seq, chunk_nr, buf);
			else
				syslog(level, "[%d] %s", chunk_nr, buf);

			line += buflen;
			len -= buflen;
		}
	}
	else
	{
		/* message short enough */
		if (syslog_sequence_numbers)
			syslog(level, "[%lu] %s", seq, line);
		else
			syslog(level, "%s", line);
	}
}
#endif							/* HAVE_SYSLOG */

#ifdef WIN32
/*
 * Get the PostgreSQL equivalent of the Windows ANSI code page.  "ANSI" system
 * interfaces (e.g. CreateFileA()) expect string arguments in this encoding.
 * Every process in a given system will find the same value at all times.
 */
static int
GetACPEncoding(void)
{
	static int	encoding = -2;

	if (encoding == -2)
		encoding = pg_codepage_to_encoding(GetACP());

	return encoding;
}

/*
 * Write a message line to the windows event log
 */
static void
write_eventlog(int level, const char *line, int len)
{
	WCHAR	   *utf16;
	int			eventlevel = EVENTLOG_ERROR_TYPE;
	static HANDLE evtHandle = INVALID_HANDLE_VALUE;

	if (evtHandle == INVALID_HANDLE_VALUE)
	{
		evtHandle = RegisterEventSource(NULL,
										event_source ? event_source : DEFAULT_EVENT_SOURCE);
		if (evtHandle == NULL)
		{
			evtHandle = INVALID_HANDLE_VALUE;
			return;
		}
	}

	switch (level)
	{
		case DEBUG5:
		case DEBUG4:
		case DEBUG3:
		case DEBUG2:
		case DEBUG1:
		case LOG:
		case LOG_SERVER_ONLY:
		case INFO:
		case NOTICE:
			eventlevel = EVENTLOG_INFORMATION_TYPE;
			break;
		case WARNING:
		case WARNING_CLIENT_ONLY:
			eventlevel = EVENTLOG_WARNING_TYPE;
			break;
		case ERROR:
		case FATAL:
		case PANIC:
		default:
			eventlevel = EVENTLOG_ERROR_TYPE;
			break;
	}

	/*
	 * If message character encoding matches the encoding expected by
	 * ReportEventA(), call it to avoid the hazards of conversion.  Otherwise,
	 * try to convert the message to UTF16 and write it with ReportEventW().
	 * Fall back on ReportEventA() if conversion failed.
	 *
	 * Since we palloc the structure required for conversion, also fall
	 * through to writing unconverted if we have not yet set up
	 * CurrentMemoryContext.
	 *
	 * Also verify that we are not on our way into error recursion trouble due
	 * to error messages thrown deep inside pgwin32_message_to_UTF16().
	 */
	if (!in_error_recursion_trouble() &&
		CurrentMemoryContext != NULL &&
		GetMessageEncoding() != GetACPEncoding())
	{
		utf16 = pgwin32_message_to_UTF16(line, len, NULL);
		if (utf16)
		{
			ReportEventW(evtHandle,
						 eventlevel,
						 0,
						 0,		/* All events are Id 0 */
						 NULL,
						 1,
						 0,
						 (LPCWSTR *) &utf16,
						 NULL);
			/* XXX Try ReportEventA() when ReportEventW() fails? */

			pfree(utf16);
			return;
		}
	}
	ReportEventA(evtHandle,
				 eventlevel,
				 0,
				 0,				/* All events are Id 0 */
				 NULL,
				 1,
				 0,
				 &line,
				 NULL);
}
#endif							/* WIN32 */

static void
write_console(const char *line, int len)
{
	int			rc;

#ifdef WIN32

	/*
	 * Try to convert the message to UTF16 and write it with WriteConsoleW().
	 * Fall back on write() if anything fails.
	 *
	 * In contrast to write_eventlog(), don't skip straight to write() based
	 * on the applicable encodings.  Unlike WriteConsoleW(), write() depends
	 * on the suitability of the console output code page.  Since we put
	 * stderr into binary mode in SubPostmasterMain(), write() skips the
	 * necessary translation anyway.
	 *
	 * WriteConsoleW() will fail if stderr is redirected, so just fall through
	 * to writing unconverted to the logfile in this case.
	 *
	 * Since we palloc the structure required for conversion, also fall
	 * through to writing unconverted if we have not yet set up
	 * CurrentMemoryContext.
	 */
	if (!in_error_recursion_trouble() &&
		!redirection_done &&
		CurrentMemoryContext != NULL)
	{
		WCHAR	   *utf16;
		int			utf16len;

		utf16 = pgwin32_message_to_UTF16(line, len, &utf16len);
		if (utf16 != NULL)
		{
			HANDLE		stdHandle;
			DWORD		written;

			stdHandle = GetStdHandle(STD_ERROR_HANDLE);
			if (WriteConsoleW(stdHandle, utf16, utf16len, &written, NULL))
			{
				pfree(utf16);
				return;
			}

			/*
			 * In case WriteConsoleW() failed, fall back to writing the
			 * message unconverted.
			 */
			pfree(utf16);
		}
	}
#else

	/*
	 * Conversion on non-win32 platforms is not implemented yet. It requires
	 * non-throw version of pg_do_encoding_conversion(), that converts
	 * unconvertable characters to '?' without errors.
	 *
	 * XXX: We have a no-throw version now. It doesn't convert to '?' though.
	 */
#endif

	/*
	 * We ignore any error from write() here.  We have no useful way to report
	 * it ... certainly whining on stderr isn't likely to be productive.
	 */
	rc = write(fileno(stderr), line, len);
	(void) rc;
}

/*
 * setup formatted_log_time, for consistent times between CSV and regular logs
 */
static void
setup_formatted_log_time(void)
{
	pg_time_t	stamp_time;
	char		msbuf[13];

	if (!saved_timeval_set)
	{
		gettimeofday(&saved_timeval, NULL);
		saved_timeval_set = true;
	}

	stamp_time = (pg_time_t) saved_timeval.tv_sec;

	/*
	 * Note: we expect that guc.c will ensure that log_timezone is set up (at
	 * least with a minimal GMT value) before Log_line_prefix can become
	 * nonempty or CSV mode can be selected.
	 */
	pg_strftime(formatted_log_time, FORMATTED_TS_LEN,
	/* leave room for milliseconds... */
				"%Y-%m-%d %H:%M:%S     %Z",
				pg_localtime(&stamp_time, log_timezone));

	/* 'paste' milliseconds into place... */
	sprintf(msbuf, ".%03d", (int) (saved_timeval.tv_usec / 1000));
	memcpy(formatted_log_time + 19, msbuf, 4);
}

/*
 * setup formatted_start_time
 */
static void
setup_formatted_start_time(void)
{
	pg_time_t	stamp_time = (pg_time_t) MyStartTime;

	/*
	 * Note: we expect that guc.c will ensure that log_timezone is set up (at
	 * least with a minimal GMT value) before Log_line_prefix can become
	 * nonempty or CSV mode can be selected.
	 */
	pg_strftime(formatted_start_time, FORMATTED_TS_LEN,
				"%Y-%m-%d %H:%M:%S %Z",
				pg_localtime(&stamp_time, log_timezone));
}

/*
 * process_log_prefix_padding --- helper function for processing the format
 * string in log_line_prefix
 *
 * Note: This function returns NULL if it finds something which
 * it deems invalid in the format string.
 */
static const char *
process_log_prefix_padding(const char *p, int *ppadding)
{
	int			paddingsign = 1;
	int			padding = 0;

	if (*p == '-')
	{
		p++;

		if (*p == '\0')			/* Did the buf end in %- ? */
			return NULL;
		paddingsign = -1;
	}

	/* generate an int version of the numerical string */
	while (*p >= '0' && *p <= '9')
		padding = padding * 10 + (*p++ - '0');

	/* format is invalid if it ends with the padding number */
	if (*p == '\0')
		return NULL;

	padding *= paddingsign;
	*ppadding = padding;
	return p;
}

/*
 * Format tag info for log lines; append to the provided buffer.
 */
static void
log_line_prefix(StringInfo buf, ErrorData *edata)
{
	/* static counter for line numbers */
	static long log_line_number = 0;

	/* has counter been reset in current process? */
	static int	log_my_pid = 0;
	int			padding;
	const char *p;

	/*
	 * This is one of the few places where we'd rather not inherit a static
	 * variable's value from the postmaster.  But since we will, reset it when
	 * MyProcPid changes. MyStartTime also changes when MyProcPid does, so
	 * reset the formatted start timestamp too.
	 */
	if (log_my_pid != MyProcPid)
	{
		log_line_number = 0;
		log_my_pid = MyProcPid;
		formatted_start_time[0] = '\0';
	}
	log_line_number++;

	if (Log_line_prefix == NULL)
		return;					/* in case guc hasn't run yet */

	for (p = Log_line_prefix; *p != '\0'; p++)
	{
		if (*p != '%')
		{
			/* literal char, just copy */
			appendStringInfoChar(buf, *p);
			continue;
		}

		/* must be a '%', so skip to the next char */
		p++;
		if (*p == '\0')
			break;				/* format error - ignore it */
		else if (*p == '%')
		{
			/* string contains %% */
			appendStringInfoChar(buf, '%');
			continue;
		}


		/*
		 * Process any formatting which may exist after the '%'.  Note that
		 * process_log_prefix_padding moves p past the padding number if it
		 * exists.
		 *
		 * Note: Since only '-', '0' to '9' are valid formatting characters we
		 * can do a quick check here to pre-check for formatting. If the char
		 * is not formatting then we can skip a useless function call.
		 *
		 * Further note: At least on some platforms, passing %*s rather than
		 * %s to appendStringInfo() is substantially slower, so many of the
		 * cases below avoid doing that unless non-zero padding is in fact
		 * specified.
		 */
		if (*p > '9')
			padding = 0;
		else if ((p = process_log_prefix_padding(p, &padding)) == NULL)
			break;

		/* process the option */
		switch (*p)
		{
			case 'a':
				if (MyProcPort)
				{
					const char *appname = application_name;

					if (appname == NULL || *appname == '\0')
						appname = _("[unknown]");
					if (padding != 0)
						appendStringInfo(buf, "%*s", padding, appname);
					else
						appendStringInfoString(buf, appname);
				}
				else if (padding != 0)
					appendStringInfoSpaces(buf,
										   padding > 0 ? padding : -padding);

				break;
			case 'b':
				{
					const char *backend_type_str;

					if (MyProcPid == PostmasterPid)
						backend_type_str = "postmaster";
					else if (MyBackendType == B_BG_WORKER)
						backend_type_str = MyBgworkerEntry->bgw_type;
					else
						backend_type_str = GetBackendTypeDesc(MyBackendType);

					if (padding != 0)
						appendStringInfo(buf, "%*s", padding, backend_type_str);
					else
						appendStringInfoString(buf, backend_type_str);
					break;
				}
			case 'u':
				if (MyProcPort)
				{
					const char *username = MyProcPort->user_name;

					if (username == NULL || *username == '\0')
						username = _("[unknown]");
					if (padding != 0)
						appendStringInfo(buf, "%*s", padding, username);
					else
						appendStringInfoString(buf, username);
				}
				else if (padding != 0)
					appendStringInfoSpaces(buf,
										   padding > 0 ? padding : -padding);
				break;
			case 'd':
				if (MyProcPort)
				{
					const char *dbname = MyProcPort->database_name;

					if (dbname == NULL || *dbname == '\0')
						dbname = _("[unknown]");
					if (padding != 0)
						appendStringInfo(buf, "%*s", padding, dbname);
					else
						appendStringInfoString(buf, dbname);
				}
				else if (padding != 0)
					appendStringInfoSpaces(buf,
										   padding > 0 ? padding : -padding);
				break;
			case 'c':
				if (padding != 0)
				{
					char		strfbuf[128];

					snprintf(strfbuf, sizeof(strfbuf) - 1, "%lx.%x",
							 (long) (MyStartTime), MyProcPid);
					appendStringInfo(buf, "%*s", padding, strfbuf);
				}
				else
					appendStringInfo(buf, "%lx.%x", (long) (MyStartTime), MyProcPid);
				break;
			case 'p':
				if (padding != 0)
					appendStringInfo(buf, "%*d", padding, MyProcPid);
				else
					appendStringInfo(buf, "%d", MyProcPid);
				break;

			case 'P':
				if (MyProc)
				{
					PGPROC	   *leader = MyProc->lockGroupLeader;

					/*
					 * Show the leader only for active parallel workers. This
					 * leaves out the leader of a parallel group.
					 */
					if (leader == NULL || leader->pid == MyProcPid)
						appendStringInfoSpaces(buf,
											   padding > 0 ? padding : -padding);
					else if (padding != 0)
						appendStringInfo(buf, "%*d", padding, leader->pid);
					else
						appendStringInfo(buf, "%d", leader->pid);
				}
				else if (padding != 0)
					appendStringInfoSpaces(buf,
										   padding > 0 ? padding : -padding);
				break;

			case 'l':
				if (padding != 0)
					appendStringInfo(buf, "%*ld", padding, log_line_number);
				else
					appendStringInfo(buf, "%ld", log_line_number);
				break;
			case 'm':
				setup_formatted_log_time();
				if (padding != 0)
					appendStringInfo(buf, "%*s", padding, formatted_log_time);
				else
					appendStringInfoString(buf, formatted_log_time);
				break;
			case 't':
				{
					pg_time_t	stamp_time = (pg_time_t) time(NULL);
					char		strfbuf[128];

					pg_strftime(strfbuf, sizeof(strfbuf),
								"%Y-%m-%d %H:%M:%S %Z",
								pg_localtime(&stamp_time, log_timezone));
					if (padding != 0)
						appendStringInfo(buf, "%*s", padding, strfbuf);
					else
						appendStringInfoString(buf, strfbuf);
				}
				break;
			case 'n':
				{
					char		strfbuf[128];

					if (!saved_timeval_set)
					{
						gettimeofday(&saved_timeval, NULL);
						saved_timeval_set = true;
					}

					snprintf(strfbuf, sizeof(strfbuf), "%ld.%03d",
							 (long) saved_timeval.tv_sec,
							 (int) (saved_timeval.tv_usec / 1000));

					if (padding != 0)
						appendStringInfo(buf, "%*s", padding, strfbuf);
					else
						appendStringInfoString(buf, strfbuf);
				}
				break;
			case 's':
				if (formatted_start_time[0] == '\0')
					setup_formatted_start_time();
				if (padding != 0)
					appendStringInfo(buf, "%*s", padding, formatted_start_time);
				else
					appendStringInfoString(buf, formatted_start_time);
				break;
			case 'i':
				if (MyProcPort)
				{
					const char *psdisp;
					int			displen;

					psdisp = get_ps_display(&displen);
					if (padding != 0)
						appendStringInfo(buf, "%*s", padding, psdisp);
					else
						appendBinaryStringInfo(buf, psdisp, displen);

				}
				else if (padding != 0)
					appendStringInfoSpaces(buf,
										   padding > 0 ? padding : -padding);
				break;
			case 'r':
				if (MyProcPort && MyProcPort->remote_host)
				{
					if (padding != 0)
					{
						if (MyProcPort->remote_port && MyProcPort->remote_port[0] != '\0')
						{
							/*
							 * This option is slightly special as the port
							 * number may be appended onto the end. Here we
							 * need to build 1 string which contains the
							 * remote_host and optionally the remote_port (if
							 * set) so we can properly align the string.
							 */

							char	   *hostport;

							hostport = psprintf("%s(%s)", MyProcPort->remote_host, MyProcPort->remote_port);
							appendStringInfo(buf, "%*s", padding, hostport);
							pfree(hostport);
						}
						else
							appendStringInfo(buf, "%*s", padding, MyProcPort->remote_host);
					}
					else
					{
						/* padding is 0, so we don't need a temp buffer */
						appendStringInfoString(buf, MyProcPort->remote_host);
						if (MyProcPort->remote_port &&
							MyProcPort->remote_port[0] != '\0')
							appendStringInfo(buf, "(%s)",
											 MyProcPort->remote_port);
					}

				}
				else if (padding != 0)
					appendStringInfoSpaces(buf,
										   padding > 0 ? padding : -padding);
				break;
			case 'h':
				if (MyProcPort && MyProcPort->remote_host)
				{
					if (padding != 0)
						appendStringInfo(buf, "%*s", padding, MyProcPort->remote_host);
					else
						appendStringInfoString(buf, MyProcPort->remote_host);
				}
				else if (padding != 0)
					appendStringInfoSpaces(buf,
										   padding > 0 ? padding : -padding);
				break;
			case 'q':
				/* in postmaster and friends, stop if %q is seen */
				/* in a backend, just ignore */
				if (MyProcPort == NULL)
					return;
				break;
			case 'v':
				/* keep VXID format in sync with lockfuncs.c */
				if (MyProc != NULL && MyProc->backendId != InvalidBackendId)
				{
					if (padding != 0)
					{
						char		strfbuf[128];

						snprintf(strfbuf, sizeof(strfbuf) - 1, "%d/%u",
								 MyProc->backendId, MyProc->lxid);
						appendStringInfo(buf, "%*s", padding, strfbuf);
					}
					else
						appendStringInfo(buf, "%d/%u", MyProc->backendId, MyProc->lxid);
				}
				else if (padding != 0)
					appendStringInfoSpaces(buf,
										   padding > 0 ? padding : -padding);
				break;
			case 'x':
				if (padding != 0)
					appendStringInfo(buf, "%*u", padding, GetTopTransactionIdIfAny());
				else
					appendStringInfo(buf, "%u", GetTopTransactionIdIfAny());
				break;
			case 'e':
				if (padding != 0)
					appendStringInfo(buf, "%*s", padding, unpack_sql_state(edata->sqlerrcode));
				else
					appendStringInfoString(buf, unpack_sql_state(edata->sqlerrcode));
				break;
			case 'Q':
				if (padding != 0)
					appendStringInfo(buf, "%*lld", padding,
									 (long long) pgstat_get_my_query_id());
				else
					appendStringInfo(buf, "%lld",
									 (long long) pgstat_get_my_query_id());
				break;
			default:
				/* format error - ignore it */
				break;
		}
	}
}

/*
 * append a CSV'd version of a string to a StringInfo
 * We use the PostgreSQL defaults for CSV, i.e. quote = escape = '"'
 * If it's NULL, append nothing.
 */
static inline void
appendCSVLiteral(StringInfo buf, const char *data)
{
	const char *p = data;
	char		c;

	/* avoid confusing an empty string with NULL */
	if (p == NULL)
		return;

	appendStringInfoCharMacro(buf, '"');
	while ((c = *p++) != '\0')
	{
		if (c == '"')
			appendStringInfoCharMacro(buf, '"');
		appendStringInfoCharMacro(buf, c);
	}
	appendStringInfoCharMacro(buf, '"');
}

/*
 * Constructs the error message, depending on the Errordata it gets, in a CSV
 * format which is described in doc/src/sgml/config.sgml.
 */
static void
write_csvlog(ErrorData *edata)
{
	StringInfoData buf;
	bool		print_stmt = false;

	/* static counter for line numbers */
	static long log_line_number = 0;

	/* has counter been reset in current process? */
	static int	log_my_pid = 0;

	/*
	 * This is one of the few places where we'd rather not inherit a static
	 * variable's value from the postmaster.  But since we will, reset it when
	 * MyProcPid changes.
	 */
	if (log_my_pid != MyProcPid)
	{
		log_line_number = 0;
		log_my_pid = MyProcPid;
		formatted_start_time[0] = '\0';
	}
	log_line_number++;

	initStringInfo(&buf);

	/*
	 * timestamp with milliseconds
	 *
	 * Check if the timestamp is already calculated for the syslog message,
	 * and use it if so.  Otherwise, get the current timestamp.  This is done
	 * to put same timestamp in both syslog and csvlog messages.
	 */
	if (formatted_log_time[0] == '\0')
		setup_formatted_log_time();

	appendStringInfoString(&buf, formatted_log_time);
	appendStringInfoChar(&buf, ',');

	/* username */
	if (MyProcPort)
		appendCSVLiteral(&buf, MyProcPort->user_name);
	appendStringInfoChar(&buf, ',');

	/* database name */
	if (MyProcPort)
		appendCSVLiteral(&buf, MyProcPort->database_name);
	appendStringInfoChar(&buf, ',');

	/* Process id  */
	if (MyProcPid != 0)
		appendStringInfo(&buf, "%d", MyProcPid);
	appendStringInfoChar(&buf, ',');

	/* Remote host and port */
	if (MyProcPort && MyProcPort->remote_host)
	{
		appendStringInfoChar(&buf, '"');
		appendStringInfoString(&buf, MyProcPort->remote_host);
		if (MyProcPort->remote_port && MyProcPort->remote_port[0] != '\0')
		{
			appendStringInfoChar(&buf, ':');
			appendStringInfoString(&buf, MyProcPort->remote_port);
		}
		appendStringInfoChar(&buf, '"');
	}
	appendStringInfoChar(&buf, ',');

	/* session id */
	appendStringInfo(&buf, "%lx.%x", (long) MyStartTime, MyProcPid);
	appendStringInfoChar(&buf, ',');

	/* Line number */
	appendStringInfo(&buf, "%ld", log_line_number);
	appendStringInfoChar(&buf, ',');

	/* PS display */
	if (MyProcPort)
	{
		StringInfoData msgbuf;
		const char *psdisp;
		int			displen;

		initStringInfo(&msgbuf);

		psdisp = get_ps_display(&displen);
		appendBinaryStringInfo(&msgbuf, psdisp, displen);
		appendCSVLiteral(&buf, msgbuf.data);

		pfree(msgbuf.data);
	}
	appendStringInfoChar(&buf, ',');

	/* session start timestamp */
	if (formatted_start_time[0] == '\0')
		setup_formatted_start_time();
	appendStringInfoString(&buf, formatted_start_time);
	appendStringInfoChar(&buf, ',');

	/* Virtual transaction id */
	/* keep VXID format in sync with lockfuncs.c */
	if (MyProc != NULL && MyProc->backendId != InvalidBackendId)
		appendStringInfo(&buf, "%d/%u", MyProc->backendId, MyProc->lxid);
	appendStringInfoChar(&buf, ',');

	/* Transaction id */
	appendStringInfo(&buf, "%u", GetTopTransactionIdIfAny());
	appendStringInfoChar(&buf, ',');

	/* Error severity */
	appendStringInfoString(&buf, _(error_severity(edata->elevel)));
	appendStringInfoChar(&buf, ',');

	/* SQL state code */
	appendStringInfoString(&buf, unpack_sql_state(edata->sqlerrcode));
	appendStringInfoChar(&buf, ',');

	/* errmessage */
	appendCSVLiteral(&buf, edata->message);
	appendStringInfoChar(&buf, ',');

	/* errdetail or errdetail_log */
	if (edata->detail_log)
		appendCSVLiteral(&buf, edata->detail_log);
	else
		appendCSVLiteral(&buf, edata->detail);
	appendStringInfoChar(&buf, ',');

	/* errhint */
	appendCSVLiteral(&buf, edata->hint);
	appendStringInfoChar(&buf, ',');

	/* internal query */
	appendCSVLiteral(&buf, edata->internalquery);
	appendStringInfoChar(&buf, ',');

	/* if printed internal query, print internal pos too */
	if (edata->internalpos > 0 && edata->internalquery != NULL)
		appendStringInfo(&buf, "%d", edata->internalpos);
	appendStringInfoChar(&buf, ',');

	/* errcontext */
	if (!edata->hide_ctx)
		appendCSVLiteral(&buf, edata->context);
	appendStringInfoChar(&buf, ',');

	/* user query --- only reported if not disabled by the caller */
	if (is_log_level_output(edata->elevel, log_min_error_statement) &&
		debug_query_string != NULL &&
		!edata->hide_stmt)
		print_stmt = true;
	if (print_stmt)
		appendCSVLiteral(&buf, debug_query_string);
	appendStringInfoChar(&buf, ',');
	if (print_stmt && edata->cursorpos > 0)
		appendStringInfo(&buf, "%d", edata->cursorpos);
	appendStringInfoChar(&buf, ',');

	/* file error location */
	if (Log_error_verbosity >= PGERROR_VERBOSE)
	{
		StringInfoData msgbuf;

		initStringInfo(&msgbuf);

		if (edata->funcname && edata->filename)
			appendStringInfo(&msgbuf, "%s, %s:%d",
							 edata->funcname, edata->filename,
							 edata->lineno);
		else if (edata->filename)
			appendStringInfo(&msgbuf, "%s:%d",
							 edata->filename, edata->lineno);
		appendCSVLiteral(&buf, msgbuf.data);
		pfree(msgbuf.data);
	}
	appendStringInfoChar(&buf, ',');

	/* application name */
	if (application_name)
		appendCSVLiteral(&buf, application_name);

	appendStringInfoChar(&buf, ',');

	/* backend type */
	if (MyProcPid == PostmasterPid)
		appendCSVLiteral(&buf, "postmaster");
	else if (MyBackendType == B_BG_WORKER)
		appendCSVLiteral(&buf, MyBgworkerEntry->bgw_type);
	else
		appendCSVLiteral(&buf, GetBackendTypeDesc(MyBackendType));

	appendStringInfoChar(&buf, ',');

	/* leader PID */
	if (MyProc)
	{
		PGPROC	   *leader = MyProc->lockGroupLeader;

		/*
		 * Show the leader only for active parallel workers.  This leaves out
		 * the leader of a parallel group.
		 */
		if (leader && leader->pid != MyProcPid)
			appendStringInfo(&buf, "%d", leader->pid);
	}
	appendStringInfoChar(&buf, ',');

	/* query id */
	appendStringInfo(&buf, "%lld", (long long) pgstat_get_my_query_id());

	appendStringInfoChar(&buf, '\n');

	/* If in the syslogger process, try to write messages direct to file */
	if (MyBackendType == B_LOGGER)
		write_syslogger_file(buf.data, buf.len, LOG_DESTINATION_CSVLOG);
	else
		write_pipe_chunks(buf.data, buf.len, LOG_DESTINATION_CSVLOG);

	pfree(buf.data);
}

/*
 * Unpack MAKE_SQLSTATE code. Note that this returns a pointer to a
 * static buffer.
 */
char *
unpack_sql_state(int sql_state)
{
	static char buf[12];
	int			i;

	for (i = 0; i < 5; i++)
	{
		buf[i] = PGUNSIXBIT(sql_state);
		sql_state >>= 6;
	}

	buf[i] = '\0';
	return buf;
}


/*
 * Write error report to server's log
 */
static void
send_message_to_server_log(ErrorData *edata)
{
	StringInfoData buf;

	initStringInfo(&buf);

	saved_timeval_set = false;
	formatted_log_time[0] = '\0';

	log_line_prefix(&buf, edata);
	appendStringInfo(&buf, "%s:  ", _(error_severity(edata->elevel)));

	if (Log_error_verbosity >= PGERROR_VERBOSE)
		appendStringInfo(&buf, "%s: ", unpack_sql_state(edata->sqlerrcode));

	if (edata->message)
		append_with_tabs(&buf, edata->message);
	else
		append_with_tabs(&buf, _("missing error text"));

	if (edata->cursorpos > 0)
		appendStringInfo(&buf, _(" at character %d"),
						 edata->cursorpos);
	else if (edata->internalpos > 0)
		appendStringInfo(&buf, _(" at character %d"),
						 edata->internalpos);

	appendStringInfoChar(&buf, '\n');

	if (Log_error_verbosity >= PGERROR_DEFAULT)
	{
		if (edata->detail_log)
		{
			log_line_prefix(&buf, edata);
			appendStringInfoString(&buf, _("DETAIL:  "));
			append_with_tabs(&buf, edata->detail_log);
			appendStringInfoChar(&buf, '\n');
		}
		else if (edata->detail)
		{
			log_line_prefix(&buf, edata);
			appendStringInfoString(&buf, _("DETAIL:  "));
			append_with_tabs(&buf, edata->detail);
			appendStringInfoChar(&buf, '\n');
		}
		if (edata->hint)
		{
			log_line_prefix(&buf, edata);
			appendStringInfoString(&buf, _("HINT:  "));
			append_with_tabs(&buf, edata->hint);
			appendStringInfoChar(&buf, '\n');
		}
		if (edata->internalquery)
		{
			log_line_prefix(&buf, edata);
			appendStringInfoString(&buf, _("QUERY:  "));
			append_with_tabs(&buf, edata->internalquery);
			appendStringInfoChar(&buf, '\n');
		}
		if (edata->context && !edata->hide_ctx)
		{
			log_line_prefix(&buf, edata);
			appendStringInfoString(&buf, _("CONTEXT:  "));
			append_with_tabs(&buf, edata->context);
			appendStringInfoChar(&buf, '\n');
		}
		if (Log_error_verbosity >= PGERROR_VERBOSE)
		{
			/* assume no newlines in funcname or filename... */
			if (edata->funcname && edata->filename)
			{
				log_line_prefix(&buf, edata);
				appendStringInfo(&buf, _("LOCATION:  %s, %s:%d\n"),
								 edata->funcname, edata->filename,
								 edata->lineno);
			}
			else if (edata->filename)
			{
				log_line_prefix(&buf, edata);
				appendStringInfo(&buf, _("LOCATION:  %s:%d\n"),
								 edata->filename, edata->lineno);
			}
		}
		if (edata->backtrace)
		{
			log_line_prefix(&buf, edata);
			appendStringInfoString(&buf, _("BACKTRACE:  "));
			append_with_tabs(&buf, edata->backtrace);
			appendStringInfoChar(&buf, '\n');
		}
	}

	/*
	 * If the user wants the query that generated this error logged, do it.
	 */
	if (is_log_level_output(edata->elevel, log_min_error_statement) &&
		debug_query_string != NULL &&
		!edata->hide_stmt)
	{
		log_line_prefix(&buf, edata);
		appendStringInfoString(&buf, _("STATEMENT:  "));
		append_with_tabs(&buf, debug_query_string);
		appendStringInfoChar(&buf, '\n');
	}

#ifdef HAVE_SYSLOG
	/* Write to syslog, if enabled */
	if (Log_destination & LOG_DESTINATION_SYSLOG)
	{
		int			syslog_level;

		switch (edata->elevel)
		{
			case DEBUG5:
			case DEBUG4:
			case DEBUG3:
			case DEBUG2:
			case DEBUG1:
				syslog_level = LOG_DEBUG;
				break;
			case LOG:
			case LOG_SERVER_ONLY:
			case INFO:
				syslog_level = LOG_INFO;
				break;
			case NOTICE:
			case WARNING:
			case WARNING_CLIENT_ONLY:
				syslog_level = LOG_NOTICE;
				break;
			case ERROR:
				syslog_level = LOG_WARNING;
				break;
			case FATAL:
				syslog_level = LOG_ERR;
				break;
			case PANIC:
			default:
				syslog_level = LOG_CRIT;
				break;
		}

		write_syslog(syslog_level, buf.data);
	}
#endif							/* HAVE_SYSLOG */

#ifdef WIN32
	/* Write to eventlog, if enabled */
	if (Log_destination & LOG_DESTINATION_EVENTLOG)
	{
		write_eventlog(edata->elevel, buf.data, buf.len);
	}
#endif							/* WIN32 */

	/* Write to stderr, if enabled */
	if ((Log_destination & LOG_DESTINATION_STDERR) || whereToSendOutput == DestDebug)
	{
		/*
		 * Use the chunking protocol if we know the syslogger should be
		 * catching stderr output, and we are not ourselves the syslogger.
		 * Otherwise, just do a vanilla write to stderr.
		 */
		if (redirection_done && MyBackendType != B_LOGGER)
			write_pipe_chunks(buf.data, buf.len, LOG_DESTINATION_STDERR);
#ifdef WIN32

		/*
		 * In a win32 service environment, there is no usable stderr. Capture
		 * anything going there and write it to the eventlog instead.
		 *
		 * If stderr redirection is active, it was OK to write to stderr above
		 * because that's really a pipe to the syslogger process.
		 */
		else if (pgwin32_is_service())
			write_eventlog(edata->elevel, buf.data, buf.len);
#endif
		else
			write_console(buf.data, buf.len);
	}

	/* If in the syslogger process, try to write messages direct to file */
	if (MyBackendType == B_LOGGER)
		write_syslogger_file(buf.data, buf.len, LOG_DESTINATION_STDERR);

	/* Write to CSV log if enabled */
	if (Log_destination & LOG_DESTINATION_CSVLOG)
	{
		if (redirection_done || MyBackendType == B_LOGGER)
		{
			/*
			 * send CSV data if it's safe to do so (syslogger doesn't need the
			 * pipe). First get back the space in the message buffer.
			 */
			pfree(buf.data);
			write_csvlog(edata);
		}
		else
		{
			/*
			 * syslogger not up (yet), so just dump the message to stderr,
			 * unless we already did so above.
			 */
			if (!(Log_destination & LOG_DESTINATION_STDERR) &&
				whereToSendOutput != DestDebug)
				write_console(buf.data, buf.len);
			pfree(buf.data);
		}
	}
	else
	{
		pfree(buf.data);
	}
}

/*
 * Send data to the syslogger using the chunked protocol
 *
 * Note: when there are multiple backends writing into the syslogger pipe,
 * it's critical that each write go into the pipe indivisibly, and not
 * get interleaved with data from other processes.  Fortunately, the POSIX
 * spec requires that writes to pipes be atomic so long as they are not
 * more than PIPE_BUF bytes long.  So we divide long messages into chunks
 * that are no more than that length, and send one chunk per write() call.
 * The collector process knows how to reassemble the chunks.
 *
 * Because of the atomic write requirement, there are only two possible
 * results from write() here: -1 for failure, or the requested number of
 * bytes.  There is not really anything we can do about a failure; retry would
 * probably be an infinite loop, and we can't even report the error usefully.
 * (There is noplace else we could send it!)  So we might as well just ignore
 * the result from write().  However, on some platforms you get a compiler
 * warning from ignoring write()'s result, so do a little dance with casting
 * rc to void to shut up the compiler.
 */
static void
write_pipe_chunks(char *data, int len, int dest)
{
	PipeProtoChunk p;
	int			fd = fileno(stderr);
	int			rc;

	Assert(len > 0);

	p.proto.nuls[0] = p.proto.nuls[1] = '\0';
	p.proto.pid = MyProcPid;

	/* write all but the last chunk */
	while (len > PIPE_MAX_PAYLOAD)
	{
		p.proto.is_last = (dest == LOG_DESTINATION_CSVLOG ? 'F' : 'f');
		p.proto.len = PIPE_MAX_PAYLOAD;
		memcpy(p.proto.data, data, PIPE_MAX_PAYLOAD);
		rc = write(fd, &p, PIPE_HEADER_SIZE + PIPE_MAX_PAYLOAD);
		(void) rc;
		data += PIPE_MAX_PAYLOAD;
		len -= PIPE_MAX_PAYLOAD;
	}

	/* write the last chunk */
	p.proto.is_last = (dest == LOG_DESTINATION_CSVLOG ? 'T' : 't');
	p.proto.len = len;
	memcpy(p.proto.data, data, len);
	rc = write(fd, &p, PIPE_HEADER_SIZE + len);
	(void) rc;
}


/*
 * Append a text string to the error report being built for the client.
 *
 * This is ordinarily identical to pq_sendstring(), but if we are in
 * error recursion trouble we skip encoding conversion, because of the
 * possibility that the problem is a failure in the encoding conversion
 * subsystem itself.  Code elsewhere should ensure that the passed-in
 * strings will be plain 7-bit ASCII, and thus not in need of conversion,
 * in such cases.  (In particular, we disable localization of error messages
 * to help ensure that's true.)
 */
static void
err_sendstring(StringInfo buf, const char *str)
{
	if (in_error_recursion_trouble())
		pq_send_ascii_string(buf, str);
	else
		pq_sendstring(buf, str);
}

/*
 * Write error report to client
 */
static void
send_message_to_frontend(ErrorData *edata)
{
	StringInfoData msgbuf;

	/*
	 * We no longer support pre-3.0 FE/BE protocol, except here.  If a client
	 * tries to connect using an older protocol version, it's nice to send the
	 * "protocol version not supported" error in a format the client
	 * understands.  If protocol hasn't been set yet, early in backend
	 * startup, assume modern protocol.
	 */
	if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 3 || FrontendProtocol == 0)
	{
		/* New style with separate fields */
		const char *sev;
		char		tbuf[12];
		int			ssval;
		int			i;

		/* 'N' (Notice) is for nonfatal conditions, 'E' is for errors */
		pq_beginmessage(&msgbuf, (edata->elevel < ERROR) ? 'N' : 'E');

		sev = error_severity(edata->elevel);
		pq_sendbyte(&msgbuf, PG_DIAG_SEVERITY);
		err_sendstring(&msgbuf, _(sev));
		pq_sendbyte(&msgbuf, PG_DIAG_SEVERITY_NONLOCALIZED);
		err_sendstring(&msgbuf, sev);

		/* unpack MAKE_SQLSTATE code */
		ssval = edata->sqlerrcode;
		for (i = 0; i < 5; i++)
		{
			tbuf[i] = PGUNSIXBIT(ssval);
			ssval >>= 6;
		}
		tbuf[i] = '\0';

		pq_sendbyte(&msgbuf, PG_DIAG_SQLSTATE);
		err_sendstring(&msgbuf, tbuf);

		/* M field is required per protocol, so always send something */
		pq_sendbyte(&msgbuf, PG_DIAG_MESSAGE_PRIMARY);
		if (edata->message)
			err_sendstring(&msgbuf, edata->message);
		else
			err_sendstring(&msgbuf, _("missing error text"));

		if (edata->detail)
		{
			pq_sendbyte(&msgbuf, PG_DIAG_MESSAGE_DETAIL);
			err_sendstring(&msgbuf, edata->detail);
		}

		/* detail_log is intentionally not used here */

		if (edata->hint)
		{
			pq_sendbyte(&msgbuf, PG_DIAG_MESSAGE_HINT);
			err_sendstring(&msgbuf, edata->hint);
		}

		if (edata->context)
		{
			pq_sendbyte(&msgbuf, PG_DIAG_CONTEXT);
			err_sendstring(&msgbuf, edata->context);
		}

		if (edata->schema_name)
		{
			pq_sendbyte(&msgbuf, PG_DIAG_SCHEMA_NAME);
			err_sendstring(&msgbuf, edata->schema_name);
		}

		if (edata->table_name)
		{
			pq_sendbyte(&msgbuf, PG_DIAG_TABLE_NAME);
			err_sendstring(&msgbuf, edata->table_name);
		}

		if (edata->column_name)
		{
			pq_sendbyte(&msgbuf, PG_DIAG_COLUMN_NAME);
			err_sendstring(&msgbuf, edata->column_name);
		}

		if (edata->datatype_name)
		{
			pq_sendbyte(&msgbuf, PG_DIAG_DATATYPE_NAME);
			err_sendstring(&msgbuf, edata->datatype_name);
		}

		if (edata->constraint_name)
		{
			pq_sendbyte(&msgbuf, PG_DIAG_CONSTRAINT_NAME);
			err_sendstring(&msgbuf, edata->constraint_name);
		}

		if (edata->cursorpos > 0)
		{
			snprintf(tbuf, sizeof(tbuf), "%d", edata->cursorpos);
			pq_sendbyte(&msgbuf, PG_DIAG_STATEMENT_POSITION);
			err_sendstring(&msgbuf, tbuf);
		}

		if (edata->internalpos > 0)
		{
			snprintf(tbuf, sizeof(tbuf), "%d", edata->internalpos);
			pq_sendbyte(&msgbuf, PG_DIAG_INTERNAL_POSITION);
			err_sendstring(&msgbuf, tbuf);
		}

		if (edata->internalquery)
		{
			pq_sendbyte(&msgbuf, PG_DIAG_INTERNAL_QUERY);
			err_sendstring(&msgbuf, edata->internalquery);
		}

		if (edata->filename)
		{
			pq_sendbyte(&msgbuf, PG_DIAG_SOURCE_FILE);
			err_sendstring(&msgbuf, edata->filename);
		}

		if (edata->lineno > 0)
		{
			snprintf(tbuf, sizeof(tbuf), "%d", edata->lineno);
			pq_sendbyte(&msgbuf, PG_DIAG_SOURCE_LINE);
			err_sendstring(&msgbuf, tbuf);
		}

		if (edata->funcname)
		{
			pq_sendbyte(&msgbuf, PG_DIAG_SOURCE_FUNCTION);
			err_sendstring(&msgbuf, edata->funcname);
		}

		pq_sendbyte(&msgbuf, '\0'); /* terminator */

		pq_endmessage(&msgbuf);
	}
	else
	{
		/* Old style --- gin up a backwards-compatible message */
		StringInfoData buf;

		initStringInfo(&buf);

		appendStringInfo(&buf, "%s:  ", _(error_severity(edata->elevel)));

		if (edata->message)
			appendStringInfoString(&buf, edata->message);
		else
			appendStringInfoString(&buf, _("missing error text"));

		appendStringInfoChar(&buf, '\n');

		/* 'N' (Notice) is for nonfatal conditions, 'E' is for errors */
		pq_putmessage_v2((edata->elevel < ERROR) ? 'N' : 'E', buf.data, buf.len + 1);

		pfree(buf.data);
	}

	/*
	 * This flush is normally not necessary, since postgres.c will flush out
	 * waiting data when control returns to the main loop. But it seems best
	 * to leave it here, so that the client has some clue what happened if the
	 * backend dies before getting back to the main loop ... error/notice
	 * messages should not be a performance-critical path anyway, so an extra
	 * flush won't hurt much ...
	 */
	pq_flush();
}


/*
 * Support routines for formatting error messages.
 */


/*
 * error_severity --- get string representing elevel
 *
 * The string is not localized here, but we mark the strings for translation
 * so that callers can invoke _() on the result.
 */
static const char *
error_severity(int elevel)
{
	const char *prefix;

	switch (elevel)
	{
		case DEBUG1:
		case DEBUG2:
		case DEBUG3:
		case DEBUG4:
		case DEBUG5:
			prefix = gettext_noop("DEBUG");
			break;
		case LOG:
		case LOG_SERVER_ONLY:
			prefix = gettext_noop("LOG");
			break;
		case INFO:
			prefix = gettext_noop("INFO");
			break;
		case NOTICE:
			prefix = gettext_noop("NOTICE");
			break;
		case WARNING:
		case WARNING_CLIENT_ONLY:
			prefix = gettext_noop("WARNING");
			break;
		case ERROR:
			prefix = gettext_noop("ERROR");
			break;
		case FATAL:
			prefix = gettext_noop("FATAL");
			break;
		case PANIC:
			prefix = gettext_noop("PANIC");
			break;
		default:
			prefix = "???";
			break;
	}

	return prefix;
}


/*
 *	append_with_tabs
 *
 *	Append the string to the StringInfo buffer, inserting a tab after any
 *	newline.
 */
static void
append_with_tabs(StringInfo buf, const char *str)
{
	char		ch;

	while ((ch = *str++) != '\0')
	{
		appendStringInfoCharMacro(buf, ch);
		if (ch == '\n')
			appendStringInfoCharMacro(buf, '\t');
	}
}


/*
 * Write errors to stderr (or by equal means when stderr is
 * not available). Used before ereport/elog can be used
 * safely (memory context, GUC load etc)
 */
void
write_stderr(const char *fmt,...)
{
	va_list		ap;

#ifdef WIN32
	char		errbuf[2048];	/* Arbitrary size? */
#endif

	fmt = _(fmt);

	va_start(ap, fmt);
#ifndef WIN32
	/* On Unix, we just fprintf to stderr */
	vfprintf(stderr, fmt, ap);
	fflush(stderr);
#else
	vsnprintf(errbuf, sizeof(errbuf), fmt, ap);

	/*
	 * On Win32, we print to stderr if running on a console, or write to
	 * eventlog if running as a service
	 */
	if (pgwin32_is_service())	/* Running as a service */
	{
		write_eventlog(ERROR, errbuf, strlen(errbuf));
	}
	else
	{
		/* Not running as service, write to stderr */
		write_console(errbuf, strlen(errbuf));
		fflush(stderr);
	}
#endif
	va_end(ap);
}


/*
 * Adjust the level of a recovery-related message per trace_recovery_messages.
 *
 * The argument is the default log level of the message, eg, DEBUG2.  (This
 * should only be applied to DEBUGn log messages, otherwise it's a no-op.)
 * If the level is >= trace_recovery_messages, we return LOG, causing the
 * message to be logged unconditionally (for most settings of
 * log_min_messages).  Otherwise, we return the argument unchanged.
 * The message will then be shown based on the setting of log_min_messages.
 *
 * Intention is to keep this for at least the whole of the 9.0 production
 * release, so we can more easily diagnose production problems in the field.
 * It should go away eventually, though, because it's an ugly and
 * hard-to-explain kluge.
 */
int
trace_recovery(int trace_level)
{
	if (trace_level < LOG &&
		trace_level >= trace_recovery_messages)
		return LOG;

	return trace_level;
}

/*-------------------------------------------------------------------------
 *
 * palloc.h
 *	  POSTGRES memory allocator definitions.
 *
 * This file contains the basic memory allocation interface that is
 * needed by almost every backend module.  It is included directly by
 * postgres.h, so the definitions here are automatically available
 * everywhere.  Keep it lean!
 *
 * Memory allocation occurs within "contexts".  Every chunk obtained from
 * palloc()/MemoryContextAlloc() is allocated within a specific context.
 * The entire contents of a context can be freed easily and quickly by
 * resetting or deleting the context --- this is both faster and less
 * prone to memory-leakage bugs than releasing chunks individually.
 * We organize contexts into context trees to allow fine-grain control
 * over chunk lifetime while preserving the certainty that we will free
 * everything that should be freed.  See utils/mmgr/README for more info.
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/palloc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PALLOC_H
#define PALLOC_H

/*
 * Type MemoryContextData is declared in nodes/memnodes.h.  Most users
 * of memory allocation should just treat it as an abstract type, so we
 * do not provide the struct contents here.
 */
typedef struct MemoryContextData *MemoryContext;

/*
 * A memory context can have callback functions registered on it.  Any such
 * function will be called once just before the context is next reset or
 * deleted.  The MemoryContextCallback struct describing such a callback
 * typically would be allocated within the context itself, thereby avoiding
 * any need to manage it explicitly (the reset/delete action will free it).
 */
typedef void (*MemoryContextCallbackFunction) (void *arg);

typedef struct MemoryContextCallback
{
	MemoryContextCallbackFunction func; /* function to call */
	void	   *arg;			/* argument to pass it */
	struct MemoryContextCallback *next; /* next in list of callbacks */
} MemoryContextCallback;

/*
 * CurrentMemoryContext is the default allocation context for palloc().
 * Avoid accessing it directly!  Instead, use MemoryContextSwitchTo()
 * to change the setting.
 */
extern PGDLLIMPORT MemoryContext CurrentMemoryContext;

/*
 * Flags for MemoryContextAllocExtended.
 */
#define MCXT_ALLOC_HUGE			0x01	/* allow huge allocation (> 1 GB) */
#define MCXT_ALLOC_NO_OOM		0x02	/* no failure if out-of-memory */
#define MCXT_ALLOC_ZERO			0x04	/* zero allocated memory */

/*
 * Fundamental memory-allocation operations (more are in utils/memutils.h)
 */
extern void *MemoryContextAlloc(MemoryContext context, Size size);
extern void *MemoryContextAllocZero(MemoryContext context, Size size);
extern void *MemoryContextAllocZeroAligned(MemoryContext context, Size size);
extern void *MemoryContextAllocExtended(MemoryContext context,
										Size size, int flags);

extern void *palloc(Size size);
extern void *palloc0(Size size);
extern void *palloc_extended(Size size, int flags);
extern pg_nodiscard void *repalloc(void *pointer, Size size);
extern void pfree(void *pointer);

/*
 * Variants with easier notation and more type safety
 */

/*
 * Allocate space for one object of type "type"
 */
#define palloc_object(type) ((type *) palloc(sizeof(type)))
#define palloc0_object(type) ((type *) palloc0(sizeof(type)))

/*
 * Allocate space for "count" objects of type "type"
 */
#define palloc_array(type, count) ((type *) palloc(sizeof(type) * (count)))
#define palloc0_array(type, count) ((type *) palloc0(sizeof(type) * (count)))

/*
 * Change size of allocation pointed to by "pointer" to have space for "count"
 * objects of type "type"
 */
#define repalloc_array(pointer, type, count) ((type *) repalloc(pointer, sizeof(type) * (count)))

/*
 * The result of palloc() is always word-aligned, so we can skip testing
 * alignment of the pointer when deciding which MemSet variant to use.
 * Note that this variant does not offer any advantage, and should not be
 * used, unless its "sz" argument is a compile-time constant; therefore, the
 * issue that it evaluates the argument multiple times isn't a problem in
 * practice.
 */
#define palloc0fast(sz) \
	( MemSetTest(0, sz) ? \
		MemoryContextAllocZeroAligned(CurrentMemoryContext, sz) : \
		MemoryContextAllocZero(CurrentMemoryContext, sz) )

/* Higher-limit allocators. */
extern void *MemoryContextAllocHuge(MemoryContext context, Size size);
extern pg_nodiscard void *repalloc_huge(void *pointer, Size size);

/*
 * Although this header file is nominally backend-only, certain frontend
 * programs like pg_controldata include it via postgres.h.  For some compilers
 * it's necessary to hide the inline definition of MemoryContextSwitchTo in
 * this scenario; hence the #ifndef FRONTEND.
 */

#ifndef FRONTEND
static inline MemoryContext
MemoryContextSwitchTo(MemoryContext context)
{
	MemoryContext old = CurrentMemoryContext;

	CurrentMemoryContext = context;
	return old;
}
#endif							/* FRONTEND */

/* Registration of memory context reset/delete callbacks */
extern void MemoryContextRegisterResetCallback(MemoryContext context,
											   MemoryContextCallback *cb);

/*
 * These are like standard strdup() except the copied string is
 * allocated in a context, not with malloc().
 */
extern char *MemoryContextStrdup(MemoryContext context, const char *string);
extern char *pstrdup(const char *in);
extern char *pnstrdup(const char *in, Size len);

extern char *pchomp(const char *in);

/* sprintf into a palloc'd buffer --- these are in psprintf.c */
extern char *psprintf(const char *fmt,...) pg_attribute_printf(1, 2);
extern size_t pvsnprintf(char *buf, size_t len, const char *fmt, va_list args) pg_attribute_printf(3, 0);

#endif							/* PALLOC_H */


/* ----------------------------------------------------------------
 *				Section 1:	variable-length datatypes (TOAST support)
 * ----------------------------------------------------------------
 */

/*
 * struct varatt_external is a traditional "TOAST pointer", that is, the
 * information needed to fetch a Datum stored out-of-line in a TOAST table.
 * The data is compressed if and only if the external size stored in
 * va_extinfo is less than va_rawsize - VARHDRSZ.
 *
 * This struct must not contain any padding, because we sometimes compare
 * these pointers using memcmp.
 *
 * Note that this information is stored unaligned within actual tuples, so
 * you need to memcpy from the tuple into a local struct variable before
 * you can look at these fields!  (The reason we use memcmp is to avoid
 * having to do that just to detect equality of two TOAST pointers...)
 */
typedef struct varatt_external
{
	int32		va_rawsize;		/* Original data size (includes header) */
	uint32		va_extinfo;		/* External saved size (without header) and
								 * compression method */
	Oid			va_valueid;		/* Unique ID of value within TOAST table */
	Oid			va_toastrelid;	/* RelID of TOAST table containing it */
}			varatt_external;

/*
 * These macros define the "saved size" portion of va_extinfo.  Its remaining
 * two high-order bits identify the compression method.
 */
#define VARLENA_EXTSIZE_BITS	30
#define VARLENA_EXTSIZE_MASK	((1U << VARLENA_EXTSIZE_BITS) - 1)

/*
 * struct varatt_indirect is a "TOAST pointer" representing an out-of-line
 * Datum that's stored in memory, not in an external toast relation.
 * The creator of such a Datum is entirely responsible that the referenced
 * storage survives for as long as referencing pointer Datums can exist.
 *
 * Note that just as for struct varatt_external, this struct is stored
 * unaligned within any containing tuple.
 */
typedef struct varatt_indirect
{
	struct varlena *pointer;	/* Pointer to in-memory varlena */
}			varatt_indirect;

/*
 * struct varatt_expanded is a "TOAST pointer" representing an out-of-line
 * Datum that is stored in memory, in some type-specific, not necessarily
 * physically contiguous format that is convenient for computation not
 * storage.  APIs for this, in particular the definition of struct
 * ExpandedObjectHeader, are in src/include/utils/expandeddatum.h.
 *
 * Note that just as for struct varatt_external, this struct is stored
 * unaligned within any containing tuple.
 */
typedef struct ExpandedObjectHeader ExpandedObjectHeader;

typedef struct varatt_expanded
{
	ExpandedObjectHeader *eohptr;
} varatt_expanded;

/*
 * Type tag for the various sorts of "TOAST pointer" datums.  The peculiar
 * value for VARTAG_ONDISK comes from a requirement for on-disk compatibility
 * with a previous notion that the tag field was the pointer datum's length.
 */
typedef enum vartag_external
{
	VARTAG_INDIRECT = 1,
	VARTAG_EXPANDED_RO = 2,
	VARTAG_EXPANDED_RW = 3,
	VARTAG_ONDISK = 18
} vartag_external;

/* this test relies on the specific tag values above */
#define VARTAG_IS_EXPANDED(tag) \
	(((tag) & ~1) == VARTAG_EXPANDED_RO)

#define VARTAG_SIZE(tag) \
	((tag) == VARTAG_INDIRECT ? sizeof(varatt_indirect) : \
	 VARTAG_IS_EXPANDED(tag) ? sizeof(varatt_expanded) : \
	 (tag) == VARTAG_ONDISK ? sizeof(varatt_external) : \
	 TrapMacro(true, "unrecognized TOAST vartag"))

/*
 * These structs describe the header of a varlena object that may have been
 * TOASTed.  Generally, don't reference these structs directly, but use the
 * macros below.
 *
 * We use separate structs for the aligned and unaligned cases because the
 * compiler might otherwise think it could generate code that assumes
 * alignment while touching fields of a 1-byte-header varlena.
 */
typedef union
{
	struct						/* Normal varlena (4-byte length) */
	{
		uint32		va_header;
		char		va_data[FLEXIBLE_ARRAY_MEMBER];
	}			va_4byte;
	struct						/* Compressed-in-line format */
	{
		uint32		va_header;
		uint32		va_tcinfo;	/* Original data size (excludes header) and
								 * compression method; see va_extinfo */
		char		va_data[FLEXIBLE_ARRAY_MEMBER]; /* Compressed data */
	}			va_compressed;
} varattrib_4b;

typedef struct
{
	uint8		va_header;
	char		va_data[FLEXIBLE_ARRAY_MEMBER]; /* Data begins here */
} varattrib_1b;

/* TOAST pointers are a subset of varattrib_1b with an identifying tag byte */
typedef struct
{
	uint8		va_header;		/* Always 0x80 or 0x01 */
	uint8		va_tag;			/* Type of datum */
	char		va_data[FLEXIBLE_ARRAY_MEMBER]; /* Type-specific data */
} varattrib_1b_e;

/*
 * Bit layouts for varlena headers on big-endian machines:
 *
 * 00xxxxxx 4-byte length word, aligned, uncompressed data (up to 1G)
 * 01xxxxxx 4-byte length word, aligned, *compressed* data (up to 1G)
 * 10000000 1-byte length word, unaligned, TOAST pointer
 * 1xxxxxxx 1-byte length word, unaligned, uncompressed data (up to 126b)
 *
 * Bit layouts for varlena headers on little-endian machines:
 *
 * xxxxxx00 4-byte length word, aligned, uncompressed data (up to 1G)
 * xxxxxx10 4-byte length word, aligned, *compressed* data (up to 1G)
 * 00000001 1-byte length word, unaligned, TOAST pointer
 * xxxxxxx1 1-byte length word, unaligned, uncompressed data (up to 126b)
 *
 * The "xxx" bits are the length field (which includes itself in all cases).
 * In the big-endian case we mask to extract the length, in the little-endian
 * case we shift.  Note that in both cases the flag bits are in the physically
 * first byte.  Also, it is not possible for a 1-byte length word to be zero;
 * this lets us disambiguate alignment padding bytes from the start of an
 * unaligned datum.  (We now *require* pad bytes to be filled with zero!)
 *
 * In TOAST pointers the va_tag field (see varattrib_1b_e) is used to discern
 * the specific type and length of the pointer datum.
 */

/*
 * Endian-dependent macros.  These are considered internal --- use the
 * external macros below instead of using these directly.
 *
 * Note: IS_1B is true for external toast records but VARSIZE_1B will return 0
 * for such records. Hence you should usually check for IS_EXTERNAL before
 * checking for IS_1B.
 */

#ifdef WORDS_BIGENDIAN

#define VARATT_IS_4B(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0x80) == 0x00)
#define VARATT_IS_4B_U(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0xC0) == 0x00)
#define VARATT_IS_4B_C(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0xC0) == 0x40)
#define VARATT_IS_1B(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0x80) == 0x80)
#define VARATT_IS_1B_E(PTR) \
	((((varattrib_1b *) (PTR))->va_header) == 0x80)
#define VARATT_NOT_PAD_BYTE(PTR) \
	(*((uint8 *) (PTR)) != 0)

/* VARSIZE_4B() should only be used on known-aligned data */
#define VARSIZE_4B(PTR) \
	(((varattrib_4b *) (PTR))->va_4byte.va_header & 0x3FFFFFFF)
#define VARSIZE_1B(PTR) \
	(((varattrib_1b *) (PTR))->va_header & 0x7F)
#define VARTAG_1B_E(PTR) \
	(((varattrib_1b_e *) (PTR))->va_tag)

#define SET_VARSIZE_4B(PTR,len) \
	(((varattrib_4b *) (PTR))->va_4byte.va_header = (len) & 0x3FFFFFFF)
#define SET_VARSIZE_4B_C(PTR,len) \
	(((varattrib_4b *) (PTR))->va_4byte.va_header = ((len) & 0x3FFFFFFF) | 0x40000000)
#define SET_VARSIZE_1B(PTR,len) \
	(((varattrib_1b *) (PTR))->va_header = (len) | 0x80)
#define SET_VARTAG_1B_E(PTR,tag) \
	(((varattrib_1b_e *) (PTR))->va_header = 0x80, \
	 ((varattrib_1b_e *) (PTR))->va_tag = (tag))

#else							/* !WORDS_BIGENDIAN */

#define VARATT_IS_4B(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0x01) == 0x00)
#define VARATT_IS_4B_U(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0x03) == 0x00)
#define VARATT_IS_4B_C(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0x03) == 0x02)
#define VARATT_IS_1B(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0x01) == 0x01)
#define VARATT_IS_1B_E(PTR) \
	((((varattrib_1b *) (PTR))->va_header) == 0x01)
#define VARATT_NOT_PAD_BYTE(PTR) \
	(*((uint8 *) (PTR)) != 0)

/* VARSIZE_4B() should only be used on known-aligned data */
#define VARSIZE_4B(PTR) \
	((((varattrib_4b *) (PTR))->va_4byte.va_header >> 2) & 0x3FFFFFFF)
#define VARSIZE_1B(PTR) \
	((((varattrib_1b *) (PTR))->va_header >> 1) & 0x7F)
#define VARTAG_1B_E(PTR) \
	(((varattrib_1b_e *) (PTR))->va_tag)

#define SET_VARSIZE_4B(PTR,len) \
	(((varattrib_4b *) (PTR))->va_4byte.va_header = (((uint32) (len)) << 2))
#define SET_VARSIZE_4B_C(PTR,len) \
	(((varattrib_4b *) (PTR))->va_4byte.va_header = (((uint32) (len)) << 2) | 0x02)
#define SET_VARSIZE_1B(PTR,len) \
	(((varattrib_1b *) (PTR))->va_header = (((uint8) (len)) << 1) | 0x01)
#define SET_VARTAG_1B_E(PTR,tag) \
	(((varattrib_1b_e *) (PTR))->va_header = 0x01, \
	 ((varattrib_1b_e *) (PTR))->va_tag = (tag))

#endif							/* WORDS_BIGENDIAN */

#define VARDATA_4B(PTR)		(((varattrib_4b *) (PTR))->va_4byte.va_data)
#define VARDATA_4B_C(PTR)	(((varattrib_4b *) (PTR))->va_compressed.va_data)
#define VARDATA_1B(PTR)		(((varattrib_1b *) (PTR))->va_data)
#define VARDATA_1B_E(PTR)	(((varattrib_1b_e *) (PTR))->va_data)

/*
 * Externally visible TOAST macros begin here.
 */

#define VARHDRSZ_EXTERNAL		offsetof(varattrib_1b_e, va_data)
#define VARHDRSZ_COMPRESSED		offsetof(varattrib_4b, va_compressed.va_data)
#define VARHDRSZ_SHORT			offsetof(varattrib_1b, va_data)

#define VARATT_SHORT_MAX		0x7F
#define VARATT_CAN_MAKE_SHORT(PTR) \
	(VARATT_IS_4B_U(PTR) && \
	 (VARSIZE(PTR) - VARHDRSZ + VARHDRSZ_SHORT) <= VARATT_SHORT_MAX)
#define VARATT_CONVERTED_SHORT_SIZE(PTR) \
	(VARSIZE(PTR) - VARHDRSZ + VARHDRSZ_SHORT)

/*
 * In consumers oblivious to data alignment, call PG_DETOAST_DATUM_PACKED(),
 * VARDATA_ANY(), VARSIZE_ANY() and VARSIZE_ANY_EXHDR().  Elsewhere, call
 * PG_DETOAST_DATUM(), VARDATA() and VARSIZE().  Directly fetching an int16,
 * int32 or wider field in the struct representing the datum layout requires
 * aligned data.  memcpy() is alignment-oblivious, as are most operations on
 * datatypes, such as text, whose layout struct contains only char fields.
 *
 * Code assembling a new datum should call VARDATA() and SET_VARSIZE().
 * (Datums begin life untoasted.)
 *
 * Other macros here should usually be used only by tuple assembly/disassembly
 * code and code that specifically wants to work with still-toasted Datums.
 */
#define VARDATA(PTR)						VARDATA_4B(PTR)
#define VARSIZE(PTR)						VARSIZE_4B(PTR)

#define VARSIZE_SHORT(PTR)					VARSIZE_1B(PTR)
#define VARDATA_SHORT(PTR)					VARDATA_1B(PTR)

#define VARTAG_EXTERNAL(PTR)				VARTAG_1B_E(PTR)
#define VARSIZE_EXTERNAL(PTR)				(VARHDRSZ_EXTERNAL + VARTAG_SIZE(VARTAG_EXTERNAL(PTR)))
#define VARDATA_EXTERNAL(PTR)				VARDATA_1B_E(PTR)

#define VARATT_IS_COMPRESSED(PTR)			VARATT_IS_4B_C(PTR)
#define VARATT_IS_EXTERNAL(PTR)				VARATT_IS_1B_E(PTR)
#define VARATT_IS_EXTERNAL_ONDISK(PTR) \
	(VARATT_IS_EXTERNAL(PTR) && VARTAG_EXTERNAL(PTR) == VARTAG_ONDISK)
#define VARATT_IS_EXTERNAL_INDIRECT(PTR) \
	(VARATT_IS_EXTERNAL(PTR) && VARTAG_EXTERNAL(PTR) == VARTAG_INDIRECT)
#define VARATT_IS_EXTERNAL_EXPANDED_RO(PTR) \
	(VARATT_IS_EXTERNAL(PTR) && VARTAG_EXTERNAL(PTR) == VARTAG_EXPANDED_RO)
#define VARATT_IS_EXTERNAL_EXPANDED_RW(PTR) \
	(VARATT_IS_EXTERNAL(PTR) && VARTAG_EXTERNAL(PTR) == VARTAG_EXPANDED_RW)
#define VARATT_IS_EXTERNAL_EXPANDED(PTR) \
	(VARATT_IS_EXTERNAL(PTR) && VARTAG_IS_EXPANDED(VARTAG_EXTERNAL(PTR)))
#define VARATT_IS_EXTERNAL_NON_EXPANDED(PTR) \
	(VARATT_IS_EXTERNAL(PTR) && !VARTAG_IS_EXPANDED(VARTAG_EXTERNAL(PTR)))
#define VARATT_IS_SHORT(PTR)				VARATT_IS_1B(PTR)
#define VARATT_IS_EXTENDED(PTR)				(!VARATT_IS_4B_U(PTR))

#define SET_VARSIZE(PTR, len)				SET_VARSIZE_4B(PTR, len)
#define SET_VARSIZE_SHORT(PTR, len)			SET_VARSIZE_1B(PTR, len)
#define SET_VARSIZE_COMPRESSED(PTR, len)	SET_VARSIZE_4B_C(PTR, len)

#define SET_VARTAG_EXTERNAL(PTR, tag)		SET_VARTAG_1B_E(PTR, tag)

#define VARSIZE_ANY(PTR) \
	(VARATT_IS_1B_E(PTR) ? VARSIZE_EXTERNAL(PTR) : \
	 (VARATT_IS_1B(PTR) ? VARSIZE_1B(PTR) : \
	  VARSIZE_4B(PTR)))

/* Size of a varlena data, excluding header */
#define VARSIZE_ANY_EXHDR(PTR) \
	(VARATT_IS_1B_E(PTR) ? VARSIZE_EXTERNAL(PTR)-VARHDRSZ_EXTERNAL : \
	 (VARATT_IS_1B(PTR) ? VARSIZE_1B(PTR)-VARHDRSZ_SHORT : \
	  VARSIZE_4B(PTR)-VARHDRSZ))

/* caution: this will not work on an external or compressed-in-line Datum */
/* caution: this will return a possibly unaligned pointer */
#define VARDATA_ANY(PTR) \
	 (VARATT_IS_1B(PTR) ? VARDATA_1B(PTR) : VARDATA_4B(PTR))

/* Decompressed size and compression method of a compressed-in-line Datum */
#define VARDATA_COMPRESSED_GET_EXTSIZE(PTR) \
	(((varattrib_4b *) (PTR))->va_compressed.va_tcinfo & VARLENA_EXTSIZE_MASK)
#define VARDATA_COMPRESSED_GET_COMPRESS_METHOD(PTR) \
	(((varattrib_4b *) (PTR))->va_compressed.va_tcinfo >> VARLENA_EXTSIZE_BITS)

/* Same for external Datums; but note argument is a struct varatt_external */
#define VARATT_EXTERNAL_GET_EXTSIZE(toast_pointer) \
	((toast_pointer).va_extinfo & VARLENA_EXTSIZE_MASK)
#define VARATT_EXTERNAL_GET_COMPRESS_METHOD(toast_pointer) \
	((toast_pointer).va_extinfo >> VARLENA_EXTSIZE_BITS)

#define VARATT_EXTERNAL_SET_SIZE_AND_COMPRESS_METHOD(toast_pointer, len, cm) \
	do { \
		Assert((cm) == TOAST_PGLZ_COMPRESSION_ID || \
			   (cm) == TOAST_LZ4_COMPRESSION_ID); \
		((toast_pointer).va_extinfo = \
			(len) | ((uint32) (cm) << VARLENA_EXTSIZE_BITS)); \
	} while (0)

/*
 * Testing whether an externally-stored value is compressed now requires
 * comparing size stored in va_extinfo (the actual length of the external data)
 * to rawsize (the original uncompressed datum's size).  The latter includes
 * VARHDRSZ overhead, the former doesn't.  We never use compression unless it
 * actually saves space, so we expect either equality or less-than.
 */
#define VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer) \
	(VARATT_EXTERNAL_GET_EXTSIZE(toast_pointer) < \
	 (toast_pointer).va_rawsize - VARHDRSZ)


/* ----------------------------------------------------------------
 *				Section 2:	Datum type + support macros
 * ----------------------------------------------------------------
 */

/*
 * A Datum contains either a value of a pass-by-value type or a pointer to a
 * value of a pass-by-reference type.  Therefore, we require:
 *
 * sizeof(Datum) == sizeof(void *) == 4 or 8
 *
 * The macros below and the analogous macros for other types should be used to
 * convert between a Datum and the appropriate C type.
 */

typedef uintptr_t Datum;

/*
 * A NullableDatum is used in places where both a Datum and its nullness needs
 * to be stored. This can be more efficient than storing datums and nullness
 * in separate arrays, due to better spatial locality, even if more space may
 * be wasted due to padding.
 */
typedef struct NullableDatum
{
#define FIELDNO_NULLABLE_DATUM_DATUM 0
	Datum		value;
#define FIELDNO_NULLABLE_DATUM_ISNULL 1
	bool		isnull;
	/* due to alignment padding this could be used for flags for free */
} NullableDatum;

#define SIZEOF_DATUM SIZEOF_VOID_P

/*
 * DatumGetBool
 *		Returns boolean value of a datum.
 *
 * Note: any nonzero value will be considered true.
 */

#define DatumGetBool(X) ((bool) ((X) != 0))

/*
 * BoolGetDatum
 *		Returns datum representation for a boolean.
 *
 * Note: any nonzero value will be considered true.
 */

#define BoolGetDatum(X) ((Datum) ((X) ? 1 : 0))

/*
 * DatumGetChar
 *		Returns character value of a datum.
 */

#define DatumGetChar(X) ((char) (X))

/*
 * CharGetDatum
 *		Returns datum representation for a character.
 */

#define CharGetDatum(X) ((Datum) (X))

/*
 * Int8GetDatum
 *		Returns datum representation for an 8-bit integer.
 */

#define Int8GetDatum(X) ((Datum) (X))

/*
 * DatumGetUInt8
 *		Returns 8-bit unsigned integer value of a datum.
 */

#define DatumGetUInt8(X) ((uint8) (X))

/*
 * UInt8GetDatum
 *		Returns datum representation for an 8-bit unsigned integer.
 */

#define UInt8GetDatum(X) ((Datum) (X))

/*
 * DatumGetInt16
 *		Returns 16-bit integer value of a datum.
 */

#define DatumGetInt16(X) ((int16) (X))

/*
 * Int16GetDatum
 *		Returns datum representation for a 16-bit integer.
 */

#define Int16GetDatum(X) ((Datum) (X))

/*
 * DatumGetUInt16
 *		Returns 16-bit unsigned integer value of a datum.
 */

#define DatumGetUInt16(X) ((uint16) (X))

/*
 * UInt16GetDatum
 *		Returns datum representation for a 16-bit unsigned integer.
 */

#define UInt16GetDatum(X) ((Datum) (X))

/*
 * DatumGetInt32
 *		Returns 32-bit integer value of a datum.
 */

#define DatumGetInt32(X) ((int32) (X))

/*
 * Int32GetDatum
 *		Returns datum representation for a 32-bit integer.
 */

#define Int32GetDatum(X) ((Datum) (X))

/*
 * DatumGetUInt32
 *		Returns 32-bit unsigned integer value of a datum.
 */

#define DatumGetUInt32(X) ((uint32) (X))

/*
 * UInt32GetDatum
 *		Returns datum representation for a 32-bit unsigned integer.
 */

#define UInt32GetDatum(X) ((Datum) (X))

/*
 * DatumGetObjectId
 *		Returns object identifier value of a datum.
 */

#define DatumGetObjectId(X) ((Oid) (X))

/*
 * ObjectIdGetDatum
 *		Returns datum representation for an object identifier.
 */

#define ObjectIdGetDatum(X) ((Datum) (X))

/*
 * DatumGetTransactionId
 *		Returns transaction identifier value of a datum.
 */

#define DatumGetTransactionId(X) ((TransactionId) (X))

/*
 * TransactionIdGetDatum
 *		Returns datum representation for a transaction identifier.
 */

#define TransactionIdGetDatum(X) ((Datum) (X))

/*
 * MultiXactIdGetDatum
 *		Returns datum representation for a multixact identifier.
 */

#define MultiXactIdGetDatum(X) ((Datum) (X))

/*
 * DatumGetCommandId
 *		Returns command identifier value of a datum.
 */

#define DatumGetCommandId(X) ((CommandId) (X))

/*
 * CommandIdGetDatum
 *		Returns datum representation for a command identifier.
 */

#define CommandIdGetDatum(X) ((Datum) (X))

/*
 * DatumGetPointer
 *		Returns pointer value of a datum.
 */

#define DatumGetPointer(X) ((Pointer) (X))

/*
 * PointerGetDatum
 *		Returns datum representation for a pointer.
 */

#define PointerGetDatum(X) ((Datum) (X))

/*
 * DatumGetCString
 *		Returns C string (null-terminated string) value of a datum.
 *
 * Note: C string is not a full-fledged Postgres type at present,
 * but type input functions use this conversion for their inputs.
 */

#define DatumGetCString(X) ((char *) DatumGetPointer(X))

/*
 * CStringGetDatum
 *		Returns datum representation for a C string (null-terminated string).
 *
 * Note: C string is not a full-fledged Postgres type at present,
 * but type output functions use this conversion for their outputs.
 * Note: CString is pass-by-reference; caller must ensure the pointed-to
 * value has adequate lifetime.
 */

#define CStringGetDatum(X) PointerGetDatum(X)

/*
 * DatumGetName
 *		Returns name value of a datum.
 */

#define DatumGetName(X) ((Name) DatumGetPointer(X))

/*
 * NameGetDatum
 *		Returns datum representation for a name.
 *
 * Note: Name is pass-by-reference; caller must ensure the pointed-to
 * value has adequate lifetime.
 */

#define NameGetDatum(X) CStringGetDatum(NameStr(*(X)))

/*
 * DatumGetInt64
 *		Returns 64-bit integer value of a datum.
 *
 * Note: this macro hides whether int64 is pass by value or by reference.
 */

#ifdef USE_FLOAT8_BYVAL
#define DatumGetInt64(X) ((int64) (X))
#else
#define DatumGetInt64(X) (* ((int64 *) DatumGetPointer(X)))
#endif

/*
 * Int64GetDatum
 *		Returns datum representation for a 64-bit integer.
 *
 * Note: if int64 is pass by reference, this function returns a reference
 * to palloc'd space.
 */

#ifdef USE_FLOAT8_BYVAL
#define Int64GetDatum(X) ((Datum) (X))
#else
extern Datum Int64GetDatum(int64 X);
#endif

/*
 * DatumGetUInt64
 *		Returns 64-bit unsigned integer value of a datum.
 *
 * Note: this macro hides whether int64 is pass by value or by reference.
 */

#ifdef USE_FLOAT8_BYVAL
#define DatumGetUInt64(X) ((uint64) (X))
#else
#define DatumGetUInt64(X) (* ((uint64 *) DatumGetPointer(X)))
#endif

/*
 * UInt64GetDatum
 *		Returns datum representation for a 64-bit unsigned integer.
 *
 * Note: if int64 is pass by reference, this function returns a reference
 * to palloc'd space.
 */

#ifdef USE_FLOAT8_BYVAL
#define UInt64GetDatum(X) ((Datum) (X))
#else
#define UInt64GetDatum(X) Int64GetDatum((int64) (X))
#endif

/*
 * Float <-> Datum conversions
 *
 * These have to be implemented as inline functions rather than macros, when
 * passing by value, because many machines pass int and float function
 * parameters/results differently; so we need to play weird games with unions.
 */

/*
 * DatumGetFloat4
 *		Returns 4-byte floating point value of a datum.
 */
static inline float4
DatumGetFloat4(Datum X)
{
	union
	{
		int32		value;
		float4		retval;
	}			myunion;

	myunion.value = DatumGetInt32(X);
	return myunion.retval;
}

/*
 * Float4GetDatum
 *		Returns datum representation for a 4-byte floating point number.
 */
static inline Datum
Float4GetDatum(float4 X)
{
	union
	{
		float4		value;
		int32		retval;
	}			myunion;

	myunion.value = X;
	return Int32GetDatum(myunion.retval);
}

/*
 * DatumGetFloat8
 *		Returns 8-byte floating point value of a datum.
 *
 * Note: this macro hides whether float8 is pass by value or by reference.
 */

#ifdef USE_FLOAT8_BYVAL
static inline float8
DatumGetFloat8(Datum X)
{
	union
	{
		int64		value;
		float8		retval;
	}			myunion;

	myunion.value = DatumGetInt64(X);
	return myunion.retval;
}
#else
#define DatumGetFloat8(X) (* ((float8 *) DatumGetPointer(X)))
#endif

/*
 * Float8GetDatum
 *		Returns datum representation for an 8-byte floating point number.
 *
 * Note: if float8 is pass by reference, this function returns a reference
 * to palloc'd space.
 */

#ifdef USE_FLOAT8_BYVAL
static inline Datum
Float8GetDatum(float8 X)
{
	union
	{
		float8		value;
		int64		retval;
	}			myunion;

	myunion.value = X;
	return Int64GetDatum(myunion.retval);
}
#else
extern Datum Float8GetDatum(float8 X);
#endif


/*
 * Int64GetDatumFast
 * Float8GetDatumFast
 *
 * These macros are intended to allow writing code that does not depend on
 * whether int64 and float8 are pass-by-reference types, while not
 * sacrificing performance when they are.  The argument must be a variable
 * that will exist and have the same value for as long as the Datum is needed.
 * In the pass-by-ref case, the address of the variable is taken to use as
 * the Datum.  In the pass-by-val case, these will be the same as the non-Fast
 * macros.
 */

#ifdef USE_FLOAT8_BYVAL
#define Int64GetDatumFast(X)  Int64GetDatum(X)
#define Float8GetDatumFast(X) Float8GetDatum(X)
#else
#define Int64GetDatumFast(X)  PointerGetDatum(&(X))
#define Float8GetDatumFast(X) PointerGetDatum(&(X))
#endif

#endif							/* POSTGRES_H */


#include <signal.h>
#include <unistd.h>
#include <sys/stat.h>
// #include "access/xact.h"
/*
 * xlogdefs.h
 *
 * Postgres write-ahead log manager record pointer and
 * timeline number definitions
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/xlogdefs.h
 */
#ifndef XLOG_DEFS_H
#define XLOG_DEFS_H

#include <fcntl.h>				/* need open() flags */

/*
 * Pointer to a location in the XLOG.  These pointers are 64 bits wide,
 * because we don't want them ever to overflow.
 */
typedef uint64 XLogRecPtr;

/*
 * Zero is used indicate an invalid pointer. Bootstrap skips the first possible
 * WAL segment, initializing the first WAL page at WAL segment size, so no XLOG
 * record can begin at zero.
 */
#define InvalidXLogRecPtr	0
#define XLogRecPtrIsInvalid(r)	((r) == InvalidXLogRecPtr)

/*
 * First LSN to use for "fake" LSNs.
 *
 * Values smaller than this can be used for special per-AM purposes.
 */
#define FirstNormalUnloggedLSN	((XLogRecPtr) 1000)

/*
 * Handy macro for printing XLogRecPtr in conventional format, e.g.,
 *
 * printf("%X/%X", LSN_FORMAT_ARGS(lsn));
 */
#define LSN_FORMAT_ARGS(lsn) (AssertVariableIsOfTypeMacro((lsn), XLogRecPtr), (uint32) ((lsn) >> 32)), ((uint32) (lsn))

/*
 * XLogSegNo - physical log file sequence number.
 */
typedef uint64 XLogSegNo;

/*
 * TimeLineID (TLI) - identifies different database histories to prevent
 * confusion after restoring a prior state of a database installation.
 * TLI does not change in a normal stop/restart of the database (including
 * crash-and-recover cases); but we must assign a new TLI after doing
 * a recovery to a prior state, a/k/a point-in-time recovery.  This makes
 * the new WAL logfile sequence we generate distinguishable from the
 * sequence that was generated in the previous incarnation.
 */
typedef uint32 TimeLineID;

/*
 * Replication origin id - this is located in this file to avoid having to
 * include origin.h in a bunch of xlog related places.
 */
typedef uint16 RepOriginId;

/*
 * This chunk of hackery attempts to determine which file sync methods
 * are available on the current platform, and to choose an appropriate
 * default method.  We assume that fsync() is always available, and that
 * configure determined whether fdatasync() is.
 */
#if defined(O_SYNC)
#define OPEN_SYNC_FLAG		O_SYNC
#elif defined(O_FSYNC)
#define OPEN_SYNC_FLAG		O_FSYNC
#endif

#if defined(O_DSYNC)
#if defined(OPEN_SYNC_FLAG)
/* O_DSYNC is distinct? */
#if O_DSYNC != OPEN_SYNC_FLAG
#define OPEN_DATASYNC_FLAG		O_DSYNC
#endif
#else							/* !defined(OPEN_SYNC_FLAG) */
/* Win32 only has O_DSYNC */
#define OPEN_DATASYNC_FLAG		O_DSYNC
#endif
#endif

#if defined(PLATFORM_DEFAULT_SYNC_METHOD)
#define DEFAULT_SYNC_METHOD		PLATFORM_DEFAULT_SYNC_METHOD
#elif defined(OPEN_DATASYNC_FLAG)
#define DEFAULT_SYNC_METHOD		SYNC_METHOD_OPEN_DSYNC
#elif defined(HAVE_FDATASYNC)
#define DEFAULT_SYNC_METHOD		SYNC_METHOD_FDATASYNC
#else
#define DEFAULT_SYNC_METHOD		SYNC_METHOD_FSYNC
#endif

#endif							/* XLOG_DEFS_H */

// #include "access/xlogutils.h"
// #include "access/xloginsert.h"
// #if PG_VERSION_NUM >= 150000
// #include "access/xlogrecovery.h"
// #endif
// #include "storage/fd.h"
// #include "storage/latch.h"
// #include "miscadmin.h"
// #include "pgstat.h"
// #include "access/xlog.h"
// #include "libpq/pqformat.h"
// #include "replication/slot.h"
// #include "replication/walreceiver.h"
// #include "postmaster/bgworker.h"
// #include "postmaster/interrupt.h"
// #include "postmaster/postmaster.h"
// #include "storage/pmsignal.h"
// #include "storage/proc.h"
// #include "storage/ipc.h"
// #include "storage/lwlock.h"
// #include "storage/shmem.h"
// #include "storage/spin.h"
// #include "tcop/tcopprot.h"
// #include "utils/builtins.h"
// #include "utils/guc.h"
// #include "utils/memutils.h"
// #include "utils/ps_status.h"

/*-------------------------------------------------------------------------
 *
 * timestamp.h
 *	  Timestamp and Interval typedefs and related macros.
 *
 * Note: this file must be includable in both frontend and backend contexts.
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/datatype/timestamp.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DATATYPE_TIMESTAMP_H
#define DATATYPE_TIMESTAMP_H

/*
 * Timestamp represents absolute time.
 *
 * Interval represents delta time. Keep track of months (and years), days,
 * and hours/minutes/seconds separately since the elapsed time spanned is
 * unknown until instantiated relative to an absolute time.
 *
 * Note that Postgres uses "time interval" to mean a bounded interval,
 * consisting of a beginning and ending time, not a time span - thomas 97/03/20
 *
 * Timestamps, as well as the h/m/s fields of intervals, are stored as
 * int64 values with units of microseconds.  (Once upon a time they were
 * double values with units of seconds.)
 *
 * TimeOffset and fsec_t are convenience typedefs for temporary variables.
 * Do not use fsec_t in values stored on-disk.
 * Also, fsec_t is only meant for *fractional* seconds; beware of overflow
 * if the value you need to store could be many seconds.
 */

typedef int64 Timestamp;
typedef int64 TimestampTz;
typedef int64 TimeOffset;
typedef int32 fsec_t;			/* fractional seconds (in microseconds) */


/*
 * Storage format for type interval.
 */
typedef struct
{
	TimeOffset	time;			/* all time units other than days, months and
								 * years */
	int32		day;			/* days, after time for alignment */
	int32		month;			/* months and years, after time for alignment */
} Interval;

/*
 * Data structure representing a broken-down interval.
 *
 * For historical reasons, this is modeled on struct pg_tm for timestamps.
 * Unlike the situation for timestamps, there's no magic interpretation
 * needed for months or years: they're just zero or not.  Note that fields
 * can be negative; however, because of the divisions done while converting
 * from struct Interval, only tm_mday could be INT_MIN.  This is important
 * because we may need to negate the values in some code paths.
 */
struct pg_itm
{
	int			tm_usec;
	int			tm_sec;
	int			tm_min;
	int64		tm_hour;		/* needs to be wide */
	int			tm_mday;
	int			tm_mon;
	int			tm_year;
};

/*
 * Data structure for decoding intervals.  We could just use struct pg_itm,
 * but then the requirement for tm_usec to be 64 bits would propagate to
 * places where it's not really needed.  Also, omitting the fields that
 * aren't used during decoding seems like a good error-prevention measure.
 */
struct pg_itm_in
{
	int64		tm_usec;		/* needs to be wide */
	int			tm_mday;
	int			tm_mon;
	int			tm_year;
};


/* Limits on the "precision" option (typmod) for these data types */
#define MAX_TIMESTAMP_PRECISION 6
#define MAX_INTERVAL_PRECISION 6

/*
 *	Round off to MAX_TIMESTAMP_PRECISION decimal places.
 *	Note: this is also used for rounding off intervals.
 */
#define TS_PREC_INV 1000000.0
#define TSROUND(j) (rint(((double) (j)) * TS_PREC_INV) / TS_PREC_INV)


/*
 * Assorted constants for datetime-related calculations
 */

#define DAYS_PER_YEAR	365.25	/* assumes leap year every four years */
#define MONTHS_PER_YEAR 12
/*
 *	DAYS_PER_MONTH is very imprecise.  The more accurate value is
 *	365.2425/12 = 30.436875, or '30 days 10:29:06'.  Right now we only
 *	return an integral number of days, but someday perhaps we should
 *	also return a 'time' value to be used as well.  ISO 8601 suggests
 *	30 days.
 */
#define DAYS_PER_MONTH	30		/* assumes exactly 30 days per month */
#define HOURS_PER_DAY	24		/* assume no daylight savings time changes */

/*
 *	This doesn't adjust for uneven daylight savings time intervals or leap
 *	seconds, and it crudely estimates leap years.  A more accurate value
 *	for days per years is 365.2422.
 */
#define SECS_PER_YEAR	(36525 * 864)	/* avoid floating-point computation */
#define SECS_PER_DAY	86400
#define SECS_PER_HOUR	3600
#define SECS_PER_MINUTE 60
#define MINS_PER_HOUR	60

#define USECS_PER_DAY	INT64CONST(86400000000)
#define USECS_PER_HOUR	INT64CONST(3600000000)
#define USECS_PER_MINUTE INT64CONST(60000000)
#define USECS_PER_SEC	INT64CONST(1000000)

/*
 * We allow numeric timezone offsets up to 15:59:59 either way from Greenwich.
 * Currently, the record holders for wackiest offsets in actual use are zones
 * Asia/Manila, at -15:56:00 until 1844, and America/Metlakatla, at +15:13:42
 * until 1867.  If we were to reject such values we would fail to dump and
 * restore old timestamptz values with these zone settings.
 */
#define MAX_TZDISP_HOUR		15	/* maximum allowed hour part */
#define TZDISP_LIMIT		((MAX_TZDISP_HOUR + 1) * SECS_PER_HOUR)

/*
 * DT_NOBEGIN represents timestamp -infinity; DT_NOEND represents +infinity
 */
#define DT_NOBEGIN		PG_INT64_MIN
#define DT_NOEND		PG_INT64_MAX

#define TIMESTAMP_NOBEGIN(j)	\
	do {(j) = DT_NOBEGIN;} while (0)

#define TIMESTAMP_IS_NOBEGIN(j) ((j) == DT_NOBEGIN)

#define TIMESTAMP_NOEND(j)		\
	do {(j) = DT_NOEND;} while (0)

#define TIMESTAMP_IS_NOEND(j)	((j) == DT_NOEND)

#define TIMESTAMP_NOT_FINITE(j) (TIMESTAMP_IS_NOBEGIN(j) || TIMESTAMP_IS_NOEND(j))


/*
 * Julian date support.
 *
 * date2j() and j2date() nominally handle the Julian date range 0..INT_MAX,
 * or 4714-11-24 BC to 5874898-06-03 AD.  In practice, date2j() will work and
 * give correct negative Julian dates for dates before 4714-11-24 BC as well.
 * We rely on it to do so back to 4714-11-01 BC.  Allowing at least one day's
 * slop is necessary so that timestamp rotation doesn't produce dates that
 * would be rejected on input.  For example, '4714-11-24 00:00 GMT BC' is a
 * legal timestamptz value, but in zones east of Greenwich it would print as
 * sometime in the afternoon of 4714-11-23 BC; if we couldn't process such a
 * date we'd have a dump/reload failure.  So the idea is for IS_VALID_JULIAN
 * to accept a slightly wider range of dates than we really support, and
 * then we apply the exact checks in IS_VALID_DATE or IS_VALID_TIMESTAMP,
 * after timezone rotation if any.  To save a few cycles, we can make
 * IS_VALID_JULIAN check only to the month boundary, since its exact cutoffs
 * are not very critical in this scheme.
 *
 * It is correct that JULIAN_MINYEAR is -4713, not -4714; it is defined to
 * allow easy comparison to tm_year values, in which we follow the convention
 * that tm_year <= 0 represents abs(tm_year)+1 BC.
 */

#define JULIAN_MINYEAR (-4713)
#define JULIAN_MINMONTH (11)
#define JULIAN_MINDAY (24)
#define JULIAN_MAXYEAR (5874898)
#define JULIAN_MAXMONTH (6)
#define JULIAN_MAXDAY (3)

#define IS_VALID_JULIAN(y,m,d) \
	(((y) > JULIAN_MINYEAR || \
	  ((y) == JULIAN_MINYEAR && ((m) >= JULIAN_MINMONTH))) && \
	 ((y) < JULIAN_MAXYEAR || \
	  ((y) == JULIAN_MAXYEAR && ((m) < JULIAN_MAXMONTH))))

/* Julian-date equivalents of Day 0 in Unix and Postgres reckoning */
#define UNIX_EPOCH_JDATE		2440588 /* == date2j(1970, 1, 1) */
#define POSTGRES_EPOCH_JDATE	2451545 /* == date2j(2000, 1, 1) */

/*
 * Range limits for dates and timestamps.
 *
 * We have traditionally allowed Julian day zero as a valid datetime value,
 * so that is the lower bound for both dates and timestamps.
 *
 * The upper limit for dates is 5874897-12-31, which is a bit less than what
 * the Julian-date code can allow.  For timestamps, the upper limit is
 * 294276-12-31.  The int64 overflow limit would be a few days later; again,
 * leaving some slop avoids worries about corner-case overflow, and provides
 * a simpler user-visible definition.
 */

/* First allowed date, and first disallowed date, in Julian-date form */
#define DATETIME_MIN_JULIAN (0)
#define DATE_END_JULIAN (2147483494)	/* == date2j(JULIAN_MAXYEAR, 1, 1) */
#define TIMESTAMP_END_JULIAN (109203528)	/* == date2j(294277, 1, 1) */

/* Timestamp limits */
#define MIN_TIMESTAMP	INT64CONST(-211813488000000000)
/* == (DATETIME_MIN_JULIAN - POSTGRES_EPOCH_JDATE) * USECS_PER_DAY */
#define END_TIMESTAMP	INT64CONST(9223371331200000000)
/* == (TIMESTAMP_END_JULIAN - POSTGRES_EPOCH_JDATE) * USECS_PER_DAY */

/* Range-check a date (given in Postgres, not Julian, numbering) */
#define IS_VALID_DATE(d) \
	((DATETIME_MIN_JULIAN - POSTGRES_EPOCH_JDATE) <= (d) && \
	 (d) < (DATE_END_JULIAN - POSTGRES_EPOCH_JDATE))

/* Range-check a timestamp */
#define IS_VALID_TIMESTAMP(t)  (MIN_TIMESTAMP <= (t) && (t) < END_TIMESTAMP)

#endif							/* DATATYPE_TIMESTAMP_H */

// #include "utils/timestamp.h"

// #include "neon.h"

// #include "postgres.h"

// #include "access/timeline.h"
// #include "access/xlogutils.h"
// #include "common/logging.h"
// #include "common/ip.h"
// #include "funcapi.h"
// #include "libpq/libpq.h"
// #include "libpq/pqformat.h"
// #include "miscadmin.h"
// #include "postmaster/interrupt.h"
// #include "replication/slot.h"
// #include "walproposer_utils.h"
// #include "replication/walsender_private.h"

// #include "storage/ipc.h"
// #include "utils/builtins.h"
// #include "utils/ps_status.h"

// #include "libpq-fe.h"
#include <netinet/tcp.h>
#include <unistd.h>

// #if PG_VERSION_NUM >= 150000
// #include "access/xlogutils.h"
// #include "access/xlogrecovery.h"
// #endif

/*
 * These variables are used similarly to openLogFile/SegNo,
 * but for walproposer to write the XLOG during recovery. walpropFileTLI is the TimeLineID
 * corresponding the filename of walpropFile.
 */
static int	walpropFile = -1;
static TimeLineID walpropFileTLI = 0;
static XLogSegNo walpropSegNo = 0;

/* START cloned file-local variables and functions from walsender.c */

/*
 * xlogreader used for replication.  Note that a WAL sender doing physical
 * replication does not need xlogreader to read WAL, but it needs one to
 * keep a state of its work.
 */
// static XLogReaderState *xlogreader = NULL;
typedef struct XLogReaderState XLogReaderState;
struct XLogReaderState
{
};

/*-------------------------------------------------------------------------
 *
 * pg_bswap.h
 *	  Byte swapping.
 *
 * Macros for reversing the byte order of 16, 32 and 64-bit unsigned integers.
 * For example, 0xAABBCCDD becomes 0xDDCCBBAA.  These are just wrappers for
 * built-in functions provided by the compiler where support exists.
 *
 * Note that all of these functions accept unsigned integers as arguments and
 * return the same.  Use caution when using these wrapper macros with signed
 * integers.
 *
 * Copyright (c) 2015-2022, PostgreSQL Global Development Group
 *
 * src/include/port/pg_bswap.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_BSWAP_H
#define PG_BSWAP_H


/*
 * In all supported versions msvc provides _byteswap_* functions in stdlib.h,
 * already included by c.h.
 */


/* implementation of uint16 pg_bswap16(uint16) */
#if defined(HAVE__BUILTIN_BSWAP16)

#define pg_bswap16(x) __builtin_bswap16(x)

#elif defined(_MSC_VER)

#define pg_bswap16(x) _byteswap_ushort(x)

#else

static inline uint16
pg_bswap16(uint16 x)
{
	return
		((x << 8) & 0xff00) |
		((x >> 8) & 0x00ff);
}

#endif							/* HAVE__BUILTIN_BSWAP16 */


/* implementation of uint32 pg_bswap32(uint32) */
#if defined(HAVE__BUILTIN_BSWAP32)

#define pg_bswap32(x) __builtin_bswap32(x)

#elif defined(_MSC_VER)

#define pg_bswap32(x) _byteswap_ulong(x)

#else

static inline uint32
pg_bswap32(uint32 x)
{
	return
		((x << 24) & 0xff000000) |
		((x << 8) & 0x00ff0000) |
		((x >> 8) & 0x0000ff00) |
		((x >> 24) & 0x000000ff);
}

#endif							/* HAVE__BUILTIN_BSWAP32 */


/* implementation of uint64 pg_bswap64(uint64) */
#if defined(HAVE__BUILTIN_BSWAP64)

#define pg_bswap64(x) __builtin_bswap64(x)


#elif defined(_MSC_VER)

#define pg_bswap64(x) _byteswap_uint64(x)

#else

static inline uint64
pg_bswap64(uint64 x)
{
	return
		((x << 56) & UINT64CONST(0xff00000000000000)) |
		((x << 40) & UINT64CONST(0x00ff000000000000)) |
		((x << 24) & UINT64CONST(0x0000ff0000000000)) |
		((x << 8) & UINT64CONST(0x000000ff00000000)) |
		((x >> 8) & UINT64CONST(0x00000000ff000000)) |
		((x >> 24) & UINT64CONST(0x0000000000ff0000)) |
		((x >> 40) & UINT64CONST(0x000000000000ff00)) |
		((x >> 56) & UINT64CONST(0x00000000000000ff));
}
#endif							/* HAVE__BUILTIN_BSWAP64 */


/*
 * Portable and fast equivalents for ntohs, ntohl, htons, htonl,
 * additionally extended to 64 bits.
 */
#ifdef WORDS_BIGENDIAN

#define pg_hton16(x)		(x)
#define pg_hton32(x)		(x)
#define pg_hton64(x)		(x)

#define pg_ntoh16(x)		(x)
#define pg_ntoh32(x)		(x)
#define pg_ntoh64(x)		(x)

#else

#define pg_hton16(x)		pg_bswap16(x)
#define pg_hton32(x)		pg_bswap32(x)
#define pg_hton64(x)		pg_bswap64(x)

#define pg_ntoh16(x)		pg_bswap16(x)
#define pg_ntoh32(x)		pg_bswap32(x)
#define pg_ntoh64(x)		pg_bswap64(x)

#endif							/* WORDS_BIGENDIAN */


/*
 * Rearrange the bytes of a Datum from big-endian order into the native byte
 * order.  On big-endian machines, this does nothing at all.  Note that the C
 * type Datum is an unsigned integer type on all platforms.
 *
 * One possible application of the DatumBigEndianToNative() macro is to make
 * bitwise comparisons cheaper.  A simple 3-way comparison of Datums
 * transformed by the macro (based on native, unsigned comparisons) will return
 * the same result as a memcmp() of the corresponding original Datums, but can
 * be much cheaper.  It's generally safe to do this on big-endian systems
 * without any special transformation occurring first.
 *
 * If SIZEOF_DATUM is not defined, then postgres.h wasn't included and these
 * macros probably shouldn't be used, so we define nothing.  Note that
 * SIZEOF_DATUM == 8 would evaluate as 0 == 8 in that case, potentially
 * leading to the wrong implementation being selected and confusing errors, so
 * defining nothing is safest.
 */
#ifdef SIZEOF_DATUM
#ifdef WORDS_BIGENDIAN
#define		DatumBigEndianToNative(x)	(x)
#else							/* !WORDS_BIGENDIAN */
#if SIZEOF_DATUM == 8
#define		DatumBigEndianToNative(x)	pg_bswap64(x)
#else							/* SIZEOF_DATUM != 8 */
#define		DatumBigEndianToNative(x)	pg_bswap32(x)
#endif							/* SIZEOF_DATUM == 8 */
#endif							/* WORDS_BIGENDIAN */
#endif							/* SIZEOF_DATUM */

#endif							/* PG_BSWAP_H */


/*-------------------------------------------------------------------------
 *
 * stringinfo.h
 *	  Declarations/definitions for "StringInfo" functions.
 *
 * StringInfo provides an extensible string data type (currently limited to a
 * length of 1GB).  It can be used to buffer either ordinary C strings
 * (null-terminated text) or arbitrary binary data.  All storage is allocated
 * with palloc() (falling back to malloc in frontend code).
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/lib/stringinfo.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef STRINGINFO_H
#define STRINGINFO_H

/*-------------------------
 * StringInfoData holds information about an extensible string.
 *		data	is the current buffer for the string (allocated with palloc).
 *		len		is the current string length.  There is guaranteed to be
 *				a terminating '\0' at data[len], although this is not very
 *				useful when the string holds binary data rather than text.
 *		maxlen	is the allocated size in bytes of 'data', i.e. the maximum
 *				string size (including the terminating '\0' char) that we can
 *				currently store in 'data' without having to reallocate
 *				more space.  We must always have maxlen > len.
 *		cursor	is initialized to zero by makeStringInfo or initStringInfo,
 *				but is not otherwise touched by the stringinfo.c routines.
 *				Some routines use it to scan through a StringInfo.
 *-------------------------
 */
typedef struct StringInfoData
{
	char	   *data;
	int			len;
	int			maxlen;
	int			cursor;
} StringInfoData;

typedef StringInfoData *StringInfo;


/*------------------------
 * There are two ways to create a StringInfo object initially:
 *
 * StringInfo stringptr = makeStringInfo();
 *		Both the StringInfoData and the data buffer are palloc'd.
 *
 * StringInfoData string;
 * initStringInfo(&string);
 *		The data buffer is palloc'd but the StringInfoData is just local.
 *		This is the easiest approach for a StringInfo object that will
 *		only live as long as the current routine.
 *
 * To destroy a StringInfo, pfree() the data buffer, and then pfree() the
 * StringInfoData if it was palloc'd.  There's no special support for this.
 *
 * NOTE: some routines build up a string using StringInfo, and then
 * release the StringInfoData but return the data string itself to their
 * caller.  At that point the data string looks like a plain palloc'd
 * string.
 *-------------------------
 */

/*------------------------
 * makeStringInfo
 * Create an empty 'StringInfoData' & return a pointer to it.
 */
extern StringInfo makeStringInfo(void);

/*------------------------
 * initStringInfo
 * Initialize a StringInfoData struct (with previously undefined contents)
 * to describe an empty string.
 */
extern void initStringInfo(StringInfo str);

/*------------------------
 * resetStringInfo
 * Clears the current content of the StringInfo, if any. The
 * StringInfo remains valid.
 */
extern void resetStringInfo(StringInfo str);

/*------------------------
 * appendStringInfo
 * Format text data under the control of fmt (an sprintf-style format string)
 * and append it to whatever is already in str.  More space is allocated
 * to str if necessary.  This is sort of like a combination of sprintf and
 * strcat.
 */
extern void appendStringInfo(StringInfo str, const char *fmt,...) pg_attribute_printf(2, 3);

/*------------------------
 * appendStringInfoVA
 * Attempt to format text data under the control of fmt (an sprintf-style
 * format string) and append it to whatever is already in str.  If successful
 * return zero; if not (because there's not enough space), return an estimate
 * of the space needed, without modifying str.  Typically the caller should
 * pass the return value to enlargeStringInfo() before trying again; see
 * appendStringInfo for standard usage pattern.
 */
extern int	appendStringInfoVA(StringInfo str, const char *fmt, va_list args) pg_attribute_printf(2, 0);

/*------------------------
 * appendStringInfoString
 * Append a null-terminated string to str.
 * Like appendStringInfo(str, "%s", s) but faster.
 */
extern void appendStringInfoString(StringInfo str, const char *s);

/*------------------------
 * appendStringInfoChar
 * Append a single byte to str.
 * Like appendStringInfo(str, "%c", ch) but much faster.
 */
extern void appendStringInfoChar(StringInfo str, char ch);

/*------------------------
 * appendStringInfoCharMacro
 * As above, but a macro for even more speed where it matters.
 * Caution: str argument will be evaluated multiple times.
 */
#define appendStringInfoCharMacro(str,ch) \
	(((str)->len + 1 >= (str)->maxlen) ? \
	 appendStringInfoChar(str, ch) : \
	 (void)((str)->data[(str)->len] = (ch), (str)->data[++(str)->len] = '\0'))

/*------------------------
 * appendStringInfoSpaces
 * Append a given number of spaces to str.
 */
extern void appendStringInfoSpaces(StringInfo str, int count);

/*------------------------
 * appendBinaryStringInfo
 * Append arbitrary binary data to a StringInfo, allocating more space
 * if necessary.
 */
extern void appendBinaryStringInfo(StringInfo str,
								   const char *data, int datalen);

/*------------------------
 * appendBinaryStringInfoNT
 * Append arbitrary binary data to a StringInfo, allocating more space
 * if necessary. Does not ensure a trailing null-byte exists.
 */
extern void appendBinaryStringInfoNT(StringInfo str,
									 const char *data, int datalen);

/*------------------------
 * enlargeStringInfo
 * Make sure a StringInfo's buffer can hold at least 'needed' more bytes.
 */
extern void enlargeStringInfo(StringInfo str, int needed);

#endif							/* STRINGINFO_H */

/*-------------------------------------------------------------------------
 *
 * pqformat.h
 *		Definitions for formatting and parsing frontend/backend messages
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/libpq/pqformat.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PQFORMAT_H
#define PQFORMAT_H

// #include "lib/stringinfo.h"
// #include "mb/pg_wchar.h"
// #include "port/pg_bswap.h"

extern void pq_beginmessage(StringInfo buf, char msgtype);
extern void pq_beginmessage_reuse(StringInfo buf, char msgtype);
extern void pq_endmessage(StringInfo buf);
extern void pq_endmessage_reuse(StringInfo buf);

extern void pq_sendbytes(StringInfo buf, const char *data, int datalen);
extern void pq_sendcountedtext(StringInfo buf, const char *str, int slen,
							   bool countincludesself);
extern void pq_sendtext(StringInfo buf, const char *str, int slen);
extern void pq_sendstring(StringInfo buf, const char *str);
extern void pq_send_ascii_string(StringInfo buf, const char *str);
extern void pq_sendfloat4(StringInfo buf, float4 f);
extern void pq_sendfloat8(StringInfo buf, float8 f);

/*
 * Append a [u]int8 to a StringInfo buffer, which already has enough space
 * preallocated.
 *
 * The use of pg_restrict allows the compiler to optimize the code based on
 * the assumption that buf, buf->len, buf->data and *buf->data don't
 * overlap. Without the annotation buf->len etc cannot be kept in a register
 * over subsequent pq_writeintN calls.
 *
 * The use of StringInfoData * rather than StringInfo is due to MSVC being
 * overly picky and demanding a * before a restrict.
 */
static inline void
pq_writeint8(StringInfoData *pg_restrict buf, uint8 i)
{
	uint8		ni = i;

	Assert(buf->len + (int) sizeof(uint8) <= buf->maxlen);
	memcpy((char *pg_restrict) (buf->data + buf->len), &ni, sizeof(uint8));
	buf->len += sizeof(uint8);
}

/*
 * Append a [u]int16 to a StringInfo buffer, which already has enough space
 * preallocated.
 */
static inline void
pq_writeint16(StringInfoData *pg_restrict buf, uint16 i)
{
	uint16		ni = pg_hton16(i);

	Assert(buf->len + (int) sizeof(uint16) <= buf->maxlen);
	memcpy((char *pg_restrict) (buf->data + buf->len), &ni, sizeof(uint16));
	buf->len += sizeof(uint16);
}

/*
 * Append a [u]int32 to a StringInfo buffer, which already has enough space
 * preallocated.
 */
static inline void
pq_writeint32(StringInfoData *pg_restrict buf, uint32 i)
{
	uint32		ni = pg_hton32(i);

	Assert(buf->len + (int) sizeof(uint32) <= buf->maxlen);
	memcpy((char *pg_restrict) (buf->data + buf->len), &ni, sizeof(uint32));
	buf->len += sizeof(uint32);
}

/*
 * Append a [u]int64 to a StringInfo buffer, which already has enough space
 * preallocated.
 */
static inline void
pq_writeint64(StringInfoData *pg_restrict buf, uint64 i)
{
	uint64		ni = pg_hton64(i);

	Assert(buf->len + (int) sizeof(uint64) <= buf->maxlen);
	memcpy((char *pg_restrict) (buf->data + buf->len), &ni, sizeof(uint64));
	buf->len += sizeof(uint64);
}

/* append a binary [u]int8 to a StringInfo buffer */
static inline void
pq_sendint8(StringInfo buf, uint8 i)
{
	enlargeStringInfo(buf, sizeof(uint8));
	pq_writeint8(buf, i);
}

/* append a binary [u]int16 to a StringInfo buffer */
static inline void
pq_sendint16(StringInfo buf, uint16 i)
{
	enlargeStringInfo(buf, sizeof(uint16));
	pq_writeint16(buf, i);
}

/* append a binary [u]int32 to a StringInfo buffer */
static inline void
pq_sendint32(StringInfo buf, uint32 i)
{
	enlargeStringInfo(buf, sizeof(uint32));
	pq_writeint32(buf, i);
}

/* append a binary [u]int64 to a StringInfo buffer */
static inline void
pq_sendint64(StringInfo buf, uint64 i)
{
	enlargeStringInfo(buf, sizeof(uint64));
	pq_writeint64(buf, i);
}

/* append a binary byte to a StringInfo buffer */
static inline void
pq_sendbyte(StringInfo buf, uint8 byt)
{
	pq_sendint8(buf, byt);
}

/*
 * Append a binary integer to a StringInfo buffer
 *
 * This function is deprecated; prefer use of the functions above.
 */
static inline void
pq_sendint(StringInfo buf, uint32 i, int b)
{
	switch (b)
	{
		case 1:
			pq_sendint8(buf, (uint8) i);
			break;
		case 2:
			pq_sendint16(buf, (uint16) i);
			break;
		case 4:
			pq_sendint32(buf, (uint32) i);
			break;
		default:
			elog(ERROR, "unsupported integer size %d", b);
			break;
	}
}


extern void pq_begintypsend(StringInfo buf);
extern bytea *pq_endtypsend(StringInfo buf);

extern void pq_puttextmessage(char msgtype, const char *str);
extern void pq_putemptymessage(char msgtype);

extern int	pq_getmsgbyte(StringInfo msg);
extern unsigned int pq_getmsgint(StringInfo msg, int b);
extern int64 pq_getmsgint64(StringInfo msg);
extern float4 pq_getmsgfloat4(StringInfo msg);
extern float8 pq_getmsgfloat8(StringInfo msg);
extern const char *pq_getmsgbytes(StringInfo msg, int datalen);
extern void pq_copymsgbytes(StringInfo msg, char *buf, int datalen);
extern char *pq_getmsgtext(StringInfo msg, int rawbytes, int *nbytes);
extern const char *pq_getmsgstring(StringInfo msg);
extern const char *pq_getmsgrawstring(StringInfo msg);
extern void pq_getmsgend(StringInfo msg);

#endif							/* PQFORMAT_H */

/* autogenerated from src/backend/utils/errcodes.txt, do not edit */
/* there is deliberately not an #ifndef ERRCODES_H here */

/* Class 00 - Successful Completion */
#define ERRCODE_SUCCESSFUL_COMPLETION MAKE_SQLSTATE('0','0','0','0','0')

/* Class 01 - Warning */
#define ERRCODE_WARNING MAKE_SQLSTATE('0','1','0','0','0')
#define ERRCODE_WARNING_DYNAMIC_RESULT_SETS_RETURNED MAKE_SQLSTATE('0','1','0','0','C')
#define ERRCODE_WARNING_IMPLICIT_ZERO_BIT_PADDING MAKE_SQLSTATE('0','1','0','0','8')
#define ERRCODE_WARNING_NULL_VALUE_ELIMINATED_IN_SET_FUNCTION MAKE_SQLSTATE('0','1','0','0','3')
#define ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED MAKE_SQLSTATE('0','1','0','0','7')
#define ERRCODE_WARNING_PRIVILEGE_NOT_REVOKED MAKE_SQLSTATE('0','1','0','0','6')
#define ERRCODE_WARNING_STRING_DATA_RIGHT_TRUNCATION MAKE_SQLSTATE('0','1','0','0','4')
#define ERRCODE_WARNING_DEPRECATED_FEATURE MAKE_SQLSTATE('0','1','P','0','1')

/* Class 02 - No Data (this is also a warning class per the SQL standard) */
#define ERRCODE_NO_DATA MAKE_SQLSTATE('0','2','0','0','0')
#define ERRCODE_NO_ADDITIONAL_DYNAMIC_RESULT_SETS_RETURNED MAKE_SQLSTATE('0','2','0','0','1')

/* Class 03 - SQL Statement Not Yet Complete */
#define ERRCODE_SQL_STATEMENT_NOT_YET_COMPLETE MAKE_SQLSTATE('0','3','0','0','0')

/* Class 08 - Connection Exception */
#define ERRCODE_CONNECTION_EXCEPTION MAKE_SQLSTATE('0','8','0','0','0')
#define ERRCODE_CONNECTION_DOES_NOT_EXIST MAKE_SQLSTATE('0','8','0','0','3')
#define ERRCODE_CONNECTION_FAILURE MAKE_SQLSTATE('0','8','0','0','6')
#define ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION MAKE_SQLSTATE('0','8','0','0','1')
#define ERRCODE_SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION MAKE_SQLSTATE('0','8','0','0','4')
#define ERRCODE_TRANSACTION_RESOLUTION_UNKNOWN MAKE_SQLSTATE('0','8','0','0','7')
#define ERRCODE_PROTOCOL_VIOLATION MAKE_SQLSTATE('0','8','P','0','1')

/* Class 09 - Triggered Action Exception */
#define ERRCODE_TRIGGERED_ACTION_EXCEPTION MAKE_SQLSTATE('0','9','0','0','0')

/* Class 0A - Feature Not Supported */
#define ERRCODE_FEATURE_NOT_SUPPORTED MAKE_SQLSTATE('0','A','0','0','0')

/* Class 0B - Invalid Transaction Initiation */
#define ERRCODE_INVALID_TRANSACTION_INITIATION MAKE_SQLSTATE('0','B','0','0','0')

/* Class 0F - Locator Exception */
#define ERRCODE_LOCATOR_EXCEPTION MAKE_SQLSTATE('0','F','0','0','0')
#define ERRCODE_L_E_INVALID_SPECIFICATION MAKE_SQLSTATE('0','F','0','0','1')

/* Class 0L - Invalid Grantor */
#define ERRCODE_INVALID_GRANTOR MAKE_SQLSTATE('0','L','0','0','0')
#define ERRCODE_INVALID_GRANT_OPERATION MAKE_SQLSTATE('0','L','P','0','1')

/* Class 0P - Invalid Role Specification */
#define ERRCODE_INVALID_ROLE_SPECIFICATION MAKE_SQLSTATE('0','P','0','0','0')

/* Class 0Z - Diagnostics Exception */
#define ERRCODE_DIAGNOSTICS_EXCEPTION MAKE_SQLSTATE('0','Z','0','0','0')
#define ERRCODE_STACKED_DIAGNOSTICS_ACCESSED_WITHOUT_ACTIVE_HANDLER MAKE_SQLSTATE('0','Z','0','0','2')

/* Class 20 - Case Not Found */
#define ERRCODE_CASE_NOT_FOUND MAKE_SQLSTATE('2','0','0','0','0')

/* Class 21 - Cardinality Violation */
#define ERRCODE_CARDINALITY_VIOLATION MAKE_SQLSTATE('2','1','0','0','0')

/* Class 22 - Data Exception */
#define ERRCODE_DATA_EXCEPTION MAKE_SQLSTATE('2','2','0','0','0')
#define ERRCODE_ARRAY_ELEMENT_ERROR MAKE_SQLSTATE('2','2','0','2','E')
#define ERRCODE_ARRAY_SUBSCRIPT_ERROR MAKE_SQLSTATE('2','2','0','2','E')
#define ERRCODE_CHARACTER_NOT_IN_REPERTOIRE MAKE_SQLSTATE('2','2','0','2','1')
#define ERRCODE_DATETIME_FIELD_OVERFLOW MAKE_SQLSTATE('2','2','0','0','8')
#define ERRCODE_DATETIME_VALUE_OUT_OF_RANGE MAKE_SQLSTATE('2','2','0','0','8')
#define ERRCODE_DIVISION_BY_ZERO MAKE_SQLSTATE('2','2','0','1','2')
#define ERRCODE_ERROR_IN_ASSIGNMENT MAKE_SQLSTATE('2','2','0','0','5')
#define ERRCODE_ESCAPE_CHARACTER_CONFLICT MAKE_SQLSTATE('2','2','0','0','B')
#define ERRCODE_INDICATOR_OVERFLOW MAKE_SQLSTATE('2','2','0','2','2')
#define ERRCODE_INTERVAL_FIELD_OVERFLOW MAKE_SQLSTATE('2','2','0','1','5')
#define ERRCODE_INVALID_ARGUMENT_FOR_LOG MAKE_SQLSTATE('2','2','0','1','E')
#define ERRCODE_INVALID_ARGUMENT_FOR_NTILE MAKE_SQLSTATE('2','2','0','1','4')
#define ERRCODE_INVALID_ARGUMENT_FOR_NTH_VALUE MAKE_SQLSTATE('2','2','0','1','6')
#define ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION MAKE_SQLSTATE('2','2','0','1','F')
#define ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION MAKE_SQLSTATE('2','2','0','1','G')
#define ERRCODE_INVALID_CHARACTER_VALUE_FOR_CAST MAKE_SQLSTATE('2','2','0','1','8')
#define ERRCODE_INVALID_DATETIME_FORMAT MAKE_SQLSTATE('2','2','0','0','7')
#define ERRCODE_INVALID_ESCAPE_CHARACTER MAKE_SQLSTATE('2','2','0','1','9')
#define ERRCODE_INVALID_ESCAPE_OCTET MAKE_SQLSTATE('2','2','0','0','D')
#define ERRCODE_INVALID_ESCAPE_SEQUENCE MAKE_SQLSTATE('2','2','0','2','5')
#define ERRCODE_NONSTANDARD_USE_OF_ESCAPE_CHARACTER MAKE_SQLSTATE('2','2','P','0','6')
#define ERRCODE_INVALID_INDICATOR_PARAMETER_VALUE MAKE_SQLSTATE('2','2','0','1','0')
#define ERRCODE_INVALID_PARAMETER_VALUE MAKE_SQLSTATE('2','2','0','2','3')
#define ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE MAKE_SQLSTATE('2','2','0','1','3')
#define ERRCODE_INVALID_REGULAR_EXPRESSION MAKE_SQLSTATE('2','2','0','1','B')
#define ERRCODE_INVALID_ROW_COUNT_IN_LIMIT_CLAUSE MAKE_SQLSTATE('2','2','0','1','W')
#define ERRCODE_INVALID_ROW_COUNT_IN_RESULT_OFFSET_CLAUSE MAKE_SQLSTATE('2','2','0','1','X')
#define ERRCODE_INVALID_TABLESAMPLE_ARGUMENT MAKE_SQLSTATE('2','2','0','2','H')
#define ERRCODE_INVALID_TABLESAMPLE_REPEAT MAKE_SQLSTATE('2','2','0','2','G')
#define ERRCODE_INVALID_TIME_ZONE_DISPLACEMENT_VALUE MAKE_SQLSTATE('2','2','0','0','9')
#define ERRCODE_INVALID_USE_OF_ESCAPE_CHARACTER MAKE_SQLSTATE('2','2','0','0','C')
#define ERRCODE_MOST_SPECIFIC_TYPE_MISMATCH MAKE_SQLSTATE('2','2','0','0','G')
#define ERRCODE_NULL_VALUE_NOT_ALLOWED MAKE_SQLSTATE('2','2','0','0','4')
#define ERRCODE_NULL_VALUE_NO_INDICATOR_PARAMETER MAKE_SQLSTATE('2','2','0','0','2')
#define ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE MAKE_SQLSTATE('2','2','0','0','3')
#define ERRCODE_SEQUENCE_GENERATOR_LIMIT_EXCEEDED MAKE_SQLSTATE('2','2','0','0','H')
#define ERRCODE_STRING_DATA_LENGTH_MISMATCH MAKE_SQLSTATE('2','2','0','2','6')
#define ERRCODE_STRING_DATA_RIGHT_TRUNCATION MAKE_SQLSTATE('2','2','0','0','1')
#define ERRCODE_SUBSTRING_ERROR MAKE_SQLSTATE('2','2','0','1','1')
#define ERRCODE_TRIM_ERROR MAKE_SQLSTATE('2','2','0','2','7')
#define ERRCODE_UNTERMINATED_C_STRING MAKE_SQLSTATE('2','2','0','2','4')
#define ERRCODE_ZERO_LENGTH_CHARACTER_STRING MAKE_SQLSTATE('2','2','0','0','F')
#define ERRCODE_FLOATING_POINT_EXCEPTION MAKE_SQLSTATE('2','2','P','0','1')
#define ERRCODE_INVALID_TEXT_REPRESENTATION MAKE_SQLSTATE('2','2','P','0','2')
#define ERRCODE_INVALID_BINARY_REPRESENTATION MAKE_SQLSTATE('2','2','P','0','3')
#define ERRCODE_BAD_COPY_FILE_FORMAT MAKE_SQLSTATE('2','2','P','0','4')
#define ERRCODE_UNTRANSLATABLE_CHARACTER MAKE_SQLSTATE('2','2','P','0','5')
#define ERRCODE_NOT_AN_XML_DOCUMENT MAKE_SQLSTATE('2','2','0','0','L')
#define ERRCODE_INVALID_XML_DOCUMENT MAKE_SQLSTATE('2','2','0','0','M')
#define ERRCODE_INVALID_XML_CONTENT MAKE_SQLSTATE('2','2','0','0','N')
#define ERRCODE_INVALID_XML_COMMENT MAKE_SQLSTATE('2','2','0','0','S')
#define ERRCODE_INVALID_XML_PROCESSING_INSTRUCTION MAKE_SQLSTATE('2','2','0','0','T')
#define ERRCODE_DUPLICATE_JSON_OBJECT_KEY_VALUE MAKE_SQLSTATE('2','2','0','3','0')
#define ERRCODE_INVALID_ARGUMENT_FOR_SQL_JSON_DATETIME_FUNCTION MAKE_SQLSTATE('2','2','0','3','1')
#define ERRCODE_INVALID_JSON_TEXT MAKE_SQLSTATE('2','2','0','3','2')
#define ERRCODE_INVALID_SQL_JSON_SUBSCRIPT MAKE_SQLSTATE('2','2','0','3','3')
#define ERRCODE_MORE_THAN_ONE_SQL_JSON_ITEM MAKE_SQLSTATE('2','2','0','3','4')
#define ERRCODE_NO_SQL_JSON_ITEM MAKE_SQLSTATE('2','2','0','3','5')
#define ERRCODE_NON_NUMERIC_SQL_JSON_ITEM MAKE_SQLSTATE('2','2','0','3','6')
#define ERRCODE_NON_UNIQUE_KEYS_IN_A_JSON_OBJECT MAKE_SQLSTATE('2','2','0','3','7')
#define ERRCODE_SINGLETON_SQL_JSON_ITEM_REQUIRED MAKE_SQLSTATE('2','2','0','3','8')
#define ERRCODE_SQL_JSON_ARRAY_NOT_FOUND MAKE_SQLSTATE('2','2','0','3','9')
#define ERRCODE_SQL_JSON_MEMBER_NOT_FOUND MAKE_SQLSTATE('2','2','0','3','A')
#define ERRCODE_SQL_JSON_NUMBER_NOT_FOUND MAKE_SQLSTATE('2','2','0','3','B')
#define ERRCODE_SQL_JSON_OBJECT_NOT_FOUND MAKE_SQLSTATE('2','2','0','3','C')
#define ERRCODE_TOO_MANY_JSON_ARRAY_ELEMENTS MAKE_SQLSTATE('2','2','0','3','D')
#define ERRCODE_TOO_MANY_JSON_OBJECT_MEMBERS MAKE_SQLSTATE('2','2','0','3','E')
#define ERRCODE_SQL_JSON_SCALAR_REQUIRED MAKE_SQLSTATE('2','2','0','3','F')

/* Class 23 - Integrity Constraint Violation */
#define ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION MAKE_SQLSTATE('2','3','0','0','0')
#define ERRCODE_RESTRICT_VIOLATION MAKE_SQLSTATE('2','3','0','0','1')
#define ERRCODE_NOT_NULL_VIOLATION MAKE_SQLSTATE('2','3','5','0','2')
#define ERRCODE_FOREIGN_KEY_VIOLATION MAKE_SQLSTATE('2','3','5','0','3')
#define ERRCODE_UNIQUE_VIOLATION MAKE_SQLSTATE('2','3','5','0','5')
#define ERRCODE_CHECK_VIOLATION MAKE_SQLSTATE('2','3','5','1','4')
#define ERRCODE_EXCLUSION_VIOLATION MAKE_SQLSTATE('2','3','P','0','1')

/* Class 24 - Invalid Cursor State */
#define ERRCODE_INVALID_CURSOR_STATE MAKE_SQLSTATE('2','4','0','0','0')

/* Class 25 - Invalid Transaction State */
#define ERRCODE_INVALID_TRANSACTION_STATE MAKE_SQLSTATE('2','5','0','0','0')
#define ERRCODE_ACTIVE_SQL_TRANSACTION MAKE_SQLSTATE('2','5','0','0','1')
#define ERRCODE_BRANCH_TRANSACTION_ALREADY_ACTIVE MAKE_SQLSTATE('2','5','0','0','2')
#define ERRCODE_HELD_CURSOR_REQUIRES_SAME_ISOLATION_LEVEL MAKE_SQLSTATE('2','5','0','0','8')
#define ERRCODE_INAPPROPRIATE_ACCESS_MODE_FOR_BRANCH_TRANSACTION MAKE_SQLSTATE('2','5','0','0','3')
#define ERRCODE_INAPPROPRIATE_ISOLATION_LEVEL_FOR_BRANCH_TRANSACTION MAKE_SQLSTATE('2','5','0','0','4')
#define ERRCODE_NO_ACTIVE_SQL_TRANSACTION_FOR_BRANCH_TRANSACTION MAKE_SQLSTATE('2','5','0','0','5')
#define ERRCODE_READ_ONLY_SQL_TRANSACTION MAKE_SQLSTATE('2','5','0','0','6')
#define ERRCODE_SCHEMA_AND_DATA_STATEMENT_MIXING_NOT_SUPPORTED MAKE_SQLSTATE('2','5','0','0','7')
#define ERRCODE_NO_ACTIVE_SQL_TRANSACTION MAKE_SQLSTATE('2','5','P','0','1')
#define ERRCODE_IN_FAILED_SQL_TRANSACTION MAKE_SQLSTATE('2','5','P','0','2')
#define ERRCODE_IDLE_IN_TRANSACTION_SESSION_TIMEOUT MAKE_SQLSTATE('2','5','P','0','3')

/* Class 26 - Invalid SQL Statement Name */
#define ERRCODE_INVALID_SQL_STATEMENT_NAME MAKE_SQLSTATE('2','6','0','0','0')

/* Class 27 - Triggered Data Change Violation */
#define ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION MAKE_SQLSTATE('2','7','0','0','0')

/* Class 28 - Invalid Authorization Specification */
#define ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION MAKE_SQLSTATE('2','8','0','0','0')
#define ERRCODE_INVALID_PASSWORD MAKE_SQLSTATE('2','8','P','0','1')

/* Class 2B - Dependent Privilege Descriptors Still Exist */
#define ERRCODE_DEPENDENT_PRIVILEGE_DESCRIPTORS_STILL_EXIST MAKE_SQLSTATE('2','B','0','0','0')
#define ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST MAKE_SQLSTATE('2','B','P','0','1')

/* Class 2D - Invalid Transaction Termination */
#define ERRCODE_INVALID_TRANSACTION_TERMINATION MAKE_SQLSTATE('2','D','0','0','0')

/* Class 2F - SQL Routine Exception */
#define ERRCODE_SQL_ROUTINE_EXCEPTION MAKE_SQLSTATE('2','F','0','0','0')
#define ERRCODE_S_R_E_FUNCTION_EXECUTED_NO_RETURN_STATEMENT MAKE_SQLSTATE('2','F','0','0','5')
#define ERRCODE_S_R_E_MODIFYING_SQL_DATA_NOT_PERMITTED MAKE_SQLSTATE('2','F','0','0','2')
#define ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED MAKE_SQLSTATE('2','F','0','0','3')
#define ERRCODE_S_R_E_READING_SQL_DATA_NOT_PERMITTED MAKE_SQLSTATE('2','F','0','0','4')

/* Class 34 - Invalid Cursor Name */
#define ERRCODE_INVALID_CURSOR_NAME MAKE_SQLSTATE('3','4','0','0','0')

/* Class 38 - External Routine Exception */
#define ERRCODE_EXTERNAL_ROUTINE_EXCEPTION MAKE_SQLSTATE('3','8','0','0','0')
#define ERRCODE_E_R_E_CONTAINING_SQL_NOT_PERMITTED MAKE_SQLSTATE('3','8','0','0','1')
#define ERRCODE_E_R_E_MODIFYING_SQL_DATA_NOT_PERMITTED MAKE_SQLSTATE('3','8','0','0','2')
#define ERRCODE_E_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED MAKE_SQLSTATE('3','8','0','0','3')
#define ERRCODE_E_R_E_READING_SQL_DATA_NOT_PERMITTED MAKE_SQLSTATE('3','8','0','0','4')

/* Class 39 - External Routine Invocation Exception */
#define ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION MAKE_SQLSTATE('3','9','0','0','0')
#define ERRCODE_E_R_I_E_INVALID_SQLSTATE_RETURNED MAKE_SQLSTATE('3','9','0','0','1')
#define ERRCODE_E_R_I_E_NULL_VALUE_NOT_ALLOWED MAKE_SQLSTATE('3','9','0','0','4')
#define ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED MAKE_SQLSTATE('3','9','P','0','1')
#define ERRCODE_E_R_I_E_SRF_PROTOCOL_VIOLATED MAKE_SQLSTATE('3','9','P','0','2')
#define ERRCODE_E_R_I_E_EVENT_TRIGGER_PROTOCOL_VIOLATED MAKE_SQLSTATE('3','9','P','0','3')

/* Class 3B - Savepoint Exception */
#define ERRCODE_SAVEPOINT_EXCEPTION MAKE_SQLSTATE('3','B','0','0','0')
#define ERRCODE_S_E_INVALID_SPECIFICATION MAKE_SQLSTATE('3','B','0','0','1')

/* Class 3D - Invalid Catalog Name */
#define ERRCODE_INVALID_CATALOG_NAME MAKE_SQLSTATE('3','D','0','0','0')

/* Class 3F - Invalid Schema Name */
#define ERRCODE_INVALID_SCHEMA_NAME MAKE_SQLSTATE('3','F','0','0','0')

/* Class 40 - Transaction Rollback */
#define ERRCODE_TRANSACTION_ROLLBACK MAKE_SQLSTATE('4','0','0','0','0')
#define ERRCODE_T_R_INTEGRITY_CONSTRAINT_VIOLATION MAKE_SQLSTATE('4','0','0','0','2')
#define ERRCODE_T_R_SERIALIZATION_FAILURE MAKE_SQLSTATE('4','0','0','0','1')
#define ERRCODE_T_R_STATEMENT_COMPLETION_UNKNOWN MAKE_SQLSTATE('4','0','0','0','3')
#define ERRCODE_T_R_DEADLOCK_DETECTED MAKE_SQLSTATE('4','0','P','0','1')

/* Class 42 - Syntax Error or Access Rule Violation */
#define ERRCODE_SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION MAKE_SQLSTATE('4','2','0','0','0')
#define ERRCODE_SYNTAX_ERROR MAKE_SQLSTATE('4','2','6','0','1')
#define ERRCODE_INSUFFICIENT_PRIVILEGE MAKE_SQLSTATE('4','2','5','0','1')
#define ERRCODE_CANNOT_COERCE MAKE_SQLSTATE('4','2','8','4','6')
#define ERRCODE_GROUPING_ERROR MAKE_SQLSTATE('4','2','8','0','3')
#define ERRCODE_WINDOWING_ERROR MAKE_SQLSTATE('4','2','P','2','0')
#define ERRCODE_INVALID_RECURSION MAKE_SQLSTATE('4','2','P','1','9')
#define ERRCODE_INVALID_FOREIGN_KEY MAKE_SQLSTATE('4','2','8','3','0')
#define ERRCODE_INVALID_NAME MAKE_SQLSTATE('4','2','6','0','2')
#define ERRCODE_NAME_TOO_LONG MAKE_SQLSTATE('4','2','6','2','2')
#define ERRCODE_RESERVED_NAME MAKE_SQLSTATE('4','2','9','3','9')
#define ERRCODE_DATATYPE_MISMATCH MAKE_SQLSTATE('4','2','8','0','4')
#define ERRCODE_INDETERMINATE_DATATYPE MAKE_SQLSTATE('4','2','P','1','8')
#define ERRCODE_COLLATION_MISMATCH MAKE_SQLSTATE('4','2','P','2','1')
#define ERRCODE_INDETERMINATE_COLLATION MAKE_SQLSTATE('4','2','P','2','2')
#define ERRCODE_WRONG_OBJECT_TYPE MAKE_SQLSTATE('4','2','8','0','9')
#define ERRCODE_GENERATED_ALWAYS MAKE_SQLSTATE('4','2','8','C','9')
#define ERRCODE_UNDEFINED_COLUMN MAKE_SQLSTATE('4','2','7','0','3')
#define ERRCODE_UNDEFINED_CURSOR MAKE_SQLSTATE('3','4','0','0','0')
#define ERRCODE_UNDEFINED_DATABASE MAKE_SQLSTATE('3','D','0','0','0')
#define ERRCODE_UNDEFINED_FUNCTION MAKE_SQLSTATE('4','2','8','8','3')
#define ERRCODE_UNDEFINED_PSTATEMENT MAKE_SQLSTATE('2','6','0','0','0')
#define ERRCODE_UNDEFINED_SCHEMA MAKE_SQLSTATE('3','F','0','0','0')
#define ERRCODE_UNDEFINED_TABLE MAKE_SQLSTATE('4','2','P','0','1')
#define ERRCODE_UNDEFINED_PARAMETER MAKE_SQLSTATE('4','2','P','0','2')
#define ERRCODE_UNDEFINED_OBJECT MAKE_SQLSTATE('4','2','7','0','4')
#define ERRCODE_DUPLICATE_COLUMN MAKE_SQLSTATE('4','2','7','0','1')
#define ERRCODE_DUPLICATE_CURSOR MAKE_SQLSTATE('4','2','P','0','3')
#define ERRCODE_DUPLICATE_DATABASE MAKE_SQLSTATE('4','2','P','0','4')
#define ERRCODE_DUPLICATE_FUNCTION MAKE_SQLSTATE('4','2','7','2','3')
#define ERRCODE_DUPLICATE_PSTATEMENT MAKE_SQLSTATE('4','2','P','0','5')
#define ERRCODE_DUPLICATE_SCHEMA MAKE_SQLSTATE('4','2','P','0','6')
#define ERRCODE_DUPLICATE_TABLE MAKE_SQLSTATE('4','2','P','0','7')
#define ERRCODE_DUPLICATE_ALIAS MAKE_SQLSTATE('4','2','7','1','2')
#define ERRCODE_DUPLICATE_OBJECT MAKE_SQLSTATE('4','2','7','1','0')
#define ERRCODE_AMBIGUOUS_COLUMN MAKE_SQLSTATE('4','2','7','0','2')
#define ERRCODE_AMBIGUOUS_FUNCTION MAKE_SQLSTATE('4','2','7','2','5')
#define ERRCODE_AMBIGUOUS_PARAMETER MAKE_SQLSTATE('4','2','P','0','8')
#define ERRCODE_AMBIGUOUS_ALIAS MAKE_SQLSTATE('4','2','P','0','9')
#define ERRCODE_INVALID_COLUMN_REFERENCE MAKE_SQLSTATE('4','2','P','1','0')
#define ERRCODE_INVALID_COLUMN_DEFINITION MAKE_SQLSTATE('4','2','6','1','1')
#define ERRCODE_INVALID_CURSOR_DEFINITION MAKE_SQLSTATE('4','2','P','1','1')
#define ERRCODE_INVALID_DATABASE_DEFINITION MAKE_SQLSTATE('4','2','P','1','2')
#define ERRCODE_INVALID_FUNCTION_DEFINITION MAKE_SQLSTATE('4','2','P','1','3')
#define ERRCODE_INVALID_PSTATEMENT_DEFINITION MAKE_SQLSTATE('4','2','P','1','4')
#define ERRCODE_INVALID_SCHEMA_DEFINITION MAKE_SQLSTATE('4','2','P','1','5')
#define ERRCODE_INVALID_TABLE_DEFINITION MAKE_SQLSTATE('4','2','P','1','6')
#define ERRCODE_INVALID_OBJECT_DEFINITION MAKE_SQLSTATE('4','2','P','1','7')

/* Class 44 - WITH CHECK OPTION Violation */
#define ERRCODE_WITH_CHECK_OPTION_VIOLATION MAKE_SQLSTATE('4','4','0','0','0')

/* Class 53 - Insufficient Resources */
#define ERRCODE_INSUFFICIENT_RESOURCES MAKE_SQLSTATE('5','3','0','0','0')
#define ERRCODE_DISK_FULL MAKE_SQLSTATE('5','3','1','0','0')
#define ERRCODE_OUT_OF_MEMORY MAKE_SQLSTATE('5','3','2','0','0')
#define ERRCODE_TOO_MANY_CONNECTIONS MAKE_SQLSTATE('5','3','3','0','0')
#define ERRCODE_CONFIGURATION_LIMIT_EXCEEDED MAKE_SQLSTATE('5','3','4','0','0')

/* Class 54 - Program Limit Exceeded */
#define ERRCODE_PROGRAM_LIMIT_EXCEEDED MAKE_SQLSTATE('5','4','0','0','0')
#define ERRCODE_STATEMENT_TOO_COMPLEX MAKE_SQLSTATE('5','4','0','0','1')
#define ERRCODE_TOO_MANY_COLUMNS MAKE_SQLSTATE('5','4','0','1','1')
#define ERRCODE_TOO_MANY_ARGUMENTS MAKE_SQLSTATE('5','4','0','2','3')

/* Class 55 - Object Not In Prerequisite State */
#define ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE MAKE_SQLSTATE('5','5','0','0','0')
#define ERRCODE_OBJECT_IN_USE MAKE_SQLSTATE('5','5','0','0','6')
#define ERRCODE_CANT_CHANGE_RUNTIME_PARAM MAKE_SQLSTATE('5','5','P','0','2')
#define ERRCODE_LOCK_NOT_AVAILABLE MAKE_SQLSTATE('5','5','P','0','3')
#define ERRCODE_UNSAFE_NEW_ENUM_VALUE_USAGE MAKE_SQLSTATE('5','5','P','0','4')

/* Class 57 - Operator Intervention */
#define ERRCODE_OPERATOR_INTERVENTION MAKE_SQLSTATE('5','7','0','0','0')
#define ERRCODE_QUERY_CANCELED MAKE_SQLSTATE('5','7','0','1','4')
#define ERRCODE_ADMIN_SHUTDOWN MAKE_SQLSTATE('5','7','P','0','1')
#define ERRCODE_CRASH_SHUTDOWN MAKE_SQLSTATE('5','7','P','0','2')
#define ERRCODE_CANNOT_CONNECT_NOW MAKE_SQLSTATE('5','7','P','0','3')
#define ERRCODE_DATABASE_DROPPED MAKE_SQLSTATE('5','7','P','0','4')
#define ERRCODE_IDLE_SESSION_TIMEOUT MAKE_SQLSTATE('5','7','P','0','5')

/* Class 58 - System Error (errors external to PostgreSQL itself) */
#define ERRCODE_SYSTEM_ERROR MAKE_SQLSTATE('5','8','0','0','0')
#define ERRCODE_IO_ERROR MAKE_SQLSTATE('5','8','0','3','0')
#define ERRCODE_UNDEFINED_FILE MAKE_SQLSTATE('5','8','P','0','1')
#define ERRCODE_DUPLICATE_FILE MAKE_SQLSTATE('5','8','P','0','2')

/* Class 72 - Snapshot Failure */
#define ERRCODE_SNAPSHOT_TOO_OLD MAKE_SQLSTATE('7','2','0','0','0')

/* Class F0 - Configuration File Error */
#define ERRCODE_CONFIG_FILE_ERROR MAKE_SQLSTATE('F','0','0','0','0')
#define ERRCODE_LOCK_FILE_EXISTS MAKE_SQLSTATE('F','0','0','0','1')

/* Class HV - Foreign Data Wrapper Error (SQL/MED) */
#define ERRCODE_FDW_ERROR MAKE_SQLSTATE('H','V','0','0','0')
#define ERRCODE_FDW_COLUMN_NAME_NOT_FOUND MAKE_SQLSTATE('H','V','0','0','5')
#define ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED MAKE_SQLSTATE('H','V','0','0','2')
#define ERRCODE_FDW_FUNCTION_SEQUENCE_ERROR MAKE_SQLSTATE('H','V','0','1','0')
#define ERRCODE_FDW_INCONSISTENT_DESCRIPTOR_INFORMATION MAKE_SQLSTATE('H','V','0','2','1')
#define ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE MAKE_SQLSTATE('H','V','0','2','4')
#define ERRCODE_FDW_INVALID_COLUMN_NAME MAKE_SQLSTATE('H','V','0','0','7')
#define ERRCODE_FDW_INVALID_COLUMN_NUMBER MAKE_SQLSTATE('H','V','0','0','8')
#define ERRCODE_FDW_INVALID_DATA_TYPE MAKE_SQLSTATE('H','V','0','0','4')
#define ERRCODE_FDW_INVALID_DATA_TYPE_DESCRIPTORS MAKE_SQLSTATE('H','V','0','0','6')
#define ERRCODE_FDW_INVALID_DESCRIPTOR_FIELD_IDENTIFIER MAKE_SQLSTATE('H','V','0','9','1')
#define ERRCODE_FDW_INVALID_HANDLE MAKE_SQLSTATE('H','V','0','0','B')
#define ERRCODE_FDW_INVALID_OPTION_INDEX MAKE_SQLSTATE('H','V','0','0','C')
#define ERRCODE_FDW_INVALID_OPTION_NAME MAKE_SQLSTATE('H','V','0','0','D')
#define ERRCODE_FDW_INVALID_STRING_LENGTH_OR_BUFFER_LENGTH MAKE_SQLSTATE('H','V','0','9','0')
#define ERRCODE_FDW_INVALID_STRING_FORMAT MAKE_SQLSTATE('H','V','0','0','A')
#define ERRCODE_FDW_INVALID_USE_OF_NULL_POINTER MAKE_SQLSTATE('H','V','0','0','9')
#define ERRCODE_FDW_TOO_MANY_HANDLES MAKE_SQLSTATE('H','V','0','1','4')
#define ERRCODE_FDW_OUT_OF_MEMORY MAKE_SQLSTATE('H','V','0','0','1')
#define ERRCODE_FDW_NO_SCHEMAS MAKE_SQLSTATE('H','V','0','0','P')
#define ERRCODE_FDW_OPTION_NAME_NOT_FOUND MAKE_SQLSTATE('H','V','0','0','J')
#define ERRCODE_FDW_REPLY_HANDLE MAKE_SQLSTATE('H','V','0','0','K')
#define ERRCODE_FDW_SCHEMA_NOT_FOUND MAKE_SQLSTATE('H','V','0','0','Q')
#define ERRCODE_FDW_TABLE_NOT_FOUND MAKE_SQLSTATE('H','V','0','0','R')
#define ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION MAKE_SQLSTATE('H','V','0','0','L')
#define ERRCODE_FDW_UNABLE_TO_CREATE_REPLY MAKE_SQLSTATE('H','V','0','0','M')
#define ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION MAKE_SQLSTATE('H','V','0','0','N')

/* Class P0 - PL/pgSQL Error */
#define ERRCODE_PLPGSQL_ERROR MAKE_SQLSTATE('P','0','0','0','0')
#define ERRCODE_RAISE_EXCEPTION MAKE_SQLSTATE('P','0','0','0','1')
#define ERRCODE_NO_DATA_FOUND MAKE_SQLSTATE('P','0','0','0','2')
#define ERRCODE_TOO_MANY_ROWS MAKE_SQLSTATE('P','0','0','0','3')
#define ERRCODE_ASSERT_FAILURE MAKE_SQLSTATE('P','0','0','0','4')

/* Class XX - Internal Error */
#define ERRCODE_INTERNAL_ERROR MAKE_SQLSTATE('X','X','0','0','0')
#define ERRCODE_DATA_CORRUPTED MAKE_SQLSTATE('X','X','0','0','1')
#define ERRCODE_INDEX_CORRUPTED MAKE_SQLSTATE('X','X','0','0','2')

/*-------------------------------------------------------------------------
 *
 * pqformat.c
 *		Routines for formatting and parsing frontend/backend messages
 *
 * Outgoing messages are built up in a StringInfo buffer (which is expansible)
 * and then sent in a single call to pq_putmessage.  This module provides data
 * formatting/conversion routines that are needed to produce valid messages.
 * Note in particular the distinction between "raw data" and "text"; raw data
 * is message protocol characters and binary values that are not subject to
 * character set conversion, while text is converted by character encoding
 * rules.
 *
 * Incoming messages are similarly read into a StringInfo buffer, via
 * pq_getmessage, and then parsed and converted from that using the routines
 * in this module.
 *
 * These same routines support reading and writing of external binary formats
 * (typsend/typreceive routines).  The conversion routines for individual
 * data types are exactly the same, only initialization and completion
 * are different.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *	src/backend/libpq/pqformat.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 * Message assembly and output:
 *		pq_beginmessage - initialize StringInfo buffer
 *		pq_sendbyte		- append a raw byte to a StringInfo buffer
 *		pq_sendint		- append a binary integer to a StringInfo buffer
 *		pq_sendint64	- append a binary 8-byte int to a StringInfo buffer
 *		pq_sendfloat4	- append a float4 to a StringInfo buffer
 *		pq_sendfloat8	- append a float8 to a StringInfo buffer
 *		pq_sendbytes	- append raw data to a StringInfo buffer
 *		pq_sendcountedtext - append a counted text string (with character set conversion)
 *		pq_sendtext		- append a text string (with conversion)
 *		pq_sendstring	- append a null-terminated text string (with conversion)
 *		pq_send_ascii_string - append a null-terminated text string (without conversion)
 *		pq_endmessage	- send the completed message to the frontend
 * Note: it is also possible to append data to the StringInfo buffer using
 * the regular StringInfo routines, but this is discouraged since required
 * character set conversion may not occur.
 *
 * typsend support (construct a bytea value containing external binary data):
 *		pq_begintypsend - initialize StringInfo buffer
 *		pq_endtypsend	- return the completed string as a "bytea*"
 *
 * Special-case message output:
 *		pq_puttextmessage - generate a character set-converted message in one step
 *		pq_putemptymessage - convenience routine for message with empty body
 *
 * Message parsing after input:
 *		pq_getmsgbyte	- get a raw byte from a message buffer
 *		pq_getmsgint	- get a binary integer from a message buffer
 *		pq_getmsgint64	- get a binary 8-byte int from a message buffer
 *		pq_getmsgfloat4 - get a float4 from a message buffer
 *		pq_getmsgfloat8 - get a float8 from a message buffer
 *		pq_getmsgbytes	- get raw data from a message buffer
 *		pq_copymsgbytes - copy raw data from a message buffer
 *		pq_getmsgtext	- get a counted text string (with conversion)
 *		pq_getmsgstring - get a null-terminated text string (with conversion)
 *		pq_getmsgrawstring - get a null-terminated text string - NO conversion
 *		pq_getmsgend	- verify message fully consumed
 */

// #include "postgres.h"

#include <sys/param.h>

// #include "libpq/libpq.h"
// #include "libpq/pqformat.h"
// #include "mb/pg_wchar.h"
// #include "port/pg_bswap.h"


/* --------------------------------
 *		pq_beginmessage		- initialize for sending a message
 * --------------------------------
 */
void
pq_beginmessage(StringInfo buf, char msgtype)
{
	initStringInfo(buf);

	/*
	 * We stash the message type into the buffer's cursor field, expecting
	 * that the pq_sendXXX routines won't touch it.  We could alternatively
	 * make it the first byte of the buffer contents, but this seems easier.
	 */
	buf->cursor = msgtype;
}

/* --------------------------------

 *		pq_beginmessage_reuse - initialize for sending a message, reuse buffer
 *
 * This requires the buffer to be allocated in a sufficiently long-lived
 * memory context.
 * --------------------------------
 */
void
pq_beginmessage_reuse(StringInfo buf, char msgtype)
{
	resetStringInfo(buf);

	/*
	 * We stash the message type into the buffer's cursor field, expecting
	 * that the pq_sendXXX routines won't touch it.  We could alternatively
	 * make it the first byte of the buffer contents, but this seems easier.
	 */
	buf->cursor = msgtype;
}

/* --------------------------------
 *		pq_sendbytes	- append raw data to a StringInfo buffer
 * --------------------------------
 */
void
pq_sendbytes(StringInfo buf, const char *data, int datalen)
{
	/* use variant that maintains a trailing null-byte, out of caution */
	appendBinaryStringInfo(buf, data, datalen);
}

/* --------------------------------
 *		pq_send_ascii_string	- append a null-terminated text string (without conversion)
 *
 * This function intentionally bypasses encoding conversion, instead just
 * silently replacing any non-7-bit-ASCII characters with question marks.
 * It is used only when we are having trouble sending an error message to
 * the client with normal localization and encoding conversion.  The caller
 * should already have taken measures to ensure the string is just ASCII;
 * the extra work here is just to make certain we don't send a badly encoded
 * string to the client (which might or might not be robust about that).
 *
 * NB: passed text string must be null-terminated, and so is the data
 * sent to the frontend.
 * --------------------------------
 */
void
pq_send_ascii_string(StringInfo buf, const char *str)
{
	while (*str)
	{
		char		ch = *str++;

		if (IS_HIGHBIT_SET(ch))
			ch = '?';
		appendStringInfoCharMacro(buf, ch);
	}
	appendStringInfoChar(buf, '\0');
}

/* --------------------------------
 *		pq_sendfloat4	- append a float4 to a StringInfo buffer
 *
 * The point of this routine is to localize knowledge of the external binary
 * representation of float4, which is a component of several datatypes.
 *
 * We currently assume that float4 should be byte-swapped in the same way
 * as int4.  This rule is not perfect but it gives us portability across
 * most IEEE-float-using architectures.
 * --------------------------------
 */
void
pq_sendfloat4(StringInfo buf, float4 f)
{
	union
	{
		float4		f;
		uint32		i;
	}			swap;

	swap.f = f;
	pq_sendint32(buf, swap.i);
}

/* --------------------------------
 *		pq_sendfloat8	- append a float8 to a StringInfo buffer
 *
 * The point of this routine is to localize knowledge of the external binary
 * representation of float8, which is a component of several datatypes.
 *
 * We currently assume that float8 should be byte-swapped in the same way
 * as int8.  This rule is not perfect but it gives us portability across
 * most IEEE-float-using architectures.
 * --------------------------------
 */
void
pq_sendfloat8(StringInfo buf, float8 f)
{
	union
	{
		float8		f;
		int64		i;
	}			swap;

	swap.f = f;
	pq_sendint64(buf, swap.i);
}


/* --------------------------------
 *		pq_begintypsend		- initialize for constructing a bytea result
 * --------------------------------
 */
void
pq_begintypsend(StringInfo buf)
{
	initStringInfo(buf);
	/* Reserve four bytes for the bytea length word */
	appendStringInfoCharMacro(buf, '\0');
	appendStringInfoCharMacro(buf, '\0');
	appendStringInfoCharMacro(buf, '\0');
	appendStringInfoCharMacro(buf, '\0');
}

/* --------------------------------
 *		pq_endtypsend	- finish constructing a bytea result
 *
 * The data buffer is returned as the palloc'd bytea value.  (We expect
 * that it will be suitably aligned for this because it has been palloc'd.)
 * We assume the StringInfoData is just a local variable in the caller and
 * need not be pfree'd.
 * --------------------------------
 */
bytea *
pq_endtypsend(StringInfo buf)
{
	bytea	   *result = (bytea *) buf->data;

	/* Insert correct length into bytea length word */
	Assert(buf->len >= VARHDRSZ);
	SET_VARSIZE(result, buf->len);

	return result;
}


/* --------------------------------
 *		pq_getmsgbyte	- get a raw byte from a message buffer
 * --------------------------------
 */
int
pq_getmsgbyte(StringInfo msg)
{
	if (msg->cursor >= msg->len)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("no data left in message")));
	return (unsigned char) msg->data[msg->cursor++];
}

/* --------------------------------
 *		pq_getmsgint	- get a binary integer from a message buffer
 *
 *		Values are treated as unsigned.
 * --------------------------------
 */
unsigned int
pq_getmsgint(StringInfo msg, int b)
{
	unsigned int result;
	unsigned char n8;
	uint16		n16;
	uint32		n32;

	switch (b)
	{
		case 1:
			pq_copymsgbytes(msg, (char *) &n8, 1);
			result = n8;
			break;
		case 2:
			pq_copymsgbytes(msg, (char *) &n16, 2);
			result = pg_ntoh16(n16);
			break;
		case 4:
			pq_copymsgbytes(msg, (char *) &n32, 4);
			result = pg_ntoh32(n32);
			break;
		default:
			elog(ERROR, "unsupported integer size %d", b);
			result = 0;			/* keep compiler quiet */
			break;
	}
	return result;
}

/* --------------------------------
 *		pq_getmsgint64	- get a binary 8-byte int from a message buffer
 *
 * It is tempting to merge this with pq_getmsgint, but we'd have to make the
 * result int64 for all data widths --- that could be a big performance
 * hit on machines where int64 isn't efficient.
 * --------------------------------
 */
int64
pq_getmsgint64(StringInfo msg)
{
	uint64		n64;

	pq_copymsgbytes(msg, (char *) &n64, sizeof(n64));

	return pg_ntoh64(n64);
}

/* --------------------------------
 *		pq_getmsgfloat4 - get a float4 from a message buffer
 *
 * See notes for pq_sendfloat4.
 * --------------------------------
 */
float4
pq_getmsgfloat4(StringInfo msg)
{
	union
	{
		float4		f;
		uint32		i;
	}			swap;

	swap.i = pq_getmsgint(msg, 4);
	return swap.f;
}

/* --------------------------------
 *		pq_getmsgfloat8 - get a float8 from a message buffer
 *
 * See notes for pq_sendfloat8.
 * --------------------------------
 */
float8
pq_getmsgfloat8(StringInfo msg)
{
	union
	{
		float8		f;
		int64		i;
	}			swap;

	swap.i = pq_getmsgint64(msg);
	return swap.f;
}

/* --------------------------------
 *		pq_getmsgbytes	- get raw data from a message buffer
 *
 *		Returns a pointer directly into the message buffer; note this
 *		may not have any particular alignment.
 * --------------------------------
 */
const char *
pq_getmsgbytes(StringInfo msg, int datalen)
{
	const char *result;

	if (datalen < 0 || datalen > (msg->len - msg->cursor))
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("insufficient data left in message")));
	result = &msg->data[msg->cursor];
	msg->cursor += datalen;
	return result;
}

/* --------------------------------
 *		pq_copymsgbytes - copy raw data from a message buffer
 *
 *		Same as above, except data is copied to caller's buffer.
 * --------------------------------
 */
void
pq_copymsgbytes(StringInfo msg, char *buf, int datalen)
{
	if (datalen < 0 || datalen > (msg->len - msg->cursor))
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("insufficient data left in message")));
	memcpy(buf, &msg->data[msg->cursor], datalen);
	msg->cursor += datalen;
}

/* --------------------------------
 *		pq_getmsgrawstring - get a null-terminated text string - NO conversion
 *
 *		Returns a pointer directly into the message buffer.
 * --------------------------------
 */
const char *
pq_getmsgrawstring(StringInfo msg)
{
	char	   *str;
	int			slen;

	str = &msg->data[msg->cursor];

	/*
	 * It's safe to use strlen() here because a StringInfo is guaranteed to
	 * have a trailing null byte.  But check we found a null inside the
	 * message.
	 */
	slen = strlen(str);
	if (msg->cursor + slen >= msg->len)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid string in message")));
	msg->cursor += slen + 1;

	return str;
}

/* --------------------------------
 *		pq_getmsgend	- verify message fully consumed
 * --------------------------------
 */
void
pq_getmsgend(StringInfo msg)
{
	if (msg->cursor != msg->len)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid message format")));
}



/*-------------------------------------------------------------------------
 * wait_event.h
 *	  Definitions related to wait event reporting
 *
 * Copyright (c) 2001-2022, PostgreSQL Global Development Group
 *
 * src/include/utils/wait_event.h
 * ----------
 */
#ifndef WAIT_EVENT_H
#define WAIT_EVENT_H


/* ----------
 * Wait Classes
 * ----------
 */
#define PG_WAIT_LWLOCK				0x01000000U
#define PG_WAIT_LOCK				0x03000000U
#define PG_WAIT_BUFFER_PIN			0x04000000U
#define PG_WAIT_ACTIVITY			0x05000000U
#define PG_WAIT_CLIENT				0x06000000U
#define PG_WAIT_EXTENSION			0x07000000U
#define PG_WAIT_IPC					0x08000000U
#define PG_WAIT_TIMEOUT				0x09000000U
#define PG_WAIT_IO					0x0A000000U

/* ----------
 * Wait Events - Activity
 *
 * Use this category when a process is waiting because it has no work to do,
 * unless the "Client" or "Timeout" category describes the situation better.
 * Typically, this should only be used for background processes.
 * ----------
 */
typedef enum
{
	WAIT_EVENT_ARCHIVER_MAIN = PG_WAIT_ACTIVITY,
	WAIT_EVENT_AUTOVACUUM_MAIN,
	WAIT_EVENT_BGWRITER_HIBERNATE,
	WAIT_EVENT_BGWRITER_MAIN,
	WAIT_EVENT_CHECKPOINTER_MAIN,
	WAIT_EVENT_LOGICAL_APPLY_MAIN,
	WAIT_EVENT_LOGICAL_LAUNCHER_MAIN,
	WAIT_EVENT_RECOVERY_WAL_STREAM,
	WAIT_EVENT_SYSLOGGER_MAIN,
	WAIT_EVENT_WAL_RECEIVER_MAIN,
	WAIT_EVENT_WAL_SENDER_MAIN,
	WAIT_EVENT_WAL_WRITER_MAIN
} WaitEventActivity;

/* ----------
 * Wait Events - Client
 *
 * Use this category when a process is waiting to send data to or receive data
 * from the frontend process to which it is connected.  This is never used for
 * a background process, which has no client connection.
 * ----------
 */
typedef enum
{
	WAIT_EVENT_CLIENT_READ = PG_WAIT_CLIENT,
	WAIT_EVENT_CLIENT_WRITE,
	WAIT_EVENT_GSS_OPEN_SERVER,
	WAIT_EVENT_LIBPQWALRECEIVER_CONNECT,
	WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE,
	WAIT_EVENT_SSL_OPEN_SERVER,
	WAIT_EVENT_WAL_SENDER_WAIT_WAL,
	WAIT_EVENT_WAL_SENDER_WRITE_DATA,
} WaitEventClient;

/* ----------
 * Wait Events - IPC
 *
 * Use this category when a process cannot complete the work it is doing because
 * it is waiting for a notification from another process.
 * ----------
 */
typedef enum
{
	WAIT_EVENT_APPEND_READY = PG_WAIT_IPC,
	WAIT_EVENT_ARCHIVE_CLEANUP_COMMAND,
	WAIT_EVENT_ARCHIVE_COMMAND,
	WAIT_EVENT_BACKEND_TERMINATION,
	WAIT_EVENT_BACKUP_WAIT_WAL_ARCHIVE,
	WAIT_EVENT_BGWORKER_SHUTDOWN,
	WAIT_EVENT_BGWORKER_STARTUP,
	WAIT_EVENT_BTREE_PAGE,
	WAIT_EVENT_BUFFER_IO,
	WAIT_EVENT_CHECKPOINT_DONE,
	WAIT_EVENT_CHECKPOINT_START,
	WAIT_EVENT_EXECUTE_GATHER,
	WAIT_EVENT_HASH_BATCH_ALLOCATE,
	WAIT_EVENT_HASH_BATCH_ELECT,
	WAIT_EVENT_HASH_BATCH_LOAD,
	WAIT_EVENT_HASH_BUILD_ALLOCATE,
	WAIT_EVENT_HASH_BUILD_ELECT,
	WAIT_EVENT_HASH_BUILD_HASH_INNER,
	WAIT_EVENT_HASH_BUILD_HASH_OUTER,
	WAIT_EVENT_HASH_GROW_BATCHES_ALLOCATE,
	WAIT_EVENT_HASH_GROW_BATCHES_DECIDE,
	WAIT_EVENT_HASH_GROW_BATCHES_ELECT,
	WAIT_EVENT_HASH_GROW_BATCHES_FINISH,
	WAIT_EVENT_HASH_GROW_BATCHES_REPARTITION,
	WAIT_EVENT_HASH_GROW_BUCKETS_ALLOCATE,
	WAIT_EVENT_HASH_GROW_BUCKETS_ELECT,
	WAIT_EVENT_HASH_GROW_BUCKETS_REINSERT,
	WAIT_EVENT_LOGICAL_SYNC_DATA,
	WAIT_EVENT_LOGICAL_SYNC_STATE_CHANGE,
	WAIT_EVENT_MQ_INTERNAL,
	WAIT_EVENT_MQ_PUT_MESSAGE,
	WAIT_EVENT_MQ_RECEIVE,
	WAIT_EVENT_MQ_SEND,
	WAIT_EVENT_PARALLEL_BITMAP_SCAN,
	WAIT_EVENT_PARALLEL_CREATE_INDEX_SCAN,
	WAIT_EVENT_PARALLEL_FINISH,
	WAIT_EVENT_PROCARRAY_GROUP_UPDATE,
	WAIT_EVENT_PROC_SIGNAL_BARRIER,
	WAIT_EVENT_PROMOTE,
	WAIT_EVENT_RECOVERY_CONFLICT_SNAPSHOT,
	WAIT_EVENT_RECOVERY_CONFLICT_TABLESPACE,
	WAIT_EVENT_RECOVERY_END_COMMAND,
	WAIT_EVENT_RECOVERY_PAUSE,
	WAIT_EVENT_REPLICATION_ORIGIN_DROP,
	WAIT_EVENT_REPLICATION_SLOT_DROP,
	WAIT_EVENT_RESTORE_COMMAND,
	WAIT_EVENT_SAFE_SNAPSHOT,
	WAIT_EVENT_SYNC_REP,
	WAIT_EVENT_WAL_RECEIVER_EXIT,
	WAIT_EVENT_WAL_RECEIVER_WAIT_START,
	WAIT_EVENT_XACT_GROUP_UPDATE
} WaitEventIPC;

/* ----------
 * Wait Events - Timeout
 *
 * Use this category when a process is waiting for a timeout to expire.
 * ----------
 */
typedef enum
{
	WAIT_EVENT_BASE_BACKUP_THROTTLE = PG_WAIT_TIMEOUT,
	WAIT_EVENT_CHECKPOINT_WRITE_DELAY,
	WAIT_EVENT_PG_SLEEP,
	WAIT_EVENT_RECOVERY_APPLY_DELAY,
	WAIT_EVENT_RECOVERY_RETRIEVE_RETRY_INTERVAL,
	WAIT_EVENT_REGISTER_SYNC_REQUEST,
	WAIT_EVENT_VACUUM_DELAY,
	WAIT_EVENT_VACUUM_TRUNCATE,
	WAIT_EVENT_BACK_PRESSURE
} WaitEventTimeout;

/* ----------
 * Wait Events - IO
 *
 * Use this category when a process is waiting for a IO.
 * ----------
 */
typedef enum
{
	WAIT_EVENT_BASEBACKUP_READ = PG_WAIT_IO,
	WAIT_EVENT_BASEBACKUP_SYNC,
	WAIT_EVENT_BASEBACKUP_WRITE,
	WAIT_EVENT_BUFFILE_READ,
	WAIT_EVENT_BUFFILE_WRITE,
	WAIT_EVENT_BUFFILE_TRUNCATE,
	WAIT_EVENT_CONTROL_FILE_READ,
	WAIT_EVENT_CONTROL_FILE_SYNC,
	WAIT_EVENT_CONTROL_FILE_SYNC_UPDATE,
	WAIT_EVENT_CONTROL_FILE_WRITE,
	WAIT_EVENT_CONTROL_FILE_WRITE_UPDATE,
	WAIT_EVENT_COPY_FILE_READ,
	WAIT_EVENT_COPY_FILE_WRITE,
	WAIT_EVENT_DATA_FILE_EXTEND,
	WAIT_EVENT_DATA_FILE_FLUSH,
	WAIT_EVENT_DATA_FILE_IMMEDIATE_SYNC,
	WAIT_EVENT_DATA_FILE_PREFETCH,
	WAIT_EVENT_DATA_FILE_READ,
	WAIT_EVENT_DATA_FILE_SYNC,
	WAIT_EVENT_DATA_FILE_TRUNCATE,
	WAIT_EVENT_DATA_FILE_WRITE,
	WAIT_EVENT_DSM_FILL_ZERO_WRITE,
	WAIT_EVENT_LOCK_FILE_ADDTODATADIR_READ,
	WAIT_EVENT_LOCK_FILE_ADDTODATADIR_SYNC,
	WAIT_EVENT_LOCK_FILE_ADDTODATADIR_WRITE,
	WAIT_EVENT_LOCK_FILE_CREATE_READ,
	WAIT_EVENT_LOCK_FILE_CREATE_SYNC,
	WAIT_EVENT_LOCK_FILE_CREATE_WRITE,
	WAIT_EVENT_LOCK_FILE_RECHECKDATADIR_READ,
	WAIT_EVENT_LOGICAL_REWRITE_CHECKPOINT_SYNC,
	WAIT_EVENT_LOGICAL_REWRITE_MAPPING_SYNC,
	WAIT_EVENT_LOGICAL_REWRITE_MAPPING_WRITE,
	WAIT_EVENT_LOGICAL_REWRITE_SYNC,
	WAIT_EVENT_LOGICAL_REWRITE_TRUNCATE,
	WAIT_EVENT_LOGICAL_REWRITE_WRITE,
	WAIT_EVENT_RELATION_MAP_READ,
	WAIT_EVENT_RELATION_MAP_SYNC,
	WAIT_EVENT_RELATION_MAP_WRITE,
	WAIT_EVENT_REORDER_BUFFER_READ,
	WAIT_EVENT_REORDER_BUFFER_WRITE,
	WAIT_EVENT_REORDER_LOGICAL_MAPPING_READ,
	WAIT_EVENT_REPLICATION_SLOT_READ,
	WAIT_EVENT_REPLICATION_SLOT_RESTORE_SYNC,
	WAIT_EVENT_REPLICATION_SLOT_SYNC,
	WAIT_EVENT_REPLICATION_SLOT_WRITE,
	WAIT_EVENT_SLRU_FLUSH_SYNC,
	WAIT_EVENT_SLRU_READ,
	WAIT_EVENT_SLRU_SYNC,
	WAIT_EVENT_SLRU_WRITE,
	WAIT_EVENT_SNAPBUILD_READ,
	WAIT_EVENT_SNAPBUILD_SYNC,
	WAIT_EVENT_SNAPBUILD_WRITE,
	WAIT_EVENT_TIMELINE_HISTORY_FILE_SYNC,
	WAIT_EVENT_TIMELINE_HISTORY_FILE_WRITE,
	WAIT_EVENT_TIMELINE_HISTORY_READ,
	WAIT_EVENT_TIMELINE_HISTORY_SYNC,
	WAIT_EVENT_TIMELINE_HISTORY_WRITE,
	WAIT_EVENT_TWOPHASE_FILE_READ,
	WAIT_EVENT_TWOPHASE_FILE_SYNC,
	WAIT_EVENT_TWOPHASE_FILE_WRITE,
	WAIT_EVENT_VERSION_FILE_WRITE,
	WAIT_EVENT_WALSENDER_TIMELINE_HISTORY_READ,
	WAIT_EVENT_WAL_BOOTSTRAP_SYNC,
	WAIT_EVENT_WAL_BOOTSTRAP_WRITE,
	WAIT_EVENT_WAL_COPY_READ,
	WAIT_EVENT_WAL_COPY_SYNC,
	WAIT_EVENT_WAL_COPY_WRITE,
	WAIT_EVENT_WAL_INIT_SYNC,
	WAIT_EVENT_WAL_INIT_WRITE,
	WAIT_EVENT_WAL_READ,
	WAIT_EVENT_WAL_SYNC,
	WAIT_EVENT_WAL_SYNC_METHOD_ASSIGN,
	WAIT_EVENT_WAL_WRITE
} WaitEventIO;


extern const char *pgstat_get_wait_event(uint32 wait_event_info);
extern const char *pgstat_get_wait_event_type(uint32 wait_event_info);
static inline void pgstat_report_wait_start(uint32 wait_event_info);
static inline void pgstat_report_wait_end(void);
extern void pgstat_set_wait_event_storage(uint32 *wait_event_info);
extern void pgstat_reset_wait_event_storage(void);

extern PGDLLIMPORT uint32 *my_wait_event_info;


/* ----------
 * pgstat_report_wait_start() -
 *
 *	Called from places where server process needs to wait.  This is called
 *	to report wait event information.  The wait information is stored
 *	as 4-bytes where first byte represents the wait event class (type of
 *	wait, for different types of wait, refer WaitClass) and the next
 *	3-bytes represent the actual wait event.  Currently 2-bytes are used
 *	for wait event which is sufficient for current usage, 1-byte is
 *	reserved for future usage.
 *
 *	Historically we used to make this reporting conditional on
 *	pgstat_track_activities, but the check for that seems to add more cost
 *	than it saves.
 *
 *	my_wait_event_info initially points to local memory, making it safe to
 *	call this before MyProc has been initialized.
 * ----------
 */
static inline void
pgstat_report_wait_start(uint32 wait_event_info)
{
	/*
	 * Since this is a four-byte field which is always read and written as
	 * four-bytes, updates are atomic.
	 */
	*(volatile uint32 *) my_wait_event_info = wait_event_info;
}

/* ----------
 * pgstat_report_wait_end() -
 *
 *	Called to report end of a wait.
 * ----------
 */
static inline void
pgstat_report_wait_end(void)
{
	/* see pgstat_report_wait_start() */
	*(volatile uint32 *) my_wait_event_info = 0;
}


#endif							/* WAIT_EVENT_H */

#include <signal.h>

/*
 * Latch structure should be treated as opaque and only accessed through
 * the public functions. It is defined here to allow embedding Latches as
 * part of bigger structs.
 */
typedef struct Latch
{
	sig_atomic_t is_set;
	sig_atomic_t maybe_sleeping;
	bool		is_shared;
	int			owner_pid;
#ifdef WIN32
	HANDLE		event;
#endif
} Latch;

/*
 * Bitmasks for events that may wake-up WaitLatch(), WaitLatchOrSocket(), or
 * WaitEventSetWait().
 */
#define WL_LATCH_SET		 (1 << 0)
#define WL_SOCKET_READABLE	 (1 << 1)
#define WL_SOCKET_WRITEABLE  (1 << 2)
#define WL_TIMEOUT			 (1 << 3)	/* not for WaitEventSetWait() */
#define WL_POSTMASTER_DEATH  (1 << 4)
#define WL_EXIT_ON_PM_DEATH	 (1 << 5)
#ifdef WIN32
#define WL_SOCKET_CONNECTED  (1 << 6)
#else
/* avoid having to deal with case on platforms not requiring it */
#define WL_SOCKET_CONNECTED  WL_SOCKET_WRITEABLE
#endif
#define WL_SOCKET_CLOSED 	 (1 << 7)
#define WL_SOCKET_MASK		(WL_SOCKET_READABLE | \
							 WL_SOCKET_WRITEABLE | \
							 WL_SOCKET_CONNECTED | \
							 WL_SOCKET_CLOSED)

/*
 * These macros encapsulate knowledge about the exact layout of XLog file
 * names, timeline history file names, and archive-status file names.
 */
#define MAXFNAMELEN		64

/*
 * TimestampDifferenceExceeds -- report whether the difference between two
 *		timestamps is >= a threshold (expressed in milliseconds)
 *
 * Both inputs must be ordinary finite timestamps (in current usage,
 * they'll be results from GetCurrentTimestamp()).
 */
bool
TimestampDifferenceExceeds(TimestampTz start_time,
						   TimestampTz stop_time,
						   int msec)
{
	TimestampTz diff = stop_time - start_time;

	return (diff >= msec * INT64CONST(1000));
}

#define XLogSegmentOffset(xlogptr, wal_segsz_bytes)	\
	((xlogptr) & ((wal_segsz_bytes) - 1))

#define MAXDATELEN		128

struct pg_tm
{
	int			tm_sec;
	int			tm_min;
	int			tm_hour;
	int			tm_mday;
	int			tm_mon;			/* see above */
	int			tm_year;		/* see above */
	int			tm_wday;
	int			tm_yday;
	int			tm_isdst;
	long int	tm_gmtoff;
	const char *tm_zone;
};

/*
 * Produce a C-string representation of a TimestampTz.
 *
 * This is mostly for use in emitting messages.  The primary difference
 * from timestamptz_out is that we force the output format to ISO.  Note
 * also that the result is in a static buffer, not pstrdup'd.
 *
 * See also pg_strftime.
 */
const char *
timestamptz_to_str(TimestampTz t)
{
	// TODO:
    return "";
}

#define FullTransactionIdPrecedes(a, b)	((a).value < (b).value)

#endif
