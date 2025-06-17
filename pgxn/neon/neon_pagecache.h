/*
 * This is for the special ioctl in the neon_pagecache kernel module.
 *
 * DO NOT MODIFY! This header must agree with what the kernel module was
 * compiled with!
 */

#ifndef NEON_PAGECACHE_H
#define NEON_PAGECACHE_H

#include <linux/types.h>


#define POSTGRES_PAGE_SIZE 8192   // 8 KiB

struct neon_key {
    __u64 key_hi;     // Upper 64 bits of 128-bit key
    __u64 key_lo;     // Lower 64 bits of 128-bit key
};

struct neon_rw_args {
    struct neon_key key;
    __u32 offset;     // Offset within page (0-8191)
    __u32 length;     // Length to read/write
    __u64 buffer;     // User buffer address
};

#define NEON_IOC_MAGIC 'N'

#define NEON_IOCTL_READ    _IOWR(NEON_IOC_MAGIC, 1, struct neon_rw_args)
#define NEON_IOCTL_WRITE   _IOWR(NEON_IOC_MAGIC, 2, struct neon_rw_args)



#endif /* NEON_PAGECACHE_H */
