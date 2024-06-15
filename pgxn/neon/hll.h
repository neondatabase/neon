/*-------------------------------------------------------------------------
 *
 * hll.h
 *	  Sliding HyperLogLog cardinality estimator
 *
 * Portions Copyright (c) 2014-2023, PostgreSQL Global Development Group
 *
 * Implements https://hal.science/hal-00465313/document
 * 
 * Based on Hideaki Ohno's C++ implementation.  This is probably not ideally
 * suited to estimating the cardinality of very large sets;  in particular, we
 * have not attempted to further optimize the implementation as described in
 * the Heule, Nunkesser and Hall paper "HyperLogLog in Practice: Algorithmic
 * Engineering of a State of The Art Cardinality Estimation Algorithm".
 *
 * A sparse representation of HyperLogLog state is used, with fixed space
 * overhead.
 *
 * The copyright terms of Ohno's original version (the MIT license) follow.
 *
 * IDENTIFICATION
 *	  src/backend/lib/hyperloglog.c
 *
 *-------------------------------------------------------------------------
 */

/*
 * Copyright (c) 2013 Hideaki Ohno <hide.o.j55{at}gmail.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the 'Software'), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 */

#ifndef HLL_H
#define HLL_H

#define HLL_BIT_WIDTH   10
#define HLL_C_BITS      (32 - HLL_BIT_WIDTH)
#define HLL_N_REGISTERS (1 << HLL_BIT_WIDTH)

/* Future possible maximum */
typedef struct FPM
{
	uint8 R;
	TimestampTz ts;
} FPM;

typedef struct LFPM
{
	FPM fpm[HLL_C_BITS];
	size_t size;
} LFPM;

/*
 * HyperLogLog is an approximate technique for computing the number of distinct
 * entries in a set.  Importantly, it does this by using a fixed amount of
 * memory.  See the 2007 paper "HyperLogLog: the analysis of a near-optimal
 * cardinality estimation algorithm" for more.
 */
typedef struct HyperLogLogState
{
	time_t		window; /* window size in microseconds */
	LFPM	    regs[HLL_N_REGISTERS];
} HyperLogLogState;

extern void   initHyperLogLog(HyperLogLogState *cState, time_t max_duration);
extern void   addHyperLogLog(HyperLogLogState *cState, uint32 hash);
extern double estimateHyperLogLog(HyperLogLogState *cState, time_t dutration);

#endif
