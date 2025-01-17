/*-------------------------------------------------------------------------
 *
 * hll.c
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

#include <math.h>

#include "postgres.h"
#include "funcapi.h"
#include "port/pg_bitutils.h"
#include "utils/timestamp.h"
#include "hll.h"


#define POW_2_32			(4294967296.0)
#define NEG_POW_2_32		(-4294967296.0)

#define ALPHA_MM ((0.7213 / (1.0 + 1.079 / HLL_N_REGISTERS)) * HLL_N_REGISTERS * HLL_N_REGISTERS)

/*
 * Worker for addHyperLogLog().
 *
 * Calculates the position of the first set bit in first b bits of x argument
 * starting from the first, reading from most significant to least significant
 * bits.
 *
 * Example (when considering fist 10 bits of x):
 *
 * rho(x = 0b1000000000)   returns 1
 * rho(x = 0b0010000000)   returns 3
 * rho(x = 0b0000000000)   returns b + 1
 *
 * "The binary address determined by the first b bits of x"
 *
 * Return value "j" used to index bit pattern to watch.
 */
static inline uint8
rho(uint32 x, uint8 b)
{
	uint8		j = 1;

	if (x == 0)
		return b + 1;

	j = 32 - pg_leftmost_one_pos32(x);

	if (j > b)
		return b + 1;

	return j;
}

/*
 * Initialize HyperLogLog track state
 */
void
initSHLL(HyperLogLogState *cState)
{
	memset(cState->regs, 0, sizeof(cState->regs));
}

/*
 * Adds element to the estimator, from caller-supplied hash.
 *
 * It is critical that the hash value passed be an actual hash value, typically
 * generated using hash_any().  The algorithm relies on a specific bit-pattern
 * observable in conjunction with stochastic averaging.  There must be a
 * uniform distribution of bits in hash values for each distinct original value
 * observed.
 */
void
addSHLL(HyperLogLogState *cState, uint32 hash)
{
	uint8		count;
	uint32		index;

	TimestampTz	now = GetCurrentTimestamp();
	/* Use the first "k" (registerWidth) bits as a zero based index */
	index = hash >> HLL_C_BITS;

	/* Compute the rank of the remaining 32 - "k" (registerWidth) bits */
	count = rho(hash << HLL_BIT_WIDTH, HLL_C_BITS);

	cState->regs[index][count] = now;
}

static uint8
getMaximum(const TimestampTz* reg, TimestampTz since)
{
	uint8 max = 0;

	for (size_t i = 0; i < HLL_C_BITS + 1; i++)
	{
		if (reg[i] >= since)
		{
			max = i;
		}
	}

	return max;
}


/*
 * Estimates cardinality, based on elements added so far
 */
double
estimateSHLL(HyperLogLogState *cState, time_t duration)
{
	double		result;
	double		sum = 0.0;
	size_t		i;
	uint8       R[HLL_N_REGISTERS];
	/* 0 indicates uninitialized timestamp, so if we need to cover the whole range than starts with 1 */
	TimestampTz since = duration == (time_t)-1 ? 1 : GetCurrentTimestamp() - duration * USECS_PER_SEC;

	for (i = 0; i < HLL_N_REGISTERS; i++)
	{
		R[i] = getMaximum(cState->regs[i], since);
		sum += 1.0 / pow(2.0, R[i]);
	}

	/* result set to "raw" HyperLogLog estimate (E in the HyperLogLog paper) */
	result = ALPHA_MM / sum;

	if (result <= (5.0 / 2.0) * HLL_N_REGISTERS)
	{
		/* Small range correction */
		int			zero_count = 0;

		for (i = 0; i < HLL_N_REGISTERS; i++)
		{
			zero_count += R[i] == 0;
		}

		if (zero_count != 0)
			result = HLL_N_REGISTERS * log((double) HLL_N_REGISTERS /
										   zero_count);
	}
	else if (result > (1.0 / 30.0) * POW_2_32)
	{
		/* Large range correction */
		result = NEG_POW_2_32 * log(1.0 - (result / POW_2_32));
	}

	return result;
}

