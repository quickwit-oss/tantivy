/**
 * This code is released under a BSD License.
 */

#ifndef INCLUDE_AVXBITPACKING_H_
#define INCLUDE_AVXBITPACKING_H_


#ifdef __AVX2__

#include "portability.h"


/* AVX2 is required */
#include <immintrin.h>
/* for memset */
#include <string.h>

#include "simdcomputil.h"

enum{ AVXBlockSize = 256};

/* max integer logarithm over a range of AVXBlockSize integers (256 integer) */
uint32_t avxmaxbits(const uint32_t * begin);

/* reads 256 values from "in", writes  "bit" 256-bit vectors to "out" */
void avxpack(const uint32_t *  in,__m256i *  out, const uint32_t bit);

/* reads 256 values from "in", writes  "bit" 256-bit vectors to "out" */
void avxpackwithoutmask(const uint32_t *  in,__m256i *  out, const uint32_t bit);

/* reads  "bit" 256-bit vectors from "in", writes  256 values to "out" */
void avxunpack(const __m256i *  in,uint32_t *  out, const uint32_t bit);




#endif /* __AVX2__ */

#endif /* INCLUDE_AVXBITPACKING_H_ */
