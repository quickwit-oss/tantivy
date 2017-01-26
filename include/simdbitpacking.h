/**
 * This code is released under a BSD License.
 */
#ifndef SIMDBITPACKING_H_
#define SIMDBITPACKING_H_

#include "portability.h"

/* SSE2 is required */
#include <emmintrin.h>
/* for memset */
#include <string.h>

#include "simdcomputil.h"

/***
* Please see example.c for various examples on how to make good use
* of these functions.
*/



/* reads 128 values from "in", writes  "bit" 128-bit vectors to "out".
 * The input values are masked so that only the least significant "bit" bits are used. */
void simdpack(const uint32_t *  in,__m128i *  out, const uint32_t bit);

/* reads 128 values from "in", writes  "bit" 128-bit vectors to "out".
 * The input values are assumed to be less than 1<<bit. */
void simdpackwithoutmask(const uint32_t *  in,__m128i *  out, const uint32_t bit);

/* reads  "bit" 128-bit vectors from "in", writes  128 values to "out" */
void simdunpack(const __m128i *  in,uint32_t *  out, const uint32_t bit);



/* how many compressed bytes are needed to compressed length integers using a bit width of bit with 
the  simdpackFOR_length function. */
int simdpack_compressedbytes(int length, const uint32_t bit);

/* like simdpack, but supports an undetermined number of inputs.
 * This is useful if you need to unpack an array of integers that is not divisible by 128 integers.
 * Returns a pointer to the (advanced) compressed array. Compressed data is stored in the memory location between 
 the provided (out) pointer and the returned pointer. */
__m128i * simdpack_length(const uint32_t *   in, size_t length, __m128i *    out, const uint32_t bit);

/* like simdunpack, but supports an undetermined number of inputs.
 * This is useful if you need to unpack an array of integers that is not divisible by 128 integers.
 * Returns a pointer to the (advanced) compressed array. The read compressed data is between the provided 
 (in) pointer and the returned pointer. */
const __m128i * simdunpack_length(const __m128i *   in, size_t length, uint32_t * out, const uint32_t bit);




/* like simdpack, but supports an undetermined small number of inputs. This is useful if you need to pack less 
than 128 integers.
 * Note that this function is much slower.
 * Returns a pointer to the (advanced) compressed array. Compressed data is stored in the memory location 
 between the provided (out) pointer and the returned pointer. */
__m128i * simdpack_shortlength(const uint32_t *   in, int length, __m128i *    out, const uint32_t bit);

/* like simdunpack, but supports an undetermined small number of inputs. This is useful if you need to unpack less
 than 128 integers.
 * Note that this function is much slower.
 * Returns a pointer to the (advanced) compressed array. The read compressed data is between the provided (in) 
 pointer and the returned pointer. */
const __m128i * simdunpack_shortlength(const __m128i *   in, int length, uint32_t * out, const uint32_t bit);

/* given a block of 128 packed values, this function sets the value at index "index" to "value" */
void simdfastset(__m128i * in128, uint32_t b, uint32_t value, size_t index);

#endif /* SIMDBITPACKING_H_ */
