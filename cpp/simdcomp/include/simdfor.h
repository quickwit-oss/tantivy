/**
 * This code is released under a BSD License.
 */
#ifndef INCLUDE_SIMDFOR_H_
#define INCLUDE_SIMDFOR_H_

#include "portability.h"

/* SSE2 is required */
#include <emmintrin.h>

#include "simdcomputil.h"
#include "simdbitpacking.h"

#ifdef __cplusplus
extern "C" {
#endif

/* reads 128 values from "in", writes  "bit" 128-bit vectors to "out" */
void simdpackFOR(uint32_t initvalue, const uint32_t *  in,__m128i *  out, const uint32_t bit);


/* reads "bit" 128-bit vectors from "in", writes  128 values to "out" */
void simdunpackFOR(uint32_t initvalue, const __m128i *  in,uint32_t *  out, const uint32_t bit);


/* how many compressed bytes are needed to compressed length integers using a bit width of bit with 
the  simdpackFOR_length function. */
int simdpackFOR_compressedbytes(int length, const uint32_t bit);

/* like simdpackFOR, but supports an undetermined number of inputs. 
This is useful if you need to pack less than 128 integers. Note that this function is much slower. 
 Compressed data is stored in the memory location between 
 the provided (out) pointer and the returned pointer. */
__m128i * simdpackFOR_length(uint32_t initvalue, const uint32_t *   in, int length, __m128i *    out, const uint32_t bit);

/* like simdunpackFOR, but supports an undetermined number of inputs. 
This is useful if you need to unpack less than 128 integers. Note that this function is much slower. 
 The read compressed data is between the provided 
 (in) pointer and the returned pointer.  */
const __m128i * simdunpackFOR_length(uint32_t initvalue, const __m128i *   in, int length, uint32_t * out, const uint32_t bit);


/* returns the value stored at the specified "slot".
* */
uint32_t simdselectFOR(uint32_t initvalue, const __m128i *in, uint32_t bit,
                int slot);

/* given a block of 128 packed values, this function sets the value at index "index" to "value" */
void simdfastsetFOR(uint32_t initvalue, __m128i * in, uint32_t bit, uint32_t value, size_t index);


/* searches "bit" 128-bit vectors from "in" (= length<=128 encoded integers) for the first encoded uint32 value
 * which is >= |key|, and returns its position. It is assumed that the values
 * stored are in sorted order.
 * The encoded key is stored in "*presult".
 * The first length decoded integers, ignoring others. If no value is larger or equal to the key,
 * length is returned. Length should be no larger than 128.
 *
 * If no value is larger or equal to the key,
* length is returned */
int simdsearchwithlengthFOR(uint32_t initvalue, const __m128i *in, uint32_t bit,
                int length, uint32_t key, uint32_t *presult);

#ifdef __cplusplus
} // extern "C"
#endif




#endif /* INCLUDE_SIMDFOR_H_ */
