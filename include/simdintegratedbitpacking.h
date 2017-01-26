/**
 * This code is released under a BSD License.
 */

#ifndef SIMD_INTEGRATED_BITPACKING_H
#define SIMD_INTEGRATED_BITPACKING_H

#include "portability.h"

/* SSE2 is required */
#include <emmintrin.h>

#include "simdcomputil.h"
#include "simdbitpacking.h"

#ifdef __cplusplus
extern "C" {
#endif

/* reads 128 values from "in", writes  "bit" 128-bit vectors to "out"
   integer values should be in sorted order (for best results).
   The differences are masked so that only the least significant "bit" bits are used. */
void simdpackd1(uint32_t initvalue, const uint32_t *  in,__m128i *  out, const uint32_t bit);


/* reads 128 values from "in", writes  "bit" 128-bit vectors to "out"
   integer values should be in sorted order (for best results).
   The difference values are assumed to be less than 1<<bit. */
void simdpackwithoutmaskd1(uint32_t initvalue, const uint32_t *  in,__m128i *  out, const uint32_t bit);


/* reads "bit" 128-bit vectors from "in", writes  128 values to "out" */
void simdunpackd1(uint32_t initvalue, const __m128i *  in,uint32_t *  out, const uint32_t bit);


/* searches "bit" 128-bit vectors from "in" (= 128 encoded integers) for the first encoded uint32 value
 * which is >= |key|, and returns its position. It is assumed that the values
 * stored are in sorted order.
 * The encoded key is stored in "*presult". If no value is larger or equal to the key,
* 128 is returned. The pointer initOffset is a pointer to the last four value decoded
* (when starting out, this can be a zero vector or initialized with _mm_set1_epi32(init)),
* and the vector gets updated.
**/
int
simdsearchd1(__m128i * initOffset, const __m128i *in, uint32_t bit,
                uint32_t key, uint32_t *presult);


/* searches "bit" 128-bit vectors from "in" (= length<=128 encoded integers) for the first encoded uint32 value
 * which is >= |key|, and returns its position. It is assumed that the values
 * stored are in sorted order.
 * The encoded key is stored in "*presult".
 * The first length decoded integers, ignoring others. If no value is larger or equal to the key,
 * length is returned. Length should be no larger than 128.
 *
 * If no value is larger or equal to the key,
* length is returned */
int simdsearchwithlengthd1(uint32_t initvalue, const __m128i *in, uint32_t bit,
                int length, uint32_t key, uint32_t *presult);



/* returns the value stored at the specified "slot".
* */
uint32_t simdselectd1(uint32_t initvalue, const __m128i *in, uint32_t bit,
                int slot);

/* given a block of 128 packed values, this function sets the value at index "index" to "value",
 * you must somehow know the previous value.
 * Because of differential coding, all following values are incremented by the offset between this new
 * value and the old value... 
 * This functions is useful if you want to modify the last value. 
 */
void simdfastsetd1fromprevious( __m128i * in, uint32_t bit, uint32_t previousvalue, uint32_t value, size_t index);

/* given a block of 128 packed values, this function sets the value at index "index" to "value",
 * This function computes the previous value if needed.
 * Because of differential coding, all following values are incremented by the offset between this new
 * value and the old value...
 * This functions is useful if you want to modify the last value. 
 */
void simdfastsetd1(uint32_t initvalue, __m128i * in, uint32_t bit, uint32_t value, size_t index);


/*Simply scan the data
* The pointer initOffset is a pointer to the last four value decoded
* (when starting out, this can be a zero vector or initialized with _mm_set1_epi32(init);),
* and the vector gets updated.
* */

void
simdscand1(__m128i * initOffset, const __m128i *in, uint32_t bit);

#ifdef __cplusplus
} // extern "C"
#endif

#endif
