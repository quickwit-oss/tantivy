/**
 * This code is released under a BSD License.
 */

#include "simdcomputil.h"
#ifdef __SSE4_1__
#include <smmintrin.h>
#endif
#include <assert.h>

#define Delta(curr, prev) \
    _mm_sub_epi32(curr, \
            _mm_or_si128(_mm_slli_si128(curr, 4), _mm_srli_si128(prev, 12)))

/* returns the integer logarithm of v (bit width) */
uint32_t bits(const uint32_t v) {
#ifdef _MSC_VER
    unsigned long answer;
    if (v == 0) {
        return 0;
    }
    _BitScanReverse(&answer, v);
    return answer + 1;
#else
    return v == 0 ? 0 : 32 - __builtin_clz(v); /* assume GCC-like compiler if not microsoft */
#endif
}



static uint32_t maxbitas32int(const __m128i accumulator) {
	const __m128i _tmp1 = _mm_or_si128(_mm_srli_si128(accumulator, 8), accumulator); /* (A,B,C,D) xor (0,0,A,B) = (A,B,C xor A,D xor B)*/
	const __m128i _tmp2 = _mm_or_si128(_mm_srli_si128(_tmp1, 4), _tmp1); /*  (A,B,C xor A,D xor B) xor  (0,0,0,C xor A)*/
	uint32_t ans =  _mm_cvtsi128_si32(_tmp2);
	return bits(ans);
}

SIMDCOMP_PURE uint32_t maxbits(const uint32_t * begin) {
	    const __m128i* pin = (const __m128i*)(begin);
	    __m128i accumulator = _mm_loadu_si128(pin);
	    uint32_t k = 1;
	    for(; 4*k < SIMDBlockSize; ++k) {
	    	__m128i newvec = _mm_loadu_si128(pin+k);
	        accumulator = _mm_or_si128(accumulator,newvec);
	    }
	    return maxbitas32int(accumulator);
}
static uint32_t orasint(const __m128i accumulator) {
	const __m128i _tmp1 = _mm_or_si128(_mm_srli_si128(accumulator, 8), accumulator); /* (A,B,C,D) xor (0,0,A,B) = (A,B,C xor A,D xor B)*/
	const __m128i _tmp2 = _mm_or_si128(_mm_srli_si128(_tmp1, 4), _tmp1); /*  (A,B,C xor A,D xor B) xor  (0,0,0,C xor A)*/
	return  _mm_cvtsi128_si32(_tmp2);
}

#ifdef __SSE4_1__

static uint32_t minasint(const __m128i accumulator) {
	const __m128i _tmp1 = _mm_min_epu32(_mm_srli_si128(accumulator, 8), accumulator); /* (A,B,C,D) xor (0,0,A,B) = (A,B,C xor A,D xor B)*/
	const __m128i _tmp2 = _mm_min_epu32(_mm_srli_si128(_tmp1, 4), _tmp1); /*  (A,B,C xor A,D xor B) xor  (0,0,0,C xor A)*/
	return  _mm_cvtsi128_si32(_tmp2);
}

static uint32_t maxasint(const __m128i accumulator) {
	const __m128i _tmp1 = _mm_max_epu32(_mm_srli_si128(accumulator, 8), accumulator); /* (A,B,C,D) xor (0,0,A,B) = (A,B,C xor A,D xor B)*/
	const __m128i _tmp2 = _mm_max_epu32(_mm_srli_si128(_tmp1, 4), _tmp1); /*  (A,B,C xor A,D xor B) xor  (0,0,0,C xor A)*/
	return  _mm_cvtsi128_si32(_tmp2);
}

uint32_t simdmin(const uint32_t * in) {
    const __m128i* pin = (const __m128i*)(in);
    __m128i accumulator =  _mm_loadu_si128(pin);
     uint32_t k = 1;
     for(; 4*k < SIMDBlockSize; ++k) {
    	 __m128i newvec = _mm_loadu_si128(pin+k);
         accumulator = _mm_min_epu32(accumulator,newvec);
     }
     return minasint(accumulator);
}

void simdmaxmin(const uint32_t * in, uint32_t * getmin, uint32_t * getmax) {
    const __m128i* pin = (const __m128i*)(in);
    __m128i minaccumulator =  _mm_loadu_si128(pin);
    __m128i maxaccumulator =  minaccumulator;
    uint32_t k = 1;
     for(; 4*k < SIMDBlockSize; ++k) {
    	 __m128i newvec = _mm_loadu_si128(pin+k);
         minaccumulator = _mm_min_epu32(minaccumulator,newvec);
         maxaccumulator = _mm_max_epu32(maxaccumulator,newvec);
     }
     *getmin = minasint(minaccumulator);
     *getmax = maxasint(maxaccumulator);
}


uint32_t simdmin_length(const uint32_t * in, uint32_t length) {
	uint32_t currentmin = 0xFFFFFFFF;
	uint32_t lengthdividedby4 = length / 4;
	uint32_t offset = lengthdividedby4 * 4;
	uint32_t k;
	if (lengthdividedby4 > 0) {
		const __m128i* pin = (const __m128i*)(in);
		__m128i accumulator = _mm_loadu_si128(pin);
		k = 1;
		for(; 4*k < lengthdividedby4 * 4; ++k) {
			__m128i newvec = _mm_loadu_si128(pin+k);
			accumulator = _mm_min_epu32(accumulator,newvec);
		}
		currentmin = minasint(accumulator);
	}
	for (k = offset; k < length; ++k)
		if (in[k] < currentmin)
			currentmin = in[k];
	return currentmin;
}

void simdmaxmin_length(const uint32_t * in, uint32_t length, uint32_t * getmin, uint32_t * getmax) {
	uint32_t lengthdividedby4 = length / 4;
	uint32_t offset = lengthdividedby4 * 4;
	uint32_t k;
	*getmin = 0xFFFFFFFF;
	*getmax = 0;
	if (lengthdividedby4 > 0) {
		const __m128i* pin = (const __m128i*)(in);
		__m128i minaccumulator = _mm_loadu_si128(pin);
		__m128i maxaccumulator = minaccumulator;
		k = 1;
		for(; 4*k < lengthdividedby4 * 4; ++k) {
			__m128i newvec = _mm_loadu_si128(pin+k);
			minaccumulator = _mm_min_epu32(minaccumulator,newvec);
			maxaccumulator = _mm_max_epu32(maxaccumulator,newvec);
		}
		*getmin = minasint(minaccumulator);
		*getmax = maxasint(maxaccumulator);
	}
	for (k = offset; k < length; ++k) {
		if (in[k] < *getmin)
			*getmin = in[k];
		if (in[k] > *getmax)
			*getmax = in[k];
	}
}

#endif

SIMDCOMP_PURE uint32_t maxbits_length(const uint32_t * in,uint32_t length) {
	  uint32_t k;
	  uint32_t lengthdividedby4 = length / 4;
	  uint32_t offset = lengthdividedby4 * 4;
	  uint32_t bigxor = 0;
	  if(lengthdividedby4 > 0) {
		    const __m128i* pin = (const __m128i*)(in);
		    __m128i accumulator = _mm_loadu_si128(pin);
		    k = 1;
		    for(; 4*k < 4*lengthdividedby4; ++k) {
		    	__m128i newvec = _mm_loadu_si128(pin+k);
		        accumulator = _mm_or_si128(accumulator,newvec);
		    }
		    bigxor = orasint(accumulator);
	  }
	  for(k = offset; k < length; ++k)
		  bigxor |= in[k];
	  return bits(bigxor);
}


/* maxbit over 128 integers (SIMDBlockSize) with provided initial value */
uint32_t simdmaxbitsd1(uint32_t initvalue, const uint32_t * in) {
    __m128i  initoffset = _mm_set1_epi32 (initvalue);
    const __m128i* pin = (const __m128i*)(in);
    __m128i newvec = _mm_loadu_si128(pin);
    __m128i accumulator = Delta(newvec , initoffset);
    __m128i oldvec = newvec;
    uint32_t k = 1;
    for(; 4*k < SIMDBlockSize; ++k) {
        newvec = _mm_loadu_si128(pin+k);
        accumulator = _mm_or_si128(accumulator,Delta(newvec , oldvec));
        oldvec = newvec;
    }
    initoffset = oldvec;
    return maxbitas32int(accumulator);
}


/* maxbit over |length| integers with provided initial value */
uint32_t simdmaxbitsd1_length(uint32_t initvalue, const uint32_t * in,
                uint32_t length) {
    __m128i newvec;
    __m128i oldvec;
    __m128i initoffset;
    __m128i accumulator;
    const __m128i *pin;
    uint32_t tmparray[4];
    uint32_t k = 1;
    uint32_t acc;

    assert(length > 0);

    pin = (const __m128i *)(in);
    initoffset = _mm_set1_epi32(initvalue);
    switch (length) {
      case 1:
        newvec = _mm_set1_epi32(in[0]);
        break;
      case 2:
        newvec = _mm_setr_epi32(in[0], in[1], in[1], in[1]);
        break;
      case 3:
        newvec = _mm_setr_epi32(in[0], in[1], in[2], in[2]);
        break;
      default:
        newvec = _mm_loadu_si128(pin);
        break;
    }
    accumulator = Delta(newvec, initoffset);
    oldvec = newvec;

    /* process 4 integers and build an accumulator */
    while (k * 4 + 4 <= length) {
        newvec = _mm_loadu_si128(pin + k);
        accumulator = _mm_or_si128(accumulator, Delta(newvec, oldvec));
        oldvec = newvec;
        k++;
    }

    /* extract the accumulator as an integer */
    _mm_storeu_si128((__m128i *)(tmparray), accumulator);
    acc = tmparray[0] | tmparray[1] | tmparray[2] | tmparray[3];

    /* now process the remaining integers */
    for (k *= 4; k < length; k++)
        acc |= in[k] - (k == 0 ? initvalue : in[k - 1]);

    /* return the number of bits */
    return bits(acc);
}
