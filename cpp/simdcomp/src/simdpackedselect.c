/**
 * This code is released under a BSD License.
 */
#ifdef __SSE4_1__
#include "simdintegratedbitpacking.h"
#include <smmintrin.h>


SIMDCOMP_ALIGNED(16) int8_t shuffle_mask_bytes[256] = {
		0,1,2,3,0,0,0,0,0,0,0,0,0,0,0,0,
		4,5,6,7,0,0,0,0,0,0,0,0,0,0,0,0,
		8,9,10,11,0,0,0,0,0,0,0,0,0,0,0,0,
		12,13,14,15,0,0,0,0,0,0,0,0,0,0,0,0,
    };

static const __m128i *shuffle_mask = (__m128i *) shuffle_mask_bytes;

uint32_t branchlessextract (__m128i out, int i)  {
  return _mm_cvtsi128_si32(_mm_shuffle_epi8(out,shuffle_mask[i]));
}

#define PrefixSum(ret, curr, prev) do {                                     \
    const __m128i _tmp1 = _mm_add_epi32(_mm_slli_si128(curr, 8), curr);     \
    const __m128i _tmp2 = _mm_add_epi32(_mm_slli_si128(_tmp1, 4), _tmp1);   \
    ret = _mm_add_epi32(_tmp2, _mm_shuffle_epi32(prev, 0xff));              \
	} while (0)

#define CHECK_AND_INCREMENT(i, out, slot)                                   \
            i += 4;                                                         \
            if (i > slot) {                                                 \
              return branchlessextract (out, slot - (i - 4));                \
            }


static uint32_t
iunpackselect1(__m128i * initOffset, const __m128i *in, int slot)
{
  int i = 0;
  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<1)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,1);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,3);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,5);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,7);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,9);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,10);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,11);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,13);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,14);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,15);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,17);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,18);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,19);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,21);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,22);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,23);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,25);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,26);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,27);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,29);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,30);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,31);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);


  return (0);
}

static uint32_t
iunpackselect2(__m128i * initOffset, const __m128i *in, int slot)
{
  int i = 0;
  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<2)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,10);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,14);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,18);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,22);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,26);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,10);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,14);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,18);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,22);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,26);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);


  return (0);
}

static uint32_t
iunpackselect3(__m128i * initOffset, const  __m128i *in, int slot)
{
  int i = 0;
  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<3)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,3);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,9);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,15);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,18);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,21);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,27);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 3-1), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,1);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,7);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,10);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,13);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,19);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,22);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,25);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,31);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 3-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,5);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,11);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,14);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,17);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,23);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,26);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,29);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);


  return (0);
}

static uint32_t
iunpackselect4(__m128i * initOffset, const __m128i *in, int slot)
{
  int i = 0;
  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<4)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);


  return (0);
}

static uint32_t
iunpackselect5(__m128i * initOffset, const __m128i *in, int slot)
{
  int i = 0;
  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<5)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,5);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,10);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,15);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,25);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 5-3), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,3);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,13);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,18);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,23);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 5-1), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,1);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,11);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,21);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,26);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,31);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 5-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,9);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,14);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,19);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,29);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 5-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,7);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,17);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,22);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,27);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);


  return (0);
}

static uint32_t
iunpackselect6(__m128i * initOffset, const __m128i *in, int slot)
{
  int i = 0;
  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<6)-1);
 
  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,18);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 6-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,10);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,22);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 6-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,14);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,18);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 6-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,10);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,22);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 6-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,14);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);


  return (0);
}

static uint32_t
iunpackselect7(__m128i * initOffset, const __m128i *in, int slot)
{
  int i = 0;
  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<7)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,7);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,14);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,21);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 7-3), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,3);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,10);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,17);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,31);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 7-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,13);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,27);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 7-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,9);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,23);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 7-5), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,5);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,19);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 7-1), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,1);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,15);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,22);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,29);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 7-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,11);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,18);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,25);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);


  return (0);
}

static uint32_t
iunpackselect8(__m128i * initOffset, const __m128i *in, int slot)
{
  int i = 0;
  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<8)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);


  return (0);
}

static uint32_t
iunpackselect9(__m128i * initOffset, const __m128i *in, int slot)
{
  int i = 0;
  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<9)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,9);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,18);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,27);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 9-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,13);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,22);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,31);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 9-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,17);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 9-3), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,3);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,21);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 9-7), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,7);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,25);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 9-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,11);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,29);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 9-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,15);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 9-1), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,1);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,10);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,19);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 9-5), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,5);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,14);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,23);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);


  return (0);
}

static uint32_t
iunpackselect10(__m128i * initOffset, const __m128i *in, int slot)
{
  int i = 0;
  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<10)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,10);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 10-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,18);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 10-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 10-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,14);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 10-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,10);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 10-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,18);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 10-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 10-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,14);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 10-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);


  return (0);
}

static uint32_t
iunpackselect11(__m128i * initOffset, const __m128i *in, int slot)
{
  int i = 0;
  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<11)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,11);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 11-1), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,1);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,23);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 11-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,13);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 11-3), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,3);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,14);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,25);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 11-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,15);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 11-5), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,5);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,27);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 11-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,17);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 11-7), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,7);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,18);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,29);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 11-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,19);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 11-9), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,9);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,31);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 11-10), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,10);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,21);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);


  return (0);
}

static uint32_t
iunpackselect12(__m128i * initOffset, const __m128i *in, int slot)
{
  int i = 0;
  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<12)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 12-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 12-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 12-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 12-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 12-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 12-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 12-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 12-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);


  return (0);
}

static uint32_t
iunpackselect13(__m128i * initOffset, const __m128i *in, int slot)
{
  int i = 0;
  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<13)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,13);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 13-7), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,7);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 13-1), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,1);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,14);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,27);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 13-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,21);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 13-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,15);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 13-9), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,9);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 13-3), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,3);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,29);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 13-10), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,10);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,23);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 13-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,17);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 13-11), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,11);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 13-5), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,5);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,18);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,31);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 13-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,25);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 13-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,19);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);


  return (0);
}

static uint32_t
iunpackselect14(__m128i * initOffset, const __m128i *in, int slot)
{
  int i = 0;
  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<14)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,14);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 14-10), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,10);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 14-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 14-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 14-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 14-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 14-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,18);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,14);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 14-10), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,10);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 14-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 14-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 14-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 14-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 14-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,18);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);


  return (0);
}

static uint32_t
iunpackselect15(__m128i * initOffset, const __m128i *in, int slot)
{
  int i = 0;
  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<15)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,15);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 15-13), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,13);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 15-11), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,11);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 15-9), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,9);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 15-7), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,7);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 15-5), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,5);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 15-3), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,3);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,18);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 15-1), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,1);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,31);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 15-14), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,14);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,29);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 15-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,27);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 15-10), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,10);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,25);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 15-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,23);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 15-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,21);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 15-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,19);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 15-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,17);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);


  return (0);
}

static uint32_t
iunpackselect16(__m128i * initOffset, const __m128i *in, int slot)
{
  int i = 0;
  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<16)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);


  return (0);
}

static uint32_t
iunpackselect17(__m128i * initOffset, const __m128i *in, int slot)
{
  int i = 0;
  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<17)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,17);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 17-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,19);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 17-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,21);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 17-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,23);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 17-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,25);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 17-10), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,10);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,27);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 17-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,29);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 17-14), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,14);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,31);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 17-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 17-1), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,1);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,18);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 17-3), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,3);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 17-5), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,5);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 17-7), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,7);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 17-9), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,9);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 17-11), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,11);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 17-13), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,13);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 17-15), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,15);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);


  return (0);
}

static uint32_t
iunpackselect18(__m128i * initOffset, const __m128i *in, int slot)
{
  int i = 0;
  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<18)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,18);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 18-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 18-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 18-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 18-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 18-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 18-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 18-10), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,10);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 18-14), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,14);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,18);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 18-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 18-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 18-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 18-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 18-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 18-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 18-10), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,10);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 18-14), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,14);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);


  return (0);
}

static uint32_t
iunpackselect19(__m128i * initOffset, const __m128i *in, int slot)
{
  int i = 0;
  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<19)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,19);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 19-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,25);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 19-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,31);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 19-18), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,18);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 19-5), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,5);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 19-11), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,11);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 19-17), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,17);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 19-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,23);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 19-10), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,10);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,29);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 19-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 19-3), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,3);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 19-9), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,9);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 19-15), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,15);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 19-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,21);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 19-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,27);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 19-14), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,14);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 19-1), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,1);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 19-7), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,7);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 19-13), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,13);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);


  return (0);
}

static uint32_t
iunpackselect20(__m128i * initOffset, const __m128i *in, int slot)
{
  int i = 0;
  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<20)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 20-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 20-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 20-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 20-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 20-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 20-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 20-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 20-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 20-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 20-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 20-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 20-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 20-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 20-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 20-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 20-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);


  return (0);
}

static uint32_t
iunpackselect21(__m128i * initOffset, const __m128i *in, int slot)
{
  int i = 0;
  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<21)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,21);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 21-10), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,10);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,31);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 21-20), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 21-9), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,9);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 21-19), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,19);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 21-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,29);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 21-18), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,18);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 21-7), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,7);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 21-17), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,17);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 21-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,27);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 21-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 21-5), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,5);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 21-15), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,15);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 21-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,25);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 21-14), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,14);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 21-3), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,3);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 21-13), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,13);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 21-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,23);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 21-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 21-1), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,1);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 21-11), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,11);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);


  return (0);
}

static uint32_t
iunpackselect22(__m128i * initOffset, const __m128i *in, int slot)
{
  int i = 0;
  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<22)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 22-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 22-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 22-14), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,14);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 22-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 22-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 22-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 22-18), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,18);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 22-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 22-20), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 22-10), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,10);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 22-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 22-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 22-14), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,14);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 22-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 22-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 22-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 22-18), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,18);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 22-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 22-20), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 22-10), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,10);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);


  return (0);
}

static uint32_t
iunpackselect23(__m128i * initOffset, const __m128i *in, int slot)
{
  int i = 0;
  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<23)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,23);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 23-14), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,14);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 23-5), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,5);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 23-19), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,19);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 23-10), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,10);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 23-1), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,1);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 23-15), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,15);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 23-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,29);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 23-20), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 23-11), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,11);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 23-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,25);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 23-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 23-7), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,7);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 23-21), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,21);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 23-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 23-3), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,3);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 23-17), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,17);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 23-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,31);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 23-22), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 23-13), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,13);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 23-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,27);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 23-18), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,18);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 23-9), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,9);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);


  return (0);
}

static uint32_t
iunpackselect24(__m128i * initOffset, const __m128i *in, int slot)
{
  int i = 0;
  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<24)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 24-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 24-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 24-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 24-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 24-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 24-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 24-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 24-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 24-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 24-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 24-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 24-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 24-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 24-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 24-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 24-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);


  return (0);
}

static uint32_t
iunpackselect25(__m128i * initOffset, const __m128i *in, int slot)
{
  int i = 0;
  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<25)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,25);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-18), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,18);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-11), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,11);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,29);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-22), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-15), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,15);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-1), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,1);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-19), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,19);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-5), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,5);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-23), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,23);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-9), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,9);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,27);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-20), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-13), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,13);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,31);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-24), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-17), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,17);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-10), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,10);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-3), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,3);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-21), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,21);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-14), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,14);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-7), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,7);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);


  return (0);
}

static uint32_t
iunpackselect26(__m128i * initOffset, const __m128i *in, int slot)
{
  int i = 0;
  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<26)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-20), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-14), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,14);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-22), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-10), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,10);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-24), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-18), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,18);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,6);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-20), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-14), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,14);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-22), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-10), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,10);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-24), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-18), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,18);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,6);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);


  return (0);
}

static uint32_t
iunpackselect27(__m128i * initOffset, const __m128i *in, int slot)
{
  int i = 0;
  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<27)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,27);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-22), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-17), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,17);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-7), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,7);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,29);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-24), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-19), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,19);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-14), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,14);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-9), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,9);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,31);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-26), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-21), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,21);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-11), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,11);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,6);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-1), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,1);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-23), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,23);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-18), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,18);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-13), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,13);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-3), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,3);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-25), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,25);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-20), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-15), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,15);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-10), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,10);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-5), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,5);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);


  return (0);
}

static uint32_t
iunpackselect28(__m128i * initOffset, const __m128i *in, int slot)
{
  int i = 0;
  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<28)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-24), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-20), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-24), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-20), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-24), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-20), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-24), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-20), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);


  return (0);
}

static uint32_t
iunpackselect29(__m128i * initOffset, const __m128i *in, int slot)
{
  int i = 0;
  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<29)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,29);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-26), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-23), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,23);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-20), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-17), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,17);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-14), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,14);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-11), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,11);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-5), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,5);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,31);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-28), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-25), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,25);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-22), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-19), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,19);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-13), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,13);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-10), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,10);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-7), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,7);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-1), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,1);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-27), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,27);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-24), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-21), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,21);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-18), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,18);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-15), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,15);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-9), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,9);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,6);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-3), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,3);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);


  return (0);
}

static uint32_t
iunpackselect30(__m128i * initOffset, const __m128i *in, int slot)
{
  int i = 0;
  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<30)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-28), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-26), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-24), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-22), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-20), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-18), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,18);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-14), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,14);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-10), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,10);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,6);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,2);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-28), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-26), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-24), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-22), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-20), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-18), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,18);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-14), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,14);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-10), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,10);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,6);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,2);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);


  return (0);
}

static uint32_t
iunpackselect31(__m128i * initOffset, const __m128i *in, int slot)
{
  int i = 0;
  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<31)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,31);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-30), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-29), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,29);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-28), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-27), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,27);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-26), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-25), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,25);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-24), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-23), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,23);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-22), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-21), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,21);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-20), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-19), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,19);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-18), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,18);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-17), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,17);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-15), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,15);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-14), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,14);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-13), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,13);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,12);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-11), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,11);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-10), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,10);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-9), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,9);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,8);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-7), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,7);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,6);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-5), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,5);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,4);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-3), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,3);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,2);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-1), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);

  tmp = _mm_srli_epi32(InReg,1);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;
  CHECK_AND_INCREMENT(i, out, slot);


  return (0);
}

static uint32_t
iunpackselect32(__m128i * initOffset , const __m128i *in, int slot)
{
  uint32_t *begin = (uint32_t *)in;
  *initOffset = _mm_load_si128(in + 31);
  return begin[slot];
}


uint32_t
simdselectd1(uint32_t init, const __m128i *in, uint32_t bit, int slot)
{
	__m128i  vecinitOffset = _mm_set1_epi32(init);
	__m128i * initOffset = &vecinitOffset;
	slot &= 127; /* to avoid problems */

   switch (bit) {
    case 0: return _mm_extract_epi32(*initOffset,3); break;

    case 1: return iunpackselect1(initOffset, in, slot); break;

    case 2: return iunpackselect2(initOffset, in, slot); break;

    case 3: return iunpackselect3(initOffset, in, slot); break;

    case 4: return iunpackselect4(initOffset, in, slot); break;

    case 5: return iunpackselect5(initOffset, in, slot); break;

    case 6: return iunpackselect6(initOffset, in, slot); break;

    case 7: return iunpackselect7(initOffset, in, slot); break;

    case 8: return iunpackselect8(initOffset, in, slot); break;

    case 9: return iunpackselect9(initOffset, in, slot); break;

    case 10: return iunpackselect10(initOffset, in, slot); break;

    case 11: return iunpackselect11(initOffset, in, slot); break;

    case 12: return iunpackselect12(initOffset, in, slot); break;

    case 13: return iunpackselect13(initOffset, in, slot); break;

    case 14: return iunpackselect14(initOffset, in, slot); break;

    case 15: return iunpackselect15(initOffset, in, slot); break;

    case 16: return iunpackselect16(initOffset, in, slot); break;

    case 17: return iunpackselect17(initOffset, in, slot); break;

    case 18: return iunpackselect18(initOffset, in, slot); break;

    case 19: return iunpackselect19(initOffset, in, slot); break;

    case 20: return iunpackselect20(initOffset, in, slot); break;

    case 21: return iunpackselect21(initOffset, in, slot); break;

    case 22: return iunpackselect22(initOffset, in, slot); break;

    case 23: return iunpackselect23(initOffset, in, slot); break;

    case 24: return iunpackselect24(initOffset, in, slot); break;

    case 25: return iunpackselect25(initOffset, in, slot); break;

    case 26: return iunpackselect26(initOffset, in, slot); break;

    case 27: return iunpackselect27(initOffset, in, slot); break;

    case 28: return iunpackselect28(initOffset, in, slot); break;

    case 29: return iunpackselect29(initOffset, in, slot); break;

    case 30: return iunpackselect30(initOffset, in, slot); break;

    case 31: return iunpackselect31(initOffset, in, slot); break;

    case 32: return iunpackselect32(initOffset, in, slot); break;

    default: break;
   }

   return (-1);
}

static void
iunpackscan1(__m128i * initOffset, const __m128i *in)
{

  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<1)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,1);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,3);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,5);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,7);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,9);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,10);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,11);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,13);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,14);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,15);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,17);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,18);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,19);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,21);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,22);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,23);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,25);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,26);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,27);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,29);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,30);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,31);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;



}

static void
iunpackscan2(__m128i * initOffset, const __m128i *in)
{

  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<2)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,10);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,14);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,18);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,22);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,26);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,10);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,14);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,18);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,22);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,26);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;



}

static void
iunpackscan3(__m128i * initOffset, const  __m128i *in)
{

  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<3)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,3);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,9);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,15);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,18);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,21);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,27);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 3-1), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,1);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,7);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,10);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,13);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,19);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,22);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,25);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,31);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 3-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,5);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,11);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,14);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,17);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,23);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,26);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,29);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;



}

static void
iunpackscan4(__m128i * initOffset, const __m128i *in)
{

  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<4)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;



}

static void
iunpackscan5(__m128i * initOffset, const __m128i *in)
{

  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<5)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,5);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,10);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,15);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,25);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 5-3), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,3);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,13);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,18);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,23);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 5-1), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,1);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,11);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,21);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,26);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,31);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 5-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,9);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,14);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,19);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,29);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 5-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,7);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,17);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,22);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,27);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;



}

static void
iunpackscan6(__m128i * initOffset, const __m128i *in)
{

  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<6)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,18);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 6-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,10);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,22);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 6-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,14);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,18);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 6-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,10);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,22);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 6-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,14);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;



}

static void
iunpackscan7(__m128i * initOffset, const __m128i *in)
{

  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<7)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,7);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,14);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,21);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 7-3), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,3);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,10);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,17);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,31);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 7-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,13);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,27);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 7-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,9);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,23);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 7-5), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,5);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,19);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 7-1), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,1);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,15);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,22);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,29);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 7-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,11);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,18);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,25);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;



}

static void
iunpackscan8(__m128i * initOffset, const __m128i *in)
{

  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<8)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;



}

static void
iunpackscan9(__m128i * initOffset, const __m128i *in)
{

  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<9)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,9);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,18);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,27);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 9-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,13);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,22);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,31);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 9-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,17);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 9-3), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,3);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,21);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 9-7), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,7);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,25);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 9-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,11);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,29);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 9-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,15);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 9-1), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,1);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,10);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,19);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 9-5), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,5);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,14);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,23);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;



}

static void
iunpackscan10(__m128i * initOffset, const __m128i *in)
{

  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<10)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,10);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 10-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,18);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 10-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 10-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,14);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 10-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,10);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 10-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,18);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 10-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 10-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,14);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 10-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;



}

static void
iunpackscan11(__m128i * initOffset, const __m128i *in)
{

  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<11)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,11);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 11-1), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,1);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,23);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 11-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,13);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 11-3), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,3);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,14);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,25);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 11-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,15);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 11-5), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,5);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,27);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 11-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,17);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 11-7), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,7);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,18);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,29);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 11-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,19);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 11-9), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,9);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,31);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 11-10), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,10);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,21);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;



}

static void
iunpackscan12(__m128i * initOffset, const __m128i *in)
{

  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<12)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 12-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 12-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 12-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 12-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 12-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 12-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 12-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 12-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;



}

static void
iunpackscan13(__m128i * initOffset, const __m128i *in)
{

  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<13)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,13);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 13-7), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,7);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 13-1), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,1);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,14);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,27);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 13-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,21);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 13-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,15);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 13-9), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,9);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 13-3), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,3);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,29);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 13-10), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,10);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,23);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 13-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,17);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 13-11), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,11);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 13-5), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,5);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,18);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,31);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 13-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,25);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 13-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,19);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;



}

static void
iunpackscan14(__m128i * initOffset, const __m128i *in)
{

  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<14)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,14);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 14-10), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,10);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 14-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 14-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 14-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 14-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 14-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,18);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,14);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 14-10), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,10);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 14-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 14-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 14-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 14-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 14-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,18);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;



}

static void
iunpackscan15(__m128i * initOffset, const __m128i *in)
{

  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<15)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,15);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 15-13), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,13);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 15-11), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,11);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 15-9), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,9);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 15-7), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,7);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 15-5), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,5);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 15-3), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,3);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,18);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 15-1), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,1);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,31);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 15-14), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,14);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,29);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 15-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,27);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 15-10), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,10);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,25);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 15-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,23);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 15-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,21);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 15-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,19);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 15-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,17);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;



}

static void
iunpackscan16(__m128i * initOffset, const __m128i *in)
{

  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<16)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;



}

static void
iunpackscan17(__m128i * initOffset, const __m128i *in)
{

  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<17)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,17);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 17-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,19);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 17-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,21);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 17-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,23);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 17-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,25);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 17-10), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,10);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,27);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 17-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,29);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 17-14), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,14);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,31);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 17-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 17-1), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,1);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,18);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 17-3), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,3);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 17-5), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,5);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 17-7), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,7);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 17-9), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,9);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 17-11), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,11);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 17-13), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,13);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 17-15), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,15);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;



}

static void
iunpackscan18(__m128i * initOffset, const __m128i *in)
{

  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<18)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,18);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 18-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 18-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 18-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 18-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 18-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 18-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 18-10), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,10);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 18-14), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,14);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,18);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 18-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 18-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 18-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 18-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 18-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 18-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 18-10), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,10);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 18-14), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,14);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;



}

static void
iunpackscan19(__m128i * initOffset, const __m128i *in)
{

  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<19)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,19);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 19-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,25);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 19-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,31);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 19-18), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,18);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 19-5), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,5);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 19-11), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,11);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 19-17), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,17);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 19-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,23);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 19-10), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,10);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,29);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 19-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 19-3), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,3);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 19-9), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,9);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 19-15), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,15);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 19-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,21);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 19-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,27);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 19-14), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,14);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 19-1), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,1);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 19-7), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,7);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 19-13), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,13);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;



}

static void
iunpackscan20(__m128i * initOffset, const __m128i *in)
{

  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<20)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 20-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 20-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 20-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 20-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 20-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 20-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 20-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 20-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 20-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 20-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 20-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 20-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 20-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 20-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 20-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 20-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;




}

static void
iunpackscan21(__m128i * initOffset, const __m128i *in)
{

  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<21)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,21);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 21-10), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,10);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,31);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 21-20), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 21-9), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,9);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 21-19), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,19);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 21-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,29);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 21-18), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,18);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 21-7), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,7);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 21-17), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,17);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 21-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,27);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 21-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 21-5), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,5);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 21-15), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,15);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 21-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,25);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 21-14), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,14);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 21-3), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,3);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 21-13), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,13);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 21-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,23);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 21-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 21-1), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,1);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 21-11), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,11);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;




}

static void
iunpackscan22(__m128i * initOffset, const __m128i *in)
{

  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<22)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 22-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 22-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 22-14), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,14);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 22-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 22-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 22-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 22-18), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,18);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 22-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 22-20), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 22-10), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,10);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 22-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 22-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 22-14), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,14);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 22-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 22-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 22-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 22-18), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,18);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 22-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 22-20), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 22-10), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,10);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;




}

static void
iunpackscan23(__m128i * initOffset, const __m128i *in)
{

  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<23)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,23);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 23-14), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,14);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 23-5), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,5);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 23-19), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,19);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 23-10), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,10);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 23-1), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,1);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 23-15), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,15);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 23-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,29);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 23-20), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 23-11), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,11);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 23-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,25);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 23-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 23-7), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,7);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 23-21), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,21);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 23-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 23-3), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,3);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 23-17), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,17);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 23-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,31);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 23-22), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 23-13), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,13);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 23-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,27);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 23-18), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,18);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 23-9), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,9);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;




}

static void
iunpackscan24(__m128i * initOffset, const __m128i *in)
{

  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<24)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 24-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 24-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 24-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 24-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 24-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 24-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 24-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 24-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 24-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 24-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 24-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 24-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 24-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 24-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 24-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 24-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;




}

static void
iunpackscan25(__m128i * initOffset, const __m128i *in)
{

  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<25)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,25);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-18), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,18);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-11), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,11);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,29);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-22), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-15), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,15);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-1), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,1);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-19), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,19);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-5), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,5);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-23), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,23);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-9), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,9);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,27);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-20), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-13), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,13);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,6);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,31);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-24), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-17), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,17);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-10), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,10);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-3), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,3);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-21), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,21);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-14), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,14);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 25-7), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,7);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;




}

static void
iunpackscan26(__m128i * initOffset, const __m128i *in)
{

  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<26)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-20), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-14), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,14);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-22), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-10), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,10);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-24), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-18), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,18);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,6);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-20), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-14), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,14);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-22), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-10), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,10);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-24), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-18), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,18);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 26-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,6);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;




}

static void
iunpackscan27(__m128i * initOffset, const __m128i *in)
{

  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<27)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,27);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-22), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-17), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,17);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-7), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,7);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,29);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-24), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-19), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,19);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-14), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,14);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-9), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,9);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,31);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-26), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-21), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,21);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-11), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,11);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,6);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-1), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,1);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-23), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,23);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-18), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,18);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-13), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,13);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-3), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,3);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-25), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,25);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-20), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-15), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,15);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-10), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,10);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 27-5), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,5);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;




}

static void
iunpackscan28(__m128i * initOffset, const __m128i *in)
{

  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<28)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-24), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-20), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-24), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-20), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-24), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-20), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-24), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-20), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 28-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;




}

static void
iunpackscan29(__m128i * initOffset, const __m128i *in)
{

  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<29)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,29);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-26), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-23), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,23);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-20), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-17), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,17);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-14), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,14);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-11), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,11);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-5), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,5);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,2);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,31);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-28), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-25), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,25);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-22), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-19), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,19);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-13), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,13);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-10), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,10);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-7), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,7);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-1), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,1);
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-27), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,27);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-24), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-21), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,21);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-18), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,18);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-15), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,15);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-9), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,9);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,6);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 29-3), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,3);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;




}

static void
iunpackscan30(__m128i * initOffset, const __m128i *in)
{

  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<30)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-28), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-26), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-24), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-22), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-20), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-18), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,18);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-14), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,14);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-10), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,10);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,6);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,2);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-28), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-26), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-24), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-22), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-20), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-18), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,18);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-14), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,14);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-10), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,10);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,6);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 30-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,2);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;




}

static void
iunpackscan31(__m128i * initOffset, const __m128i *in)
{

  __m128i InReg = _mm_loadu_si128(in);
  __m128i out;
  __m128i tmp;
  __m128i mask = _mm_set1_epi32((1U<<31)-1);

  tmp = InReg;
  out = _mm_and_si128(tmp, mask);
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,31);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-30), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,30);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-29), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,29);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-28), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,28);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-27), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,27);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-26), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,26);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-25), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,25);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-24), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,24);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-23), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,23);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-22), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,22);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-21), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,21);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-20), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,20);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-19), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,19);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-18), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,18);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-17), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,17);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-16), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,16);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-15), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,15);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-14), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,14);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-13), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,13);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-12), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,12);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-11), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,11);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-10), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,10);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-9), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,9);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-8), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,8);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-7), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,7);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-6), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,6);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-5), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,5);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-4), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,4);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-3), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,3);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-2), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,2);
  out = tmp;
  ++in;  InReg = _mm_loadu_si128(in);
  out = _mm_or_si128(out, _mm_and_si128(_mm_slli_epi32(InReg, 31-1), mask));

  PrefixSum(out, out, *initOffset);
  *initOffset = out;


  tmp = _mm_srli_epi32(InReg,1);
  out = tmp;
  PrefixSum(out, out, *initOffset);
  *initOffset = out;


}

static void
iunpackscan32(__m128i * initOffset , const __m128i *in)
{
	*initOffset = _mm_load_si128(in + 31);
}


void
simdscand1(__m128i * initOffset, const __m128i *in, uint32_t bit)
{
   switch (bit) {
    case 0: return; break;

    case 1: iunpackscan1(initOffset, in); break;

    case 2: iunpackscan2(initOffset, in); break;

    case 3: iunpackscan3(initOffset, in); break;

    case 4: iunpackscan4(initOffset, in); break;

    case 5: iunpackscan5(initOffset, in); break;

    case 6: iunpackscan6(initOffset, in); break;

    case 7: iunpackscan7(initOffset, in); break;

    case 8: iunpackscan8(initOffset, in); break;

    case 9: iunpackscan9(initOffset, in); break;

    case 10: iunpackscan10(initOffset, in); break;

    case 11: iunpackscan11(initOffset, in); break;

    case 12: iunpackscan12(initOffset, in); break;

    case 13: iunpackscan13(initOffset, in); break;

    case 14: iunpackscan14(initOffset, in); break;

    case 15: iunpackscan15(initOffset, in); break;

    case 16: iunpackscan16(initOffset, in); break;

    case 17: iunpackscan17(initOffset, in); break;

    case 18: iunpackscan18(initOffset, in); break;

    case 19: iunpackscan19(initOffset, in); break;

    case 20: iunpackscan20(initOffset, in); break;

    case 21: iunpackscan21(initOffset, in); break;

    case 22: iunpackscan22(initOffset, in); break;

    case 23: iunpackscan23(initOffset, in); break;

    case 24: iunpackscan24(initOffset, in); break;

    case 25: iunpackscan25(initOffset, in); break;

    case 26: iunpackscan26(initOffset, in); break;

    case 27: iunpackscan27(initOffset, in); break;

    case 28: iunpackscan28(initOffset, in); break;

    case 29: iunpackscan29(initOffset, in); break;

    case 30: iunpackscan30(initOffset, in); break;

    case 31: iunpackscan31(initOffset, in); break;

    case 32: iunpackscan32(initOffset, in); break;

    default: break;
   }

   return ;
}

#endif
