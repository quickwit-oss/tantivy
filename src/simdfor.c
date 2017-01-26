/**
 * This code is released under a BSD License.
 */

#include "simdfor.h"



static __m128i  iunpackFOR0(__m128i initOffset, const __m128i *   _in , uint32_t *    _out) {
    __m128i       *out = (__m128i*)(_out);
    int i;
    (void) _in;
    for (i = 0; i < 8; ++i) {
        _mm_store_si128(out++, initOffset);
    	_mm_store_si128(out++, initOffset);
        _mm_store_si128(out++, initOffset);
        _mm_store_si128(out++, initOffset);
    }

    return initOffset;
}




static void ipackFOR0(__m128i initOffset , const uint32_t *   _in , __m128i *  out  ) {
    (void) initOffset;
    (void) _in;
    (void) out;
}


static void ipackFOR1(__m128i  initOffset, const uint32_t *   _in, __m128i *   out) {
    const __m128i       *in = (const __m128i*)(_in);
    __m128i    OutReg;


    __m128i CurrIn = _mm_load_si128(in);
    __m128i InReg = _mm_sub_epi32(CurrIn, initOffset);
    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 1));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 2));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 3));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 5));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 6));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 7));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 9));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 10));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 11));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 13));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 14));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 15));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 17));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 18));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 19));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 21));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 22));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 23));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 25));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 26));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 27));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 29));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 30));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 31));
    _mm_store_si128(out, OutReg);


}



static void ipackFOR2(__m128i  initOffset, const uint32_t *   _in, __m128i *   out) {
    const __m128i       *in = (const __m128i*)(_in);
    __m128i    OutReg;


    __m128i CurrIn = _mm_load_si128(in);
    __m128i InReg = _mm_sub_epi32(CurrIn, initOffset);
    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 2));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 6));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 10));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 14));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 18));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 22));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 26));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 30));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 2));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 6));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 10));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 14));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 18));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 22));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 26));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 30));
    _mm_store_si128(out, OutReg);


}



static void ipackFOR3(__m128i  initOffset, const uint32_t *   _in, __m128i *   out) {
    const __m128i       *in = (const __m128i*)(_in);
    __m128i    OutReg;


    __m128i CurrIn = _mm_load_si128(in);
    __m128i InReg = _mm_sub_epi32(CurrIn, initOffset);
    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 3));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 6));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 9));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 15));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 18));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 21));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 27));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 30));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 3 - 1);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 1));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 7));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 10));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 13));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 19));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 22));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 25));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 31));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 3 - 2);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 2));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 5));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 11));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 14));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 17));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 23));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 26));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 29));
    _mm_store_si128(out, OutReg);


}



static void ipackFOR4(__m128i  initOffset, const uint32_t *   _in, __m128i *   out) {
    const __m128i       *in = (const __m128i*)(_in);
    __m128i    OutReg;


    __m128i CurrIn = _mm_load_si128(in);
    __m128i InReg = _mm_sub_epi32(CurrIn, initOffset);
    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    _mm_store_si128(out, OutReg);


}



static void ipackFOR5(__m128i  initOffset, const uint32_t *   _in, __m128i *   out) {
    const __m128i       *in = (const __m128i*)(_in);
    __m128i    OutReg;


    __m128i CurrIn = _mm_load_si128(in);
    __m128i InReg = _mm_sub_epi32(CurrIn, initOffset);
    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 5));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 10));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 15));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 25));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 30));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 5 - 3);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 3));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 13));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 18));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 23));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 5 - 1);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 1));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 6));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 11));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 21));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 26));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 31));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 5 - 4);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 9));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 14));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 19));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 29));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 5 - 2);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 2));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 7));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 17));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 22));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 27));
    _mm_store_si128(out, OutReg);


}



static void ipackFOR6(__m128i  initOffset, const uint32_t *   _in, __m128i *   out) {
    const __m128i       *in = (const __m128i*)(_in);
    __m128i    OutReg;


    __m128i CurrIn = _mm_load_si128(in);
    __m128i InReg = _mm_sub_epi32(CurrIn, initOffset);
    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 6));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 18));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 30));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 6 - 4);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 10));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 22));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 6 - 2);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 2));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 14));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 26));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 6));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 18));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 30));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 6 - 4);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 10));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 22));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 6 - 2);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 2));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 14));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 26));
    _mm_store_si128(out, OutReg);


}



static void ipackFOR7(__m128i  initOffset, const uint32_t *   _in, __m128i *   out) {
    const __m128i       *in = (const __m128i*)(_in);
    __m128i    OutReg;


    __m128i CurrIn = _mm_load_si128(in);
    __m128i InReg = _mm_sub_epi32(CurrIn, initOffset);
    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 7));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 14));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 21));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 7 - 3);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 3));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 10));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 17));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 31));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 7 - 6);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 6));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 13));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 27));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 7 - 2);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 2));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 9));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 23));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 30));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 7 - 5);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 5));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 19));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 26));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 7 - 1);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 1));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 15));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 22));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 29));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 7 - 4);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 11));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 18));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 25));
    _mm_store_si128(out, OutReg);


}



static void ipackFOR8(__m128i  initOffset, const uint32_t *   _in, __m128i *   out) {
    const __m128i       *in = (const __m128i*)(_in);
    __m128i    OutReg;


    __m128i CurrIn = _mm_load_si128(in);
    __m128i InReg = _mm_sub_epi32(CurrIn, initOffset);
    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);


}



static void ipackFOR9(__m128i  initOffset, const uint32_t *   _in, __m128i *   out) {
    const __m128i       *in = (const __m128i*)(_in);
    __m128i    OutReg;


    __m128i CurrIn = _mm_load_si128(in);
    __m128i InReg = _mm_sub_epi32(CurrIn, initOffset);
    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 9));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 18));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 27));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 9 - 4);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 13));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 22));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 31));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 9 - 8);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 17));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 26));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 9 - 3);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 3));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 21));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 30));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 9 - 7);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 7));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 25));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 9 - 2);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 2));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 11));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 29));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 9 - 6);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 6));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 15));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 9 - 1);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 1));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 10));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 19));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 9 - 5);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 5));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 14));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 23));
    _mm_store_si128(out, OutReg);


}



static void ipackFOR10(__m128i  initOffset, const uint32_t *   _in, __m128i *   out) {
    const __m128i       *in = (const __m128i*)(_in);
    __m128i    OutReg;


    __m128i CurrIn = _mm_load_si128(in);
    __m128i InReg = _mm_sub_epi32(CurrIn, initOffset);
    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 10));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 30));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 10 - 8);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 18));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 10 - 6);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 6));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 26));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 10 - 4);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 14));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 10 - 2);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 2));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 22));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 10));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 30));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 10 - 8);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 18));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 10 - 6);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 6));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 26));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 10 - 4);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 14));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 10 - 2);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 2));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 22));
    _mm_store_si128(out, OutReg);


}



static void ipackFOR11(__m128i  initOffset, const uint32_t *   _in, __m128i *   out) {
    const __m128i       *in = (const __m128i*)(_in);
    __m128i    OutReg;


    __m128i CurrIn = _mm_load_si128(in);
    __m128i InReg = _mm_sub_epi32(CurrIn, initOffset);
    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 11));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 22));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 11 - 1);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 1));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 23));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 11 - 2);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 2));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 13));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 11 - 3);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 3));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 14));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 25));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 11 - 4);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 15));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 26));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 11 - 5);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 5));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 27));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 11 - 6);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 6));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 17));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 11 - 7);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 7));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 18));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 29));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 11 - 8);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 19));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 30));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 11 - 9);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 9));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 31));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 11 - 10);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 10));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 21));
    _mm_store_si128(out, OutReg);


}



static void ipackFOR12(__m128i  initOffset, const uint32_t *   _in, __m128i *   out) {
    const __m128i       *in = (const __m128i*)(_in);
    __m128i    OutReg;


    __m128i CurrIn = _mm_load_si128(in);
    __m128i InReg = _mm_sub_epi32(CurrIn, initOffset);
    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 12 - 4);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 12 - 8);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 12 - 4);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 12 - 8);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 12 - 4);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 12 - 8);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 12 - 4);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 12 - 8);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    _mm_store_si128(out, OutReg);


}



static void ipackFOR13(__m128i  initOffset, const uint32_t *   _in, __m128i *   out) {
    const __m128i       *in = (const __m128i*)(_in);
    __m128i    OutReg;


    __m128i CurrIn = _mm_load_si128(in);
    __m128i InReg = _mm_sub_epi32(CurrIn, initOffset);
    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 13));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 26));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 13 - 7);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 7));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 13 - 1);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 1));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 14));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 27));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 13 - 8);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 21));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 13 - 2);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 2));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 15));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 13 - 9);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 9));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 22));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 13 - 3);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 3));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 29));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 13 - 10);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 10));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 23));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 13 - 4);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 17));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 30));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 13 - 11);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 11));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 13 - 5);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 5));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 18));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 31));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 13 - 12);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 25));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 13 - 6);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 6));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 19));
    _mm_store_si128(out, OutReg);


}



static void ipackFOR14(__m128i  initOffset, const uint32_t *   _in, __m128i *   out) {
    const __m128i       *in = (const __m128i*)(_in);
    __m128i    OutReg;


    __m128i CurrIn = _mm_load_si128(in);
    __m128i InReg = _mm_sub_epi32(CurrIn, initOffset);
    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 14));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 14 - 10);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 10));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 14 - 6);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 6));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 14 - 2);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 2));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 30));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 14 - 12);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 26));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 14 - 8);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 22));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 14 - 4);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 18));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 14));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 14 - 10);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 10));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 14 - 6);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 6));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 14 - 2);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 2));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 30));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 14 - 12);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 26));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 14 - 8);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 22));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 14 - 4);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 18));
    _mm_store_si128(out, OutReg);


}



static void ipackFOR15(__m128i  initOffset, const uint32_t *   _in, __m128i *   out) {
    const __m128i       *in = (const __m128i*)(_in);
    __m128i    OutReg;


    __m128i CurrIn = _mm_load_si128(in);
    __m128i InReg = _mm_sub_epi32(CurrIn, initOffset);
    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 15));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 30));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 15 - 13);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 13));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 15 - 11);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 11));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 26));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 15 - 9);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 9));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 15 - 7);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 7));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 22));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 15 - 5);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 5));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 15 - 3);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 3));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 18));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 15 - 1);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 1));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 31));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 15 - 14);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 14));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 29));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 15 - 12);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 27));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 15 - 10);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 10));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 25));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 15 - 8);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 23));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 15 - 6);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 6));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 21));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 15 - 4);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 19));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 15 - 2);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 2));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 17));
    _mm_store_si128(out, OutReg);


}



static void ipackFOR16(__m128i  initOffset, const uint32_t *   _in, __m128i *   out) {
    const __m128i       *in = (const __m128i*)(_in);
    __m128i    OutReg;


    __m128i CurrIn = _mm_load_si128(in);
    __m128i InReg = _mm_sub_epi32(CurrIn, initOffset);
    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);


}



static void ipackFOR17(__m128i  initOffset, const uint32_t *   _in, __m128i *   out) {
    const __m128i       *in = (const __m128i*)(_in);
    __m128i    OutReg;


    __m128i CurrIn = _mm_load_si128(in);
    __m128i InReg = _mm_sub_epi32(CurrIn, initOffset);
    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 17));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 17 - 2);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 2));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 19));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 17 - 4);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 21));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 17 - 6);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 6));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 23));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 17 - 8);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 25));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 17 - 10);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 10));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 27));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 17 - 12);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 29));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 17 - 14);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 14));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 31));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 17 - 16);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 17 - 1);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 1));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 18));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 17 - 3);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 3));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 17 - 5);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 5));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 22));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 17 - 7);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 7));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 17 - 9);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 9));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 26));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 17 - 11);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 11));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 17 - 13);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 13));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 30));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 17 - 15);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 15));
    _mm_store_si128(out, OutReg);


}



static void ipackFOR18(__m128i  initOffset, const uint32_t *   _in, __m128i *   out) {
    const __m128i       *in = (const __m128i*)(_in);
    __m128i    OutReg;


    __m128i CurrIn = _mm_load_si128(in);
    __m128i InReg = _mm_sub_epi32(CurrIn, initOffset);
    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 18));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 18 - 4);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 22));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 18 - 8);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 26));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 18 - 12);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 30));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 18 - 16);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 18 - 2);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 2));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 18 - 6);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 6));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 18 - 10);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 10));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 18 - 14);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 14));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 18));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 18 - 4);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 22));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 18 - 8);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 26));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 18 - 12);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 30));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 18 - 16);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 18 - 2);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 2));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 18 - 6);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 6));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 18 - 10);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 10));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 18 - 14);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 14));
    _mm_store_si128(out, OutReg);


}



static void ipackFOR19(__m128i  initOffset, const uint32_t *   _in, __m128i *   out) {
    const __m128i       *in = (const __m128i*)(_in);
    __m128i    OutReg;


    __m128i CurrIn = _mm_load_si128(in);
    __m128i InReg = _mm_sub_epi32(CurrIn, initOffset);
    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 19));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 19 - 6);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 6));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 25));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 19 - 12);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 31));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 19 - 18);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 18));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 19 - 5);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 5));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 19 - 11);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 11));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 30));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 19 - 17);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 17));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 19 - 4);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 23));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 19 - 10);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 10));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 29));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 19 - 16);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 19 - 3);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 3));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 22));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 19 - 9);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 9));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 19 - 15);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 15));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 19 - 2);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 2));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 21));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 19 - 8);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 27));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 19 - 14);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 14));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 19 - 1);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 1));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 19 - 7);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 7));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 26));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 19 - 13);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 13));
    _mm_store_si128(out, OutReg);


}



static void ipackFOR20(__m128i  initOffset, const uint32_t *   _in, __m128i *   out) {
    const __m128i       *in = (const __m128i*)(_in);
    __m128i    OutReg;


    __m128i CurrIn = _mm_load_si128(in);
    __m128i InReg = _mm_sub_epi32(CurrIn, initOffset);
    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 20 - 8);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 20 - 16);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 20 - 4);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 20 - 12);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 20 - 8);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 20 - 16);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 20 - 4);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 20 - 12);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 20 - 8);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 20 - 16);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 20 - 4);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 20 - 12);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 20 - 8);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 20 - 16);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 20 - 4);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 20 - 12);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    _mm_store_si128(out, OutReg);


}



static void ipackFOR21(__m128i  initOffset, const uint32_t *   _in, __m128i *   out) {
    const __m128i       *in = (const __m128i*)(_in);
    __m128i    OutReg;


    __m128i CurrIn = _mm_load_si128(in);
    __m128i InReg = _mm_sub_epi32(CurrIn, initOffset);
    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 21));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 21 - 10);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 10));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 31));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 21 - 20);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 21 - 9);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 9));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 30));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 21 - 19);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 19));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 21 - 8);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 29));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 21 - 18);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 18));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 21 - 7);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 7));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 21 - 17);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 17));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 21 - 6);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 6));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 27));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 21 - 16);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 21 - 5);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 5));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 26));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 21 - 15);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 15));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 21 - 4);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 25));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 21 - 14);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 14));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 21 - 3);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 3));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 21 - 13);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 13));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 21 - 2);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 2));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 23));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 21 - 12);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 21 - 1);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 1));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 22));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 21 - 11);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 11));
    _mm_store_si128(out, OutReg);


}



static void ipackFOR22(__m128i  initOffset, const uint32_t *   _in, __m128i *   out) {
    const __m128i       *in = (const __m128i*)(_in);
    __m128i    OutReg;


    __m128i CurrIn = _mm_load_si128(in);
    __m128i InReg = _mm_sub_epi32(CurrIn, initOffset);
    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 22));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 22 - 12);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 22 - 2);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 2));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 22 - 14);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 14));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 22 - 4);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 26));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 22 - 16);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 22 - 6);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 6));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 22 - 18);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 18));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 22 - 8);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 30));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 22 - 20);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 22 - 10);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 10));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 22));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 22 - 12);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 22 - 2);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 2));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 22 - 14);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 14));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 22 - 4);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 26));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 22 - 16);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 22 - 6);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 6));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 22 - 18);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 18));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 22 - 8);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 30));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 22 - 20);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 22 - 10);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 10));
    _mm_store_si128(out, OutReg);


}



static void ipackFOR23(__m128i  initOffset, const uint32_t *   _in, __m128i *   out) {
    const __m128i       *in = (const __m128i*)(_in);
    __m128i    OutReg;


    __m128i CurrIn = _mm_load_si128(in);
    __m128i InReg = _mm_sub_epi32(CurrIn, initOffset);
    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 23));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 23 - 14);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 14));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 23 - 5);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 5));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 23 - 19);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 19));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 23 - 10);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 10));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 23 - 1);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 1));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 23 - 15);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 15));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 23 - 6);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 6));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 29));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 23 - 20);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 23 - 11);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 11));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 23 - 2);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 2));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 25));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 23 - 16);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 23 - 7);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 7));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 30));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 23 - 21);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 21));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 23 - 12);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 23 - 3);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 3));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 26));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 23 - 17);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 17));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 23 - 8);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 31));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 23 - 22);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 22));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 23 - 13);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 13));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 23 - 4);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 27));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 23 - 18);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 18));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 23 - 9);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 9));
    _mm_store_si128(out, OutReg);


}



static void ipackFOR24(__m128i  initOffset, const uint32_t *   _in, __m128i *   out) {
    const __m128i       *in = (const __m128i*)(_in);
    __m128i    OutReg;


    __m128i CurrIn = _mm_load_si128(in);
    __m128i InReg = _mm_sub_epi32(CurrIn, initOffset);
    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 24 - 16);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 24 - 8);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 24 - 16);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 24 - 8);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 24 - 16);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 24 - 8);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 24 - 16);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 24 - 8);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 24 - 16);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 24 - 8);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 24 - 16);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 24 - 8);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 24 - 16);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 24 - 8);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 24 - 16);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 24 - 8);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    _mm_store_si128(out, OutReg);


}



static void ipackFOR25(__m128i  initOffset, const uint32_t *   _in, __m128i *   out) {
    const __m128i       *in = (const __m128i*)(_in);
    __m128i    OutReg;


    __m128i CurrIn = _mm_load_si128(in);
    __m128i InReg = _mm_sub_epi32(CurrIn, initOffset);
    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 25));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 25 - 18);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 18));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 25 - 11);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 11));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 25 - 4);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 29));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 25 - 22);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 22));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 25 - 15);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 15));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 25 - 8);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 25 - 1);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 1));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 26));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 25 - 19);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 19));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 25 - 12);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 25 - 5);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 5));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 30));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 25 - 23);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 23));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 25 - 16);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 25 - 9);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 9));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 25 - 2);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 2));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 27));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 25 - 20);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 25 - 13);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 13));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 25 - 6);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 6));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 31));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 25 - 24);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 25 - 17);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 17));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 25 - 10);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 10));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 25 - 3);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 3));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 25 - 21);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 21));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 25 - 14);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 14));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 25 - 7);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 7));
    _mm_store_si128(out, OutReg);


}



static void ipackFOR26(__m128i  initOffset, const uint32_t *   _in, __m128i *   out) {
    const __m128i       *in = (const __m128i*)(_in);
    __m128i    OutReg;


    __m128i CurrIn = _mm_load_si128(in);
    __m128i InReg = _mm_sub_epi32(CurrIn, initOffset);
    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 26));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 26 - 20);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 26 - 14);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 14));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 26 - 8);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 26 - 2);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 2));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 26 - 22);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 22));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 26 - 16);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 26 - 10);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 10));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 26 - 4);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 30));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 26 - 24);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 26 - 18);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 18));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 26 - 12);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 26 - 6);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 6));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 26));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 26 - 20);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 26 - 14);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 14));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 26 - 8);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 26 - 2);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 2));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 26 - 22);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 22));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 26 - 16);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 26 - 10);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 10));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 26 - 4);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 30));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 26 - 24);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 26 - 18);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 18));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 26 - 12);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 26 - 6);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 6));
    _mm_store_si128(out, OutReg);


}



static void ipackFOR27(__m128i  initOffset, const uint32_t *   _in, __m128i *   out) {
    const __m128i       *in = (const __m128i*)(_in);
    __m128i    OutReg;


    __m128i CurrIn = _mm_load_si128(in);
    __m128i InReg = _mm_sub_epi32(CurrIn, initOffset);
    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 27));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 27 - 22);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 22));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 27 - 17);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 17));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 27 - 12);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 27 - 7);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 7));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 27 - 2);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 2));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 29));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 27 - 24);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 27 - 19);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 19));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 27 - 14);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 14));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 27 - 9);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 9));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 27 - 4);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 31));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 27 - 26);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 26));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 27 - 21);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 21));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 27 - 16);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 27 - 11);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 11));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 27 - 6);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 6));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 27 - 1);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 1));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 27 - 23);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 23));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 27 - 18);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 18));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 27 - 13);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 13));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 27 - 8);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 27 - 3);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 3));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 30));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 27 - 25);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 25));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 27 - 20);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 27 - 15);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 15));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 27 - 10);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 10));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 27 - 5);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 5));
    _mm_store_si128(out, OutReg);


}



static void ipackFOR28(__m128i  initOffset, const uint32_t *   _in, __m128i *   out) {
    const __m128i       *in = (const __m128i*)(_in);
    __m128i    OutReg;


    __m128i CurrIn = _mm_load_si128(in);
    __m128i InReg = _mm_sub_epi32(CurrIn, initOffset);
    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 28 - 24);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 28 - 20);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 28 - 16);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 28 - 12);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 28 - 8);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 28 - 4);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 28 - 24);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 28 - 20);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 28 - 16);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 28 - 12);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 28 - 8);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 28 - 4);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 28 - 24);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 28 - 20);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 28 - 16);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 28 - 12);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 28 - 8);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 28 - 4);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 28 - 24);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 28 - 20);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 28 - 16);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 28 - 12);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 28 - 8);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 28 - 4);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    _mm_store_si128(out, OutReg);


}



static void ipackFOR29(__m128i  initOffset, const uint32_t *   _in, __m128i *   out) {
    const __m128i       *in = (const __m128i*)(_in);
    __m128i    OutReg;


    __m128i CurrIn = _mm_load_si128(in);
    __m128i InReg = _mm_sub_epi32(CurrIn, initOffset);
    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 29));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 29 - 26);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 26));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 29 - 23);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 23));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 29 - 20);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 29 - 17);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 17));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 29 - 14);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 14));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 29 - 11);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 11));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 29 - 8);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 29 - 5);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 5));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 29 - 2);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 2));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 31));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 29 - 28);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 29 - 25);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 25));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 29 - 22);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 22));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 29 - 19);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 19));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 29 - 16);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 29 - 13);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 13));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 29 - 10);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 10));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 29 - 7);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 7));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 29 - 4);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 29 - 1);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 1));
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 30));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 29 - 27);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 27));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 29 - 24);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 29 - 21);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 21));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 29 - 18);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 18));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 29 - 15);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 15));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 29 - 12);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 29 - 9);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 9));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 29 - 6);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 6));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 29 - 3);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 3));
    _mm_store_si128(out, OutReg);


}



static void ipackFOR30(__m128i  initOffset, const uint32_t *   _in, __m128i *   out) {
    const __m128i       *in = (const __m128i*)(_in);
    __m128i    OutReg;


    __m128i CurrIn = _mm_load_si128(in);
    __m128i InReg = _mm_sub_epi32(CurrIn, initOffset);
    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 30));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 30 - 28);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 30 - 26);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 26));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 30 - 24);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 30 - 22);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 22));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 30 - 20);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 30 - 18);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 18));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 30 - 16);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 30 - 14);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 14));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 30 - 12);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 30 - 10);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 10));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 30 - 8);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 30 - 6);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 6));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 30 - 4);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 30 - 2);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 2));
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 30));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 30 - 28);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 30 - 26);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 26));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 30 - 24);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 30 - 22);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 22));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 30 - 20);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 30 - 18);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 18));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 30 - 16);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 30 - 14);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 14));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 30 - 12);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 30 - 10);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 10));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 30 - 8);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 30 - 6);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 6));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 30 - 4);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 30 - 2);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 2));
    _mm_store_si128(out, OutReg);


}



static void ipackFOR31(__m128i  initOffset, const uint32_t *   _in, __m128i *   out) {
    const __m128i       *in = (const __m128i*)(_in);
    __m128i    OutReg;


    __m128i CurrIn = _mm_load_si128(in);
    __m128i InReg = _mm_sub_epi32(CurrIn, initOffset);
    OutReg = InReg;
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 31));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 31 - 30);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 30));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 31 - 29);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 29));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 31 - 28);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 28));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 31 - 27);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 27));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 31 - 26);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 26));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 31 - 25);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 25));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 31 - 24);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 24));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 31 - 23);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 23));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 31 - 22);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 22));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 31 - 21);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 21));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 31 - 20);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 20));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 31 - 19);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 19));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 31 - 18);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 18));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 31 - 17);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 17));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 31 - 16);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 16));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 31 - 15);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 15));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 31 - 14);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 14));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 31 - 13);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 13));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 31 - 12);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 12));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 31 - 11);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 11));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 31 - 10);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 10));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 31 - 9);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 9));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 31 - 8);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 8));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 31 - 7);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 7));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 31 - 6);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 6));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 31 - 5);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 5));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 31 - 4);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 4));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 31 - 3);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 3));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 31 - 2);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 2));
    _mm_store_si128(out, OutReg);

    ++out;
    OutReg = _mm_srli_epi32(InReg, 31 - 1);
    ++in;
    CurrIn = _mm_load_si128(in);
    InReg = _mm_sub_epi32(CurrIn, initOffset);

    OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, 1));
    _mm_store_si128(out, OutReg);


}



static void ipackFOR32(__m128i  initOffset, const uint32_t *   _in, __m128i *   out) {
    const __m128i       *in = (const __m128i*)(_in);
    __m128i    OutReg;

    __m128i InReg = _mm_load_si128(in);
    (void) initOffset;

    OutReg = InReg;
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    InReg = _mm_load_si128(in);

    OutReg = InReg;
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    InReg = _mm_load_si128(in);

    OutReg = InReg;
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    InReg = _mm_load_si128(in);

    OutReg = InReg;
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    InReg = _mm_load_si128(in);

    OutReg = InReg;
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    InReg = _mm_load_si128(in);

    OutReg = InReg;
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    InReg = _mm_load_si128(in);

    OutReg = InReg;
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    InReg = _mm_load_si128(in);

    OutReg = InReg;
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    InReg = _mm_load_si128(in);

    OutReg = InReg;
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    InReg = _mm_load_si128(in);

    OutReg = InReg;
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    InReg = _mm_load_si128(in);

    OutReg = InReg;
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    InReg = _mm_load_si128(in);

    OutReg = InReg;
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    InReg = _mm_load_si128(in);

    OutReg = InReg;
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    InReg = _mm_load_si128(in);

    OutReg = InReg;
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    InReg = _mm_load_si128(in);

    OutReg = InReg;
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    InReg = _mm_load_si128(in);

    OutReg = InReg;
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    InReg = _mm_load_si128(in);

    OutReg = InReg;
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    InReg = _mm_load_si128(in);

    OutReg = InReg;
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    InReg = _mm_load_si128(in);

    OutReg = InReg;
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    InReg = _mm_load_si128(in);

    OutReg = InReg;
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    InReg = _mm_load_si128(in);

    OutReg = InReg;
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    InReg = _mm_load_si128(in);

    OutReg = InReg;
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    InReg = _mm_load_si128(in);

    OutReg = InReg;
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    InReg = _mm_load_si128(in);

    OutReg = InReg;
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    InReg = _mm_load_si128(in);

    OutReg = InReg;
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    InReg = _mm_load_si128(in);

    OutReg = InReg;
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    InReg = _mm_load_si128(in);

    OutReg = InReg;
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    InReg = _mm_load_si128(in);

    OutReg = InReg;
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    InReg = _mm_load_si128(in);

    OutReg = InReg;
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    InReg = _mm_load_si128(in);

    OutReg = InReg;
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    InReg = _mm_load_si128(in);

    OutReg = InReg;
    _mm_store_si128(out, OutReg);

    ++out;
    ++in;
    InReg = _mm_load_si128(in);

    OutReg = InReg;
    _mm_store_si128(out, OutReg);


}




static __m128i iunpackFOR1(__m128i  initOffset, const  __m128i*   in, uint32_t *   _out) {

    __m128i*   out = (__m128i*)(_out);
    __m128i    InReg = _mm_load_si128(in);
    __m128i    OutReg;
    __m128i     tmp;
    const __m128i mask =  _mm_set1_epi32((1U<<1)-1);



    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,1);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,2);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,3);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,5);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,6);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,7);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,9);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,10);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,11);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,13);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,14);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,15);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,17);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,18);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,19);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,21);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,22);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,23);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,25);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,26);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,27);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,29);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,30);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,31);
    OutReg = tmp;
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);


    return initOffset;

}




static __m128i iunpackFOR2(__m128i  initOffset, const  __m128i*   in, uint32_t *   _out) {

    __m128i*   out = (__m128i*)(_out);
    __m128i    InReg = _mm_load_si128(in);
    __m128i    OutReg;
    __m128i     tmp;
    const __m128i mask =  _mm_set1_epi32((1U<<2)-1);



    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,2);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,6);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,10);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,14);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,18);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,22);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,26);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,30);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,2);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,6);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,10);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,14);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,18);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,22);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,26);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,30);
    OutReg = tmp;
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);


    return initOffset;

}




static __m128i iunpackFOR3(__m128i  initOffset, const  __m128i*   in, uint32_t *   _out) {

    __m128i*   out = (__m128i*)(_out);
    __m128i    InReg = _mm_load_si128(in);
    __m128i    OutReg;
    __m128i     tmp;
    const __m128i mask =  _mm_set1_epi32((1U<<3)-1);



    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,3);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,6);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,9);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,15);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,18);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,21);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,27);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,30);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 3-1), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,1);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,7);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,10);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,13);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,19);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,22);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,25);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,31);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 3-2), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,2);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,5);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,11);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,14);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,17);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,23);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,26);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,29);
    OutReg = tmp;
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);


    return initOffset;

}




static __m128i iunpackFOR4(__m128i  initOffset, const  __m128i*   in, uint32_t *   _out) {

    __m128i*   out = (__m128i*)(_out);
    __m128i    InReg = _mm_load_si128(in);
    __m128i    OutReg;
    __m128i     tmp;
    const __m128i mask =  _mm_set1_epi32((1U<<4)-1);



    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = tmp;
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);


    return initOffset;

}




static __m128i iunpackFOR5(__m128i  initOffset, const  __m128i*   in, uint32_t *   _out) {

    __m128i*   out = (__m128i*)(_out);
    __m128i    InReg = _mm_load_si128(in);
    __m128i    OutReg;
    __m128i     tmp;
    const __m128i mask =  _mm_set1_epi32((1U<<5)-1);



    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,5);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,10);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,15);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,25);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,30);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 5-3), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,3);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,13);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,18);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,23);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 5-1), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,1);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,6);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,11);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,21);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,26);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,31);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 5-4), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,9);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,14);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,19);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,29);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 5-2), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,2);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,7);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,17);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,22);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,27);
    OutReg = tmp;
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);


    return initOffset;

}




static __m128i iunpackFOR6(__m128i  initOffset, const  __m128i*   in, uint32_t *   _out) {

    __m128i*   out = (__m128i*)(_out);
    __m128i    InReg = _mm_load_si128(in);
    __m128i    OutReg;
    __m128i     tmp;
    const __m128i mask =  _mm_set1_epi32((1U<<6)-1);



    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,6);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,18);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,30);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 6-4), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,10);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,22);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 6-2), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,2);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,14);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,26);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,6);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,18);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,30);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 6-4), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,10);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,22);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 6-2), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,2);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,14);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,26);
    OutReg = tmp;
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);


    return initOffset;

}




static __m128i iunpackFOR7(__m128i  initOffset, const  __m128i*   in, uint32_t *   _out) {

    __m128i*   out = (__m128i*)(_out);
    __m128i    InReg = _mm_load_si128(in);
    __m128i    OutReg;
    __m128i     tmp;
    const __m128i mask =  _mm_set1_epi32((1U<<7)-1);



    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,7);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,14);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,21);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 7-3), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,3);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,10);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,17);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,31);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 7-6), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,6);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,13);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,27);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 7-2), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,2);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,9);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,23);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,30);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 7-5), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,5);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,19);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,26);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 7-1), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,1);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,15);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,22);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,29);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 7-4), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,11);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,18);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,25);
    OutReg = tmp;
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);


    return initOffset;

}




static __m128i iunpackFOR8(__m128i  initOffset, const  __m128i*   in, uint32_t *   _out) {

    __m128i*   out = (__m128i*)(_out);
    __m128i    InReg = _mm_load_si128(in);
    __m128i    OutReg;
    __m128i     tmp;
    const __m128i mask =  _mm_set1_epi32((1U<<8)-1);



    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);


    return initOffset;

}




static __m128i iunpackFOR9(__m128i  initOffset, const  __m128i*   in, uint32_t *   _out) {

    __m128i*   out = (__m128i*)(_out);
    __m128i    InReg = _mm_load_si128(in);
    __m128i    OutReg;
    __m128i     tmp;
    const __m128i mask =  _mm_set1_epi32((1U<<9)-1);



    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,9);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,18);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,27);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 9-4), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,13);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,22);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,31);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 9-8), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,17);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,26);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 9-3), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,3);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,21);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,30);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 9-7), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,7);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,25);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 9-2), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,2);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,11);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,29);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 9-6), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,6);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,15);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 9-1), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,1);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,10);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,19);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 9-5), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,5);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,14);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,23);
    OutReg = tmp;
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);


    return initOffset;

}




static __m128i iunpackFOR10(__m128i  initOffset, const  __m128i*   in, uint32_t *   _out) {

    __m128i*   out = (__m128i*)(_out);
    __m128i    InReg = _mm_load_si128(in);
    __m128i    OutReg;
    __m128i     tmp;
    const __m128i mask =  _mm_set1_epi32((1U<<10)-1);



    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,10);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,30);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 10-8), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,18);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 10-6), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,6);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,26);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 10-4), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,14);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 10-2), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,2);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,22);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,10);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,30);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 10-8), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,18);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 10-6), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,6);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,26);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 10-4), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,14);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 10-2), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,2);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,22);
    OutReg = tmp;
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);


    return initOffset;

}




static __m128i iunpackFOR11(__m128i  initOffset, const  __m128i*   in, uint32_t *   _out) {

    __m128i*   out = (__m128i*)(_out);
    __m128i    InReg = _mm_load_si128(in);
    __m128i    OutReg;
    __m128i     tmp;
    const __m128i mask =  _mm_set1_epi32((1U<<11)-1);



    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,11);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,22);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 11-1), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,1);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,23);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 11-2), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,2);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,13);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 11-3), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,3);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,14);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,25);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 11-4), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,15);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,26);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 11-5), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,5);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,27);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 11-6), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,6);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,17);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 11-7), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,7);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,18);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,29);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 11-8), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,19);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,30);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 11-9), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,9);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,31);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 11-10), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,10);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,21);
    OutReg = tmp;
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);


    return initOffset;

}




static __m128i iunpackFOR12(__m128i  initOffset, const  __m128i*   in, uint32_t *   _out) {

    __m128i*   out = (__m128i*)(_out);
    __m128i    InReg = _mm_load_si128(in);
    __m128i    OutReg;
    __m128i     tmp;
    const __m128i mask =  _mm_set1_epi32((1U<<12)-1);



    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 12-4), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 12-8), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 12-4), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 12-8), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 12-4), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 12-8), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 12-4), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 12-8), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = tmp;
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);


    return initOffset;

}




static __m128i iunpackFOR13(__m128i  initOffset, const  __m128i*   in, uint32_t *   _out) {

    __m128i*   out = (__m128i*)(_out);
    __m128i    InReg = _mm_load_si128(in);
    __m128i    OutReg;
    __m128i     tmp;
    const __m128i mask =  _mm_set1_epi32((1U<<13)-1);



    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,13);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,26);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 13-7), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,7);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 13-1), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,1);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,14);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,27);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 13-8), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,21);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 13-2), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,2);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,15);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 13-9), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,9);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,22);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 13-3), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,3);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,29);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 13-10), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,10);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,23);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 13-4), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,17);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,30);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 13-11), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,11);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 13-5), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,5);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,18);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,31);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 13-12), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,25);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 13-6), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,6);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,19);
    OutReg = tmp;
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);


    return initOffset;

}




static __m128i iunpackFOR14(__m128i  initOffset, const  __m128i*   in, uint32_t *   _out) {

    __m128i*   out = (__m128i*)(_out);
    __m128i    InReg = _mm_load_si128(in);
    __m128i    OutReg;
    __m128i     tmp;
    const __m128i mask =  _mm_set1_epi32((1U<<14)-1);



    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,14);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 14-10), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,10);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 14-6), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,6);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 14-2), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,2);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,30);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 14-12), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,26);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 14-8), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,22);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 14-4), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,18);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,14);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 14-10), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,10);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 14-6), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,6);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 14-2), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,2);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,30);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 14-12), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,26);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 14-8), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,22);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 14-4), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,18);
    OutReg = tmp;
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);


    return initOffset;

}




static __m128i iunpackFOR15(__m128i  initOffset, const  __m128i*   in, uint32_t *   _out) {

    __m128i*   out = (__m128i*)(_out);
    __m128i    InReg = _mm_load_si128(in);
    __m128i    OutReg;
    __m128i     tmp;
    const __m128i mask =  _mm_set1_epi32((1U<<15)-1);



    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,15);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,30);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 15-13), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,13);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 15-11), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,11);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,26);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 15-9), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,9);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 15-7), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,7);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,22);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 15-5), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,5);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 15-3), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,3);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,18);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 15-1), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,1);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,31);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 15-14), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,14);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,29);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 15-12), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,27);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 15-10), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,10);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,25);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 15-8), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,23);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 15-6), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,6);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,21);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 15-4), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,19);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 15-2), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,2);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,17);
    OutReg = tmp;
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);


    return initOffset;

}




static __m128i iunpackFOR16(__m128i  initOffset, const  __m128i*   in, uint32_t *   _out) {

    __m128i*   out = (__m128i*)(_out);
    __m128i    InReg = _mm_load_si128(in);
    __m128i    OutReg;
    __m128i     tmp;
    const __m128i mask =  _mm_set1_epi32((1U<<16)-1);



    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);


    return initOffset;

}




static __m128i iunpackFOR17(__m128i  initOffset, const  __m128i*   in, uint32_t *   _out) {

    __m128i*   out = (__m128i*)(_out);
    __m128i    InReg = _mm_load_si128(in);
    __m128i    OutReg;
    __m128i     tmp;
    const __m128i mask =  _mm_set1_epi32((1U<<17)-1);



    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,17);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 17-2), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,2);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,19);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 17-4), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,21);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 17-6), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,6);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,23);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 17-8), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,25);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 17-10), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,10);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,27);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 17-12), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,29);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 17-14), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,14);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,31);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 17-16), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 17-1), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,1);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,18);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 17-3), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,3);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 17-5), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,5);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,22);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 17-7), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,7);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 17-9), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,9);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,26);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 17-11), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,11);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 17-13), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,13);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,30);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 17-15), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,15);
    OutReg = tmp;
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);


    return initOffset;

}




static __m128i iunpackFOR18(__m128i  initOffset, const  __m128i*   in, uint32_t *   _out) {

    __m128i*   out = (__m128i*)(_out);
    __m128i    InReg = _mm_load_si128(in);
    __m128i    OutReg;
    __m128i     tmp;
    const __m128i mask =  _mm_set1_epi32((1U<<18)-1);



    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,18);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 18-4), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,22);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 18-8), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,26);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 18-12), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,30);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 18-16), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 18-2), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,2);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 18-6), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,6);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 18-10), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,10);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 18-14), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,14);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,18);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 18-4), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,22);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 18-8), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,26);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 18-12), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,30);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 18-16), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 18-2), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,2);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 18-6), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,6);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 18-10), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,10);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 18-14), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,14);
    OutReg = tmp;
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);


    return initOffset;

}




static __m128i iunpackFOR19(__m128i  initOffset, const  __m128i*   in, uint32_t *   _out) {

    __m128i*   out = (__m128i*)(_out);
    __m128i    InReg = _mm_load_si128(in);
    __m128i    OutReg;
    __m128i     tmp;
    const __m128i mask =  _mm_set1_epi32((1U<<19)-1);



    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,19);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 19-6), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,6);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,25);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 19-12), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,31);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 19-18), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,18);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 19-5), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,5);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 19-11), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,11);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,30);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 19-17), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,17);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 19-4), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,23);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 19-10), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,10);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,29);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 19-16), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 19-3), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,3);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,22);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 19-9), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,9);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 19-15), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,15);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 19-2), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,2);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,21);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 19-8), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,27);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 19-14), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,14);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 19-1), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,1);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 19-7), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,7);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,26);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 19-13), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,13);
    OutReg = tmp;
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);


    return initOffset;

}




static __m128i iunpackFOR20(__m128i  initOffset, const  __m128i*   in, uint32_t *   _out) {

    __m128i*   out = (__m128i*)(_out);
    __m128i    InReg = _mm_load_si128(in);
    __m128i    OutReg;
    __m128i     tmp;
    const __m128i mask =  _mm_set1_epi32((1U<<20)-1);



    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 20-8), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 20-16), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 20-4), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 20-12), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 20-8), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 20-16), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 20-4), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 20-12), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 20-8), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 20-16), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 20-4), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 20-12), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 20-8), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 20-16), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 20-4), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 20-12), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = tmp;
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);


    return initOffset;

}




static __m128i iunpackFOR21(__m128i  initOffset, const  __m128i*   in, uint32_t *   _out) {

    __m128i*   out = (__m128i*)(_out);
    __m128i    InReg = _mm_load_si128(in);
    __m128i    OutReg;
    __m128i     tmp;
    const __m128i mask =  _mm_set1_epi32((1U<<21)-1);



    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,21);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 21-10), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,10);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,31);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 21-20), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 21-9), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,9);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,30);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 21-19), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,19);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 21-8), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,29);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 21-18), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,18);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 21-7), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,7);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 21-17), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,17);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 21-6), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,6);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,27);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 21-16), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 21-5), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,5);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,26);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 21-15), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,15);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 21-4), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,25);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 21-14), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,14);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 21-3), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,3);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 21-13), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,13);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 21-2), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,2);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,23);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 21-12), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 21-1), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,1);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,22);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 21-11), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,11);
    OutReg = tmp;
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);


    return initOffset;

}




static __m128i iunpackFOR22(__m128i  initOffset, const  __m128i*   in, uint32_t *   _out) {

    __m128i*   out = (__m128i*)(_out);
    __m128i    InReg = _mm_load_si128(in);
    __m128i    OutReg;
    __m128i     tmp;
    const __m128i mask =  _mm_set1_epi32((1U<<22)-1);



    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,22);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 22-12), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 22-2), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,2);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 22-14), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,14);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 22-4), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,26);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 22-16), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 22-6), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,6);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 22-18), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,18);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 22-8), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,30);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 22-20), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 22-10), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,10);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,22);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 22-12), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 22-2), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,2);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 22-14), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,14);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 22-4), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,26);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 22-16), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 22-6), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,6);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 22-18), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,18);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 22-8), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,30);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 22-20), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 22-10), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,10);
    OutReg = tmp;
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);


    return initOffset;

}




static __m128i iunpackFOR23(__m128i  initOffset, const  __m128i*   in, uint32_t *   _out) {

    __m128i*   out = (__m128i*)(_out);
    __m128i    InReg = _mm_load_si128(in);
    __m128i    OutReg;
    __m128i     tmp;
    const __m128i mask =  _mm_set1_epi32((1U<<23)-1);



    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,23);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 23-14), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,14);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 23-5), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,5);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 23-19), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,19);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 23-10), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,10);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 23-1), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,1);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 23-15), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,15);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 23-6), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,6);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,29);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 23-20), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 23-11), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,11);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 23-2), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,2);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,25);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 23-16), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 23-7), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,7);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,30);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 23-21), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,21);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 23-12), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 23-3), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,3);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,26);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 23-17), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,17);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 23-8), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,31);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 23-22), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,22);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 23-13), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,13);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 23-4), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,27);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 23-18), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,18);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 23-9), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,9);
    OutReg = tmp;
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);


    return initOffset;

}




static __m128i iunpackFOR24(__m128i  initOffset, const  __m128i*   in, uint32_t *   _out) {

    __m128i*   out = (__m128i*)(_out);
    __m128i    InReg = _mm_load_si128(in);
    __m128i    OutReg;
    __m128i     tmp;
    const __m128i mask =  _mm_set1_epi32((1U<<24)-1);



    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 24-16), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 24-8), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 24-16), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 24-8), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 24-16), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 24-8), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 24-16), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 24-8), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 24-16), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 24-8), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 24-16), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 24-8), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 24-16), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 24-8), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 24-16), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 24-8), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = tmp;
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);


    return initOffset;

}




static __m128i iunpackFOR25(__m128i  initOffset, const  __m128i*   in, uint32_t *   _out) {

    __m128i*   out = (__m128i*)(_out);
    __m128i    InReg = _mm_load_si128(in);
    __m128i    OutReg;
    __m128i     tmp;
    const __m128i mask =  _mm_set1_epi32((1U<<25)-1);



    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,25);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 25-18), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,18);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 25-11), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,11);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 25-4), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,29);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 25-22), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,22);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 25-15), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,15);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 25-8), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 25-1), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,1);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,26);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 25-19), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,19);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 25-12), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 25-5), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,5);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,30);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 25-23), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,23);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 25-16), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 25-9), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,9);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 25-2), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,2);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,27);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 25-20), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 25-13), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,13);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 25-6), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,6);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,31);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 25-24), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 25-17), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,17);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 25-10), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,10);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 25-3), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,3);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 25-21), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,21);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 25-14), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,14);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 25-7), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,7);
    OutReg = tmp;
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);


    return initOffset;

}




static __m128i iunpackFOR26(__m128i  initOffset, const  __m128i*   in, uint32_t *   _out) {

    __m128i*   out = (__m128i*)(_out);
    __m128i    InReg = _mm_load_si128(in);
    __m128i    OutReg;
    __m128i     tmp;
    const __m128i mask =  _mm_set1_epi32((1U<<26)-1);



    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,26);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 26-20), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 26-14), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,14);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 26-8), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 26-2), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,2);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 26-22), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,22);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 26-16), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 26-10), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,10);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 26-4), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,30);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 26-24), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 26-18), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,18);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 26-12), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 26-6), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,6);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,26);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 26-20), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 26-14), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,14);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 26-8), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 26-2), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,2);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 26-22), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,22);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 26-16), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 26-10), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,10);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 26-4), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,30);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 26-24), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 26-18), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,18);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 26-12), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 26-6), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,6);
    OutReg = tmp;
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);


    return initOffset;

}




static __m128i iunpackFOR27(__m128i  initOffset, const  __m128i*   in, uint32_t *   _out) {

    __m128i*   out = (__m128i*)(_out);
    __m128i    InReg = _mm_load_si128(in);
    __m128i    OutReg;
    __m128i     tmp;
    const __m128i mask =  _mm_set1_epi32((1U<<27)-1);



    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,27);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 27-22), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,22);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 27-17), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,17);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 27-12), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 27-7), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,7);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 27-2), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,2);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,29);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 27-24), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 27-19), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,19);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 27-14), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,14);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 27-9), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,9);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 27-4), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,31);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 27-26), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,26);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 27-21), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,21);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 27-16), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 27-11), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,11);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 27-6), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,6);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 27-1), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,1);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 27-23), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,23);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 27-18), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,18);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 27-13), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,13);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 27-8), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 27-3), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,3);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,30);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 27-25), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,25);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 27-20), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 27-15), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,15);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 27-10), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,10);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 27-5), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,5);
    OutReg = tmp;
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);


    return initOffset;

}




static __m128i iunpackFOR28(__m128i  initOffset, const  __m128i*   in, uint32_t *   _out) {

    __m128i*   out = (__m128i*)(_out);
    __m128i    InReg = _mm_load_si128(in);
    __m128i    OutReg;
    __m128i     tmp;
    const __m128i mask =  _mm_set1_epi32((1U<<28)-1);



    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 28-24), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 28-20), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 28-16), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 28-12), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 28-8), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 28-4), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 28-24), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 28-20), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 28-16), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 28-12), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 28-8), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 28-4), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 28-24), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 28-20), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 28-16), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 28-12), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 28-8), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 28-4), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 28-24), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 28-20), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 28-16), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 28-12), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 28-8), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 28-4), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = tmp;
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);


    return initOffset;

}




static __m128i iunpackFOR29(__m128i  initOffset, const  __m128i*   in, uint32_t *   _out) {

    __m128i*   out = (__m128i*)(_out);
    __m128i    InReg = _mm_load_si128(in);
    __m128i    OutReg;
    __m128i     tmp;
    const __m128i mask =  _mm_set1_epi32((1U<<29)-1);



    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,29);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 29-26), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,26);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 29-23), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,23);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 29-20), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 29-17), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,17);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 29-14), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,14);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 29-11), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,11);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 29-8), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 29-5), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,5);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 29-2), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,2);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,31);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 29-28), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 29-25), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,25);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 29-22), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,22);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 29-19), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,19);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 29-16), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 29-13), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,13);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 29-10), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,10);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 29-7), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,7);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 29-4), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 29-1), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,1);
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,30);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 29-27), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,27);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 29-24), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 29-21), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,21);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 29-18), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,18);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 29-15), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,15);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 29-12), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 29-9), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,9);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 29-6), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,6);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 29-3), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,3);
    OutReg = tmp;
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);


    return initOffset;

}




static __m128i iunpackFOR30(__m128i  initOffset, const  __m128i*   in, uint32_t *   _out) {

    __m128i*   out = (__m128i*)(_out);
    __m128i    InReg = _mm_load_si128(in);
    __m128i    OutReg;
    __m128i     tmp;
    const __m128i mask =  _mm_set1_epi32((1U<<30)-1);



    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,30);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 30-28), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 30-26), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,26);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 30-24), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 30-22), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,22);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 30-20), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 30-18), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,18);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 30-16), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 30-14), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,14);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 30-12), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 30-10), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,10);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 30-8), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 30-6), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,6);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 30-4), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 30-2), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,2);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,30);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 30-28), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 30-26), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,26);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 30-24), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 30-22), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,22);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 30-20), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 30-18), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,18);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 30-16), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 30-14), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,14);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 30-12), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 30-10), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,10);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 30-8), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 30-6), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,6);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 30-4), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 30-2), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,2);
    OutReg = tmp;
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);


    return initOffset;

}




static __m128i iunpackFOR31(__m128i  initOffset, const  __m128i*   in, uint32_t *   _out) {

    __m128i*   out = (__m128i*)(_out);
    __m128i    InReg = _mm_load_si128(in);
    __m128i    OutReg;
    __m128i     tmp;
    const __m128i mask =  _mm_set1_epi32((1U<<31)-1);



    tmp = InReg;
    OutReg = _mm_and_si128(tmp, mask);
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,31);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 31-30), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,30);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 31-29), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,29);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 31-28), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,28);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 31-27), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,27);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 31-26), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,26);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 31-25), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,25);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 31-24), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,24);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 31-23), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,23);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 31-22), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,22);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 31-21), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,21);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 31-20), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,20);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 31-19), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,19);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 31-18), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,18);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 31-17), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,17);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 31-16), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,16);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 31-15), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,15);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 31-14), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,14);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 31-13), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,13);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 31-12), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,12);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 31-11), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,11);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 31-10), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,10);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 31-9), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,9);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 31-8), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,8);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 31-7), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,7);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 31-6), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,6);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 31-5), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,5);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 31-4), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,4);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 31-3), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,3);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 31-2), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,2);
    OutReg = tmp;
    ++in;    InReg = _mm_load_si128(in);
    OutReg = _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, 31-1), mask));

    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);

    tmp = _mm_srli_epi32(InReg,1);
    OutReg = tmp;
    OutReg = _mm_add_epi32(OutReg, initOffset);
    _mm_store_si128(out++, OutReg);


    return initOffset;

}



static __m128i iunpackFOR32(__m128i initvalue , const  __m128i*   in, uint32_t *    _out) {
	__m128i * mout = (__m128i *)_out;
	__m128i invec;
	size_t k;
	(void) initvalue;
	for(k = 0; k < 128/4; ++k) {
		invec =  _mm_load_si128(in++);
	    _mm_store_si128(mout++, invec);
	}
	return invec;
}





void simdpackFOR(uint32_t initvalue,  const uint32_t *   in, __m128i *    out, const uint32_t bit) {
    __m128i  initOffset = _mm_set1_epi32 (initvalue);
    switch(bit) {
        case 0: ipackFOR0(initOffset,in,out); break;

        case 1: ipackFOR1(initOffset,in,out); break;

        case 2: ipackFOR2(initOffset,in,out); break;

        case 3: ipackFOR3(initOffset,in,out); break;

        case 4: ipackFOR4(initOffset,in,out); break;

        case 5: ipackFOR5(initOffset,in,out); break;

        case 6: ipackFOR6(initOffset,in,out); break;

        case 7: ipackFOR7(initOffset,in,out); break;

        case 8: ipackFOR8(initOffset,in,out); break;

        case 9: ipackFOR9(initOffset,in,out); break;

        case 10: ipackFOR10(initOffset,in,out); break;

        case 11: ipackFOR11(initOffset,in,out); break;

        case 12: ipackFOR12(initOffset,in,out); break;

        case 13: ipackFOR13(initOffset,in,out); break;

        case 14: ipackFOR14(initOffset,in,out); break;

        case 15: ipackFOR15(initOffset,in,out); break;

        case 16: ipackFOR16(initOffset,in,out); break;

        case 17: ipackFOR17(initOffset,in,out); break;

        case 18: ipackFOR18(initOffset,in,out); break;

        case 19: ipackFOR19(initOffset,in,out); break;

        case 20: ipackFOR20(initOffset,in,out); break;

        case 21: ipackFOR21(initOffset,in,out); break;

        case 22: ipackFOR22(initOffset,in,out); break;

        case 23: ipackFOR23(initOffset,in,out); break;

        case 24: ipackFOR24(initOffset,in,out); break;

        case 25: ipackFOR25(initOffset,in,out); break;

        case 26: ipackFOR26(initOffset,in,out); break;

        case 27: ipackFOR27(initOffset,in,out); break;

        case 28: ipackFOR28(initOffset,in,out); break;

        case 29: ipackFOR29(initOffset,in,out); break;

        case 30: ipackFOR30(initOffset,in,out); break;

        case 31: ipackFOR31(initOffset,in,out); break;

        case 32: ipackFOR32(initOffset,in,out); break;

        default: break;
    }
}




void simdunpackFOR(uint32_t initvalue, const __m128i *   in, uint32_t * out, const uint32_t bit) {
    __m128i  initOffset = _mm_set1_epi32 (initvalue);
    switch(bit) {
        case 0: iunpackFOR0(initOffset, in,out); break;

        case 1: iunpackFOR1(initOffset, in,out); break;

        case 2: iunpackFOR2(initOffset, in,out); break;

        case 3: iunpackFOR3(initOffset, in,out); break;

        case 4: iunpackFOR4(initOffset, in,out); break;

        case 5: iunpackFOR5(initOffset, in,out); break;

        case 6: iunpackFOR6(initOffset, in,out); break;

        case 7: iunpackFOR7(initOffset, in,out); break;

        case 8: iunpackFOR8(initOffset, in,out); break;

        case 9: iunpackFOR9(initOffset, in,out); break;

        case 10: iunpackFOR10(initOffset, in,out); break;

        case 11: iunpackFOR11(initOffset, in,out); break;

        case 12: iunpackFOR12(initOffset, in,out); break;

        case 13: iunpackFOR13(initOffset, in,out); break;

        case 14: iunpackFOR14(initOffset, in,out); break;

        case 15: iunpackFOR15(initOffset, in,out); break;

        case 16: iunpackFOR16(initOffset, in,out); break;

        case 17: iunpackFOR17(initOffset, in,out); break;

        case 18: iunpackFOR18(initOffset, in,out); break;

        case 19: iunpackFOR19(initOffset, in,out); break;

        case 20: iunpackFOR20(initOffset, in,out); break;

        case 21: iunpackFOR21(initOffset, in,out); break;

        case 22: iunpackFOR22(initOffset, in,out); break;

        case 23: iunpackFOR23(initOffset, in,out); break;

        case 24: iunpackFOR24(initOffset, in,out); break;

        case 25: iunpackFOR25(initOffset, in,out); break;

        case 26: iunpackFOR26(initOffset, in,out); break;

        case 27: iunpackFOR27(initOffset, in,out); break;

        case 28: iunpackFOR28(initOffset, in,out); break;

        case 29: iunpackFOR29(initOffset, in,out); break;

        case 30: iunpackFOR30(initOffset, in,out); break;

        case 31: iunpackFOR31(initOffset, in,out); break;

        case 32: iunpackFOR32(initOffset, in,out); break;

        default: break;
    }
}


uint32_t simdselectFOR(uint32_t initvalue, const __m128i *in, uint32_t bit,
                int slot) {
	const uint32_t * pin = (const uint32_t *) in;
	if( bit == 0) {
                return initvalue;
        } else if (bit == 32) {
		/* silly special case */
		return pin[slot];
	} else {
		const int lane = slot % 4; /* we have 4 interleaved lanes */
		const int bitsinlane = (slot / 4) * bit; /* how many bits in lane */
		const int firstwordinlane = bitsinlane / 32;
		const int secondwordinlane = (bitsinlane + bit - 1) / 32;
		const uint32_t firstpart = pin[4 * firstwordinlane + lane]
				>> (bitsinlane % 32);
		const uint32_t mask = (1 << bit) - 1;
		if (firstwordinlane == secondwordinlane) {
			/* easy common case*/
			return initvalue + (firstpart & mask);
		} else {
			/* harder case where we need to combine two words */
			const uint32_t secondpart = pin[4 * firstwordinlane + 4 + lane];
			const int usablebitsinfirstword = 32 - (bitsinlane % 32);
			return initvalue
					+ ((firstpart | (secondpart << usablebitsinfirstword))
							& mask);
		}
	}

}




int simdsearchwithlengthFOR(uint32_t initvalue, const __m128i *in, uint32_t bit,
                int length, uint32_t key, uint32_t *presult) {
	int count = length;
	int begin = 0;
	uint32_t val;
	while (count > 0) {
		int step = count / 2;
		val = simdselectFOR(initvalue, in, bit, begin + step);
	    if (val < key) {
	    	begin += step + 1;
	    	count -= step + 1;
	    } else count = step;
	}
	*presult = simdselectFOR(initvalue, in, bit, begin);
	return begin;
}

int simdpackFOR_compressedbytes(int length, const uint32_t bit) {
	if(bit == 0) return 0;/* nothing to do */
    if(bit == 32) {
        return length * sizeof(uint32_t);
    }
    return (((length + 3 )/ 4) * bit + 31 ) / 32 * sizeof(__m128i);
}

__m128i * simdpackFOR_length(uint32_t initvalue, const uint32_t *   in, int length, __m128i *    out, const uint32_t bit) {
	int k;
	int inwordpointer;
	__m128i P;
	uint32_t firstpass;
    __m128i offset;
	if(bit == 0) return out;/* nothing to do */
    if(bit == 32) {
        memcpy(out,in,length*sizeof(uint32_t));
        return (__m128i *)((uint32_t *) out + length);
    }
    offset = _mm_set1_epi32(initvalue);
    inwordpointer = 0;
    P = _mm_setzero_si128();
    for(k = 0; k < length / 4 ; ++k) {
        __m128i value = _mm_sub_epi32(_mm_loadu_si128(((const __m128i * ) in + k)),offset);
        P = _mm_or_si128(P,_mm_slli_epi32(value, inwordpointer));
        firstpass = sizeof(uint32_t) * 8 - inwordpointer;
        if(bit<firstpass) {
            inwordpointer+=bit;
        } else {
            _mm_storeu_si128(out++, P);
            P = _mm_srli_epi32(value, firstpass);
            inwordpointer=bit-firstpass;
        }
    }
    if(length % 4 != 0) {
        uint32_t buffer[4];
        __m128i value;
        for(k = 0; k < (length % 4); ++k) {
            buffer[k] = in[length/4*4+k];
        }
        for(k = (length % 4); k < 4 ; ++k) {
            buffer[k] = 0;
        }
        value = _mm_sub_epi32(_mm_loadu_si128((__m128i * ) buffer),offset);
        P = _mm_or_si128(P,_mm_slli_epi32(value, inwordpointer));
        firstpass = sizeof(uint32_t) * 8 - inwordpointer;
        if(bit<firstpass) {
            inwordpointer+=bit;
        } else {
            _mm_storeu_si128(out++, P);
            P = _mm_srli_epi32(value, firstpass);
            inwordpointer=bit-firstpass;
        }
    }
    if(inwordpointer != 0) {
        _mm_storeu_si128(out++, P);
    }
    return out;
}


const __m128i * simdunpackFOR_length(uint32_t initvalue, const __m128i *   in, int length, uint32_t * out, const uint32_t bit) {
    int k;
    __m128i maskbits;
    int inwordpointer;
    __m128i P;
    __m128i offset;
    if(length == 0) return in;
    if(bit == 0) {
        for(k = 0; k < length; ++k) {
            out[k] = initvalue;
        }
        return in;
    }
    if(bit == 32) {
        memcpy(out,in,length*sizeof(uint32_t));
        return (const __m128i *)((const uint32_t *) in + length);
    }
    offset = _mm_set1_epi32(initvalue);
    maskbits = _mm_set1_epi32((1<<bit)-1);
    inwordpointer = 0;
    P = _mm_loadu_si128((__m128i * ) in);
    ++in;
    for(k = 0; k < length  / 4; ++k) {
        __m128i answer = _mm_srli_epi32(P, inwordpointer);
        const uint32_t firstpass = sizeof(uint32_t) * 8 - inwordpointer;
        if(bit < firstpass) {
            inwordpointer += bit;
        } else {
            P = _mm_loadu_si128((__m128i * ) in);
            ++in;
            answer = _mm_or_si128(_mm_slli_epi32(P, firstpass),answer);
            inwordpointer = bit - firstpass;
        }
        answer = _mm_and_si128(maskbits,answer);
        _mm_storeu_si128((__m128i *)out, _mm_add_epi32(answer,offset));
        out += 4;
    }
    if(length % 4 != 0) {
        uint32_t buffer[4];
    	__m128i answer = _mm_srli_epi32(P, inwordpointer);
        const uint32_t firstpass = sizeof(uint32_t) * 8 - inwordpointer;
        if(bit < firstpass) {
            inwordpointer += bit;
        } else {
            P = _mm_loadu_si128((__m128i * ) in);
            ++in;
            answer = _mm_or_si128(_mm_slli_epi32(P, firstpass),answer);
            inwordpointer = bit - firstpass;
        }
        answer = _mm_and_si128(maskbits,answer);
        _mm_storeu_si128((__m128i *)buffer, _mm_add_epi32(answer,offset));
        for(k = 0; k < (length % 4); ++k) {
            *out = buffer[k];
            ++out;
        }
    }
    return in;
}


void simdfastsetFOR(uint32_t initvalue, __m128i * in, uint32_t bit, uint32_t value, size_t index) {
	simdfastset(in, bit, value - initvalue, index);
}
