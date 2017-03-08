#include "simdcomp.h"
#include "simdcomputil.h"

// assumes datain has a size of 128 uint32
// and that buffer is large enough to host the data.
size_t compress_sorted(
        const uint32_t* datain,
        uint8_t* output,
        const uint32_t offset) {
    const uint32_t b = simdmaxbitsd1(offset, datain);
    *output++ = b;
    simdpackwithoutmaskd1(offset, datain, (__m128i *) output,  b);
    return 1 + b * sizeof(__m128i);;
}

// assumes datain has a size of 128 uint32
// and that buffer is large enough to host the data.
size_t uncompress_sorted(
        const uint8_t* compressed_data, 
        uint32_t* output, 
        uint32_t offset) {
    const uint32_t b = *compressed_data++;
    simdunpackd1(offset, (__m128i *)compressed_data, output, b);
    return 1 + b * sizeof(__m128i);
}

size_t compress_unsorted(
        const uint32_t* datain,
        uint8_t* output) {
    const uint32_t b = maxbits(datain);
    *output++ = b;
    simdpackwithoutmask(datain, (__m128i *) output,  b);
    return 1 + b * sizeof(__m128i);;
}

size_t uncompress_unsorted(
        const uint8_t* compressed_data, 
        uint32_t* output) {
    const uint32_t b = *compressed_data++;
    simdunpack((__m128i *)compressed_data, output, b);
    return 1 + b * sizeof(__m128i);
}