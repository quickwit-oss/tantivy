/* Type "make example" to build this example program. */
#include <stdio.h>
#include <time.h>
#include <stdlib.h>
#include "simdcomp.h"

/**
We provide several different code examples.
**/


/* very simple test to illustrate a simple application */
int compress_decompress_demo() {
    size_t k, N = 9999;
    __m128i * endofbuf;
    int howmanybytes;
    float compratio;
    uint32_t * datain = malloc(N * sizeof(uint32_t));
    uint8_t * buffer;
    uint32_t * backbuffer = malloc(N * sizeof(uint32_t));
    uint32_t b;
    printf("== simple test\n");

    for (k = 0; k < N; ++k) {       /* start with k=0, not k=1! */
        datain[k] = k;
    }

    b = maxbits_length(datain, N);
    buffer = malloc(simdpack_compressedbytes(N,b));
    endofbuf = simdpack_length(datain, N, (__m128i *)buffer, b);
    howmanybytes = (endofbuf-(__m128i *)buffer)*sizeof(__m128i); /* number of compressed bytes */
    compratio = N*sizeof(uint32_t) * 1.0 / howmanybytes;
    /* endofbuf points to the end of the compressed data */
    buffer = realloc(buffer,(endofbuf-(__m128i *)buffer)*sizeof(__m128i)); /* optional but safe. */
    printf("Compressed %d integers down to %d bytes (comp. ratio = %f).\n",(int)N,howmanybytes,compratio);
    /* in actual applications b must be stored and retrieved: caller is responsible for that. */
    simdunpack_length((const __m128i *)buffer, N, backbuffer, b); /* will return a pointer to endofbuf */ 

    for (k = 0; k < N; ++k) {
        if(datain[k] != backbuffer[k]) {
            printf("bug at %lu \n",(unsigned long)k);
            return -1;
        }
    }
    printf("Code works!\n");
    free(datain);
    free(buffer);
    free(backbuffer);
    return 0;
}



/* compresses data from datain to buffer, returns how many bytes written
used below in simple_demo */
size_t compress(uint32_t * datain, size_t length, uint8_t * buffer) {
    uint32_t offset;
    uint8_t * initout;
    size_t k;
    if(length/SIMDBlockSize*SIMDBlockSize != length) {
        printf("Data length should be a multiple of %i \n",SIMDBlockSize);
    }
    offset = 0;
    initout = buffer;
    for(k = 0; k < length / SIMDBlockSize; ++k) {
        uint32_t b = simdmaxbitsd1(offset,
                                   datain + k * SIMDBlockSize);
        *buffer++ = b;
        simdpackwithoutmaskd1(offset, datain + k * SIMDBlockSize, (__m128i *) buffer,
                              b);
        offset = datain[k * SIMDBlockSize + SIMDBlockSize - 1];
        buffer += b * sizeof(__m128i);
    }
    return buffer - initout;
}

/* Another illustration ... */
void simple_demo() {
    size_t REPEAT = 10, gap;
    size_t N = 1000 * SIMDBlockSize;/* SIMDBlockSize is 128 */
    uint32_t * datain = malloc(N * sizeof(uint32_t));
    size_t compsize;
    clock_t start, end;
    uint8_t * buffer = malloc(N * sizeof(uint32_t) + N / SIMDBlockSize); /* output buffer */
    uint32_t * backbuffer = malloc(SIMDBlockSize * sizeof(uint32_t));
    printf("== simple demo\n");
    for (gap = 1; gap <= 243; gap *= 3) {
        size_t k, repeat;
        uint32_t offset = 0;
        uint32_t bogus = 0;
        double numberofseconds;

        printf("\n");
        printf(" gap = %lu \n", (unsigned long) gap);
        datain[0] = 0;
        for (k = 1; k < N; ++k)
            datain[k] = datain[k-1] + ( rand() % (gap + 1) );
        compsize = compress(datain,N,buffer);
        printf("compression ratio = %f \n",  (N * sizeof(uint32_t))/ (compsize * 1.0 ));
        start = clock();
        for(repeat = 0; repeat < REPEAT; ++repeat) {
            uint8_t * decbuffer = buffer;
            for (k = 0; k * SIMDBlockSize < N; ++k) {
                uint8_t b = *decbuffer++;
                simdunpackd1(offset, (__m128i *) decbuffer, backbuffer, b);
                /* do something here with backbuffer */
                bogus += backbuffer[3];
                decbuffer += b * sizeof(__m128i);
                offset = backbuffer[SIMDBlockSize - 1];
            }
        }
        end = clock();
        numberofseconds = (end-start)/(double)CLOCKS_PER_SEC;
        printf("decoding speed in million of integers per second %f \n",N*REPEAT/(numberofseconds*1000.0*1000.0));
        start = clock();
        for(repeat = 0; repeat < REPEAT; ++repeat) {
            uint8_t * decbuffer = buffer;
            for (k = 0; k * SIMDBlockSize < N; ++k) {
                memcpy(backbuffer,decbuffer+k*SIMDBlockSize,SIMDBlockSize*sizeof(uint32_t));
                bogus += backbuffer[3] - backbuffer[100];
            }
        }
        end = clock();
        numberofseconds = (end-start)/(double)CLOCKS_PER_SEC;
        printf("memcpy speed in million of integers per second %f \n",N*REPEAT/(numberofseconds*1000.0*1000.0));
        printf("ignore me %i \n",bogus);
        printf("All tests are in CPU cache. Avoid out-of-cache decoding in applications.\n");
    }
    free(buffer);
    free(datain);
    free(backbuffer);
}

/* Used below in more_sophisticated_demo ... */
size_t varying_bit_width_compress(uint32_t * datain, size_t length, uint8_t * buffer) {
    uint8_t * initout;
    size_t k;
    if(length/SIMDBlockSize*SIMDBlockSize != length) {
        printf("Data length should be a multiple of %i \n",SIMDBlockSize);
    }
    initout = buffer;
    for(k = 0; k < length / SIMDBlockSize; ++k) {
        uint32_t b = maxbits(datain);
        *buffer++ = b;
        simdpackwithoutmask(datain, (__m128i *)buffer, b);
        datain += SIMDBlockSize;
        buffer += b * sizeof(__m128i);
    }
    return buffer - initout;
}

/* Here we compress the data in blocks of 128 integers with varying bit width */
int varying_bit_width_demo() {
    size_t nn = 128 * 2;
    uint32_t * datainn = malloc(nn * sizeof(uint32_t));
    uint8_t * buffern = malloc(nn * sizeof(uint32_t) + nn / SIMDBlockSize);
    uint8_t * initbuffern = buffern;
    uint32_t * backbuffern = malloc(nn * sizeof(uint32_t));
    size_t k, compsize;
    printf("== varying bit-width demo\n");

    for(k=0; k<nn; ++k) {
        datainn[k] = rand() % (k + 1);
    }

    compsize = varying_bit_width_compress(datainn,nn,buffern);
    printf("encoded size: %u (original size: %u)\n", (unsigned)compsize,
           (unsigned)(nn * sizeof(uint32_t)));

    for (k = 0; k * SIMDBlockSize < nn; ++k) {
        uint32_t b = *buffern;
        buffern++;
        simdunpack((const __m128i *)buffern, backbuffern + k * SIMDBlockSize, b);
        buffern += b * sizeof(__m128i);
    }

    for (k = 0; k < nn; ++k) {
        if(backbuffern[k] != datainn[k]) {
            printf("bug\n");
            return -1;
        }
    }
    printf("Code works!\n");
    free(datainn);
    free(initbuffern);
    free(backbuffern);
    return 0;
}

int main() {
    if(compress_decompress_demo() != 0) return -1;
    if(varying_bit_width_demo() != 0) return -1;
    simple_demo();
    return 0;
}
