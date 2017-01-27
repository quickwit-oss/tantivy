/**
 * This code is released under a BSD License.
 */
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include "simdcomp.h"


#define get_random_char() (uint8_t)(rand() % 256);


int main() {
    int N = 5000 * SIMDBlockSize, gap;
    __m128i * buffer = malloc(SIMDBlockSize * sizeof(uint32_t));
    uint32_t * datain = malloc(N * sizeof(uint32_t));
    uint32_t * backbuffer = malloc(SIMDBlockSize * sizeof(uint32_t));

    srand(time(NULL));

    for (gap = 1; gap <= 387420489; gap *= 3) {
        int k;
        printf(" gap = %u \n", gap);

    /* simulate some random character string, don't care about endiannes */
        for (k = 0; k < N; ++k) {
        uint8_t _tmp[4];
 
            _tmp[0] = get_random_char();
            _tmp[1] = get_random_char();
            _tmp[2] = get_random_char();
            _tmp[3] = get_random_char();

            memmove(&datain[k], _tmp, 4);
        }
        for (k = 0; k * SIMDBlockSize < N; ++k) {
            /*
               First part works for general arrays (sorted or unsorted)
            */
            int j;
               /* we compute the bit width */
            const uint32_t b = maxbits(datain + k * SIMDBlockSize);
            /* we read 128 integers at "datain + k * SIMDBlockSize" and
               write b 128-bit vectors at "buffer" */
            simdpackwithoutmask(datain + k * SIMDBlockSize, buffer, b);
            /* we read back b1 128-bit vectors at "buffer" and write 128 integers at backbuffer */
            simdunpack(buffer, backbuffer, b);/* uncompressed */
            for (j = 0; j < SIMDBlockSize; ++j) {
                uint8_t chars_back[4];
                uint8_t chars_in[4];

                memmove(chars_back, &backbuffer[j], 4);
                memmove(chars_in, &datain[k * SIMDBlockSize + j], 4);

                if (chars_in[0] != chars_back[0]
                    || chars_in[1] != chars_back[1]
                    || chars_in[2] != chars_back[2]
                    || chars_in[3] != chars_back[3]) {
                    printf("bug in simdpack\n");
                    return -2;
                }
            }

            {
                /*
                 next part assumes that the data is sorted (uses differential coding)
                */
                uint32_t offset = 0;
                /* we compute the bit width */
                const uint32_t b1 = simdmaxbitsd1(offset,
                datain + k * SIMDBlockSize);
                   /* we read 128 integers at "datain + k * SIMDBlockSize" and
                  write b1 128-bit vectors at "buffer" */
                   simdpackwithoutmaskd1(offset, datain + k * SIMDBlockSize, buffer,
                b1);
                   /* we read back b1 128-bit vectors at "buffer" and write 128 integers at backbuffer */
                   simdunpackd1(offset, buffer, backbuffer, b1);
                for (j = 0; j < SIMDBlockSize; ++j) {
                    uint8_t chars_back[4];
                    uint8_t chars_in[4];

                    memmove(chars_back, &backbuffer[j], 4);
                    memmove(chars_in, &datain[k * SIMDBlockSize + j], 4);

                    if (chars_in[0] != chars_back[0]
                        || chars_in[1] != chars_back[1]
                        || chars_in[2] != chars_back[2]
                        || chars_in[3] != chars_back[3]) {
                        printf("bug in simdpack\n");
                        return -3;
                    }
                }
                offset = datain[k * SIMDBlockSize + SIMDBlockSize - 1];
            }
        }
    }
    free(buffer);
    free(datain);
    free(backbuffer);
    printf("Code looks good.\n");
    return 0;
}
