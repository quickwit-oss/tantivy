/**
 * This code is released under a BSD License.
 */
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "simdcomp.h"

#ifdef _MSC_VER
# include <windows.h>

__int64 freq;

typedef __int64 time_snap_t;

static time_snap_t time_snap(void)
{
	__int64 now;

	QueryPerformanceCounter((LARGE_INTEGER *)&now);

	return (__int64)((now*1000000)/freq);
}
# define TIME_SNAP_FMT "%I64d"
#else
# define time_snap clock
# define TIME_SNAP_FMT "%lu"
typedef clock_t time_snap_t;
#endif


void benchmarkSelect() {
    uint32_t buffer[128];
    uint32_t backbuffer[128];
    uint32_t initial = 33;
    uint32_t b;
    time_snap_t S1, S2, S3;
    int i;
    printf("benchmarking select \n");

    /* this test creates delta encoded buffers with different bits, then
     * performs lower bound searches for each key */
    for (b = 0; b <= 32; b++) {
        uint32_t prev = initial;
        uint32_t out[128];
        /* initialize the buffer */
        for (i = 0; i < 128; i++) {
            buffer[i] =  ((uint32_t)(1655765 * i )) ;
            if(b < 32) buffer[i] %= (1<<b);
        }
        for (i = 0; i < 128; i++) {
            buffer[i] = buffer[i] + prev;
            prev = buffer[i];
        }

        for (i = 1; i < 128; i++) {
            if(buffer[i] < buffer[i-1] )
                buffer[i] = buffer[i-1];
        }
        assert(simdmaxbitsd1(initial, buffer)<=b);

        for (i = 0; i < 128; i++) {
            out[i] = 0; /* memset would do too */
        }

        /* delta-encode to 'i' bits */
        simdpackwithoutmaskd1(initial, buffer, (__m128i *)out, b);

        S1 = time_snap();
        for (i = 0; i < 128 * 10; i++) {
            uint32_t valretrieved = simdselectd1(initial, (__m128i *)out, b, (uint32_t)i % 128);
            assert(valretrieved == buffer[i%128]);
        }
        S2 = time_snap();
        for (i = 0; i < 128 * 10; i++) {
            simdunpackd1(initial,  (__m128i *)out, backbuffer, b);
            assert(backbuffer[i % 128] == buffer[i % 128]);
        }
        S3 = time_snap();
        printf("bit width = %d, fast select function time = " TIME_SNAP_FMT ", naive time = " TIME_SNAP_FMT "  \n", b, (S2-S1), (S3-S2));
    }
}

int uint32_cmp(const void *a, const void *b)
{
    const uint32_t *ia = (const uint32_t *)a;
    const uint32_t *ib = (const uint32_t *)b;
    if(*ia < *ib)
        return -1;
    else if (*ia > *ib)
        return 1;
    return 0;
}

/* adapted from wikipedia */
int binary_search(uint32_t * A, uint32_t key, int imin, int imax)
{
    int imid;
    imax --;
    while(imin + 1 < imax) {
        imid = imin + ((imax - imin) / 2);

        if (A[imid] > key) {
            imax = imid;
        } else if (A[imid] < key) {
            imin = imid;
        } else {
            return imid;
        }
    }
    return imax;
}


/* adapted from wikipedia */
int lower_bound(uint32_t * A, uint32_t key, int imin, int imax)
{
    int imid;
    imax --;
    while(imin + 1 < imax) {
        imid = imin + ((imax - imin) / 2);

        if (A[imid] >= key) {
            imax = imid;
        } else if (A[imid] < key) {
            imin = imid;
        }
    }
    if(A[imin] >= key) return imin;
    return imax;
}

void benchmarkSearch() {
    uint32_t buffer[128];
    uint32_t backbuffer[128];
    uint32_t out[128];
    uint32_t result, initial = 0;
    uint32_t b, i;
    time_snap_t S1, S2, S3, S4;

    printf("benchmarking search \n");

    /* this test creates delta encoded buffers with different bits, then
     * performs lower bound searches for each key */
    for (b = 0; b <= 32; b++) {
        uint32_t prev = initial;
        /* initialize the buffer */
        for (i = 0; i < 128; i++) {
            buffer[i] =  ((uint32_t)rand()) ;
            if(b < 32) buffer[i] %= (1<<b);
        }

        qsort(buffer,128, sizeof(uint32_t), uint32_cmp);

        for (i = 0; i < 128; i++) {
            buffer[i] = buffer[i] + prev;
            prev = buffer[i];
        }
        for (i = 1; i < 128; i++) {
            if(buffer[i] < buffer[i-1] )
                buffer[i] = buffer[i-1];
        }
        assert(simdmaxbitsd1(initial, buffer)<=b);
        for (i = 0; i < 128; i++) {
            out[i] = 0; /* memset would do too */
        }

        /* delta-encode to 'i' bits */
        simdpackwithoutmaskd1(initial, buffer, (__m128i *)out, b);
        simdunpackd1(initial,  (__m128i *)out, backbuffer, b);

        for (i = 0; i < 128; i++) {
            assert(buffer[i] == backbuffer[i]);
         }
        S1 = time_snap();
        for (i = 0; i < 128 * 10; i++) {

            int pos;
            uint32_t pseudorandomkey  =  buffer[i%128];
            __m128i vecinitial = _mm_set1_epi32(initial);
            pos = simdsearchd1(&vecinitial, (__m128i *)out, b,
                               pseudorandomkey, &result);
            if((result < pseudorandomkey) || (buffer[pos] != result)) {
                printf("bug A.\n");
            } else if (pos > 0) {
                if(buffer[pos-1] >= pseudorandomkey)
                    printf("bug B.\n");
            }
        }
        S2 = time_snap();
        for (i = 0; i < 128 * 10; i++) {
            int pos;
            uint32_t pseudorandomkey  =  buffer[i%128];
            simdunpackd1(initial,  (__m128i *)out, backbuffer, b);
            pos =  lower_bound(backbuffer, pseudorandomkey, 0, 128);
            result = backbuffer[pos];

            if((result < pseudorandomkey) || (buffer[pos] != result)) {
                printf("bug C.\n");
            } else if (pos > 0) {
                if(buffer[pos-1] >= pseudorandomkey)
                    printf("bug D.\n");
            }
        }
        S3 = time_snap();
        for (i = 0; i < 128 * 10; i++) {

            int pos;
            uint32_t pseudorandomkey  =  buffer[i%128];
            pos = simdsearchwithlengthd1(initial, (__m128i *)out, b, 128,
                               pseudorandomkey, &result);
            if((result < pseudorandomkey) || (buffer[pos] != result)) {
                printf("bug A.\n");
            } else if (pos > 0) {
                if(buffer[pos-1] >= pseudorandomkey)
                    printf("bug B.\n");
            }
        }
        S4 = time_snap();

        printf("bit width = %d, fast search function time = " TIME_SNAP_FMT ", naive time = " TIME_SNAP_FMT " , fast with length time = " TIME_SNAP_FMT "  \n", b, (S2-S1), (S3-S2), (S4-S3) );
    }
}


int main() {
#ifdef _MSC_VER
    QueryPerformanceFrequency((LARGE_INTEGER *)&freq);
#endif
    benchmarkSearch();
    benchmarkSelect();
    return 0;
}
