/**
 * This code is released under a BSD License.
 */
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include "simdcomp.h"



int testshortpack() {
	int bit;
	size_t i;
	size_t length;
	__m128i * bb;
	srand(0);
	printf("testshortpack\n");
	for (bit = 0; bit < 32; ++bit) {
		const size_t N = 128;
		uint32_t * data = malloc(N * sizeof(uint32_t));
		uint32_t * backdata = malloc(N * sizeof(uint32_t));
		uint32_t * buffer = malloc((2 * N + 1024) * sizeof(uint32_t));

		for (i = 0; i < N; ++i) {
			data[i] = rand() & ((1 << bit) - 1);
		}
		for (length = 0; length <= N; ++length) {
			for (i = 0; i < N; ++i) {
				backdata[i] = 0;
			}
			bb = simdpack_shortlength(data, length, (__m128i *) buffer,
					bit);
			if((bb - (__m128i *) buffer) * sizeof(__m128i) != (unsigned) simdpack_compressedbytes(length,bit)) {
			 printf("bug\n");
			 return -1;
			}
			simdunpack_shortlength((__m128i *) buffer, length,
					backdata, bit);
			for (i = 0; i < length; ++i) {

				if (data[i] != backdata[i]) {
				    printf("bug\n");
					return -1;
				}
			}
		}
		free(data);
		free(backdata);
		free(buffer);
	}
	return 0;
}

int testlongpack() {
	int bit;
	size_t i;
	size_t length;
	__m128i * bb;
	srand(0);
	printf("testlongpack\n");
	for (bit = 0; bit < 32; ++bit) {
		const size_t N = 2048;
		uint32_t * data = malloc(N * sizeof(uint32_t));
		uint32_t * backdata = malloc(N * sizeof(uint32_t));
		uint32_t * buffer = malloc((2 * N + 1024) * sizeof(uint32_t));

		for (i = 0; i < N; ++i) {
			data[i] = rand() & ((1 << bit) - 1);
		}
		for (length = 0; length <= N; ++length) {
			for (i = 0; i < N; ++i) {
				backdata[i] = 0;
			}
			bb = simdpack_length(data, length, (__m128i *) buffer,
					bit);
			if((bb - (__m128i *) buffer) * sizeof(__m128i) != (unsigned) simdpack_compressedbytes(length,bit)) {
			 printf("bug\n");
			 return -1;
			}
			simdunpack_length((__m128i *) buffer, length,
					backdata, bit);
			for (i = 0; i < length; ++i) {

				if (data[i] != backdata[i]) {
				    printf("bug\n");
					return -1;
				}
			}
		}
		free(data);
		free(backdata);
		free(buffer);
	}
	return 0;
}



int testset() {
	int bit;
	size_t i;
	const size_t N = 128;
	uint32_t * data = malloc(N * sizeof(uint32_t));
	uint32_t * backdata = malloc(N * sizeof(uint32_t));
	uint32_t * buffer = malloc((2 * N + 1024) * sizeof(uint32_t));

	srand(0);

	for (bit = 0; bit < 32; ++bit) {
		printf("simple set %d \n",bit);

		for (i = 0; i < N; ++i) {
			data[i] = rand() & ((1 << bit) - 1);
		}
		for (i = 0; i < N; ++i) {
			backdata[i] = 0;
		}
		simdpack(data, (__m128i *) buffer, bit);
		simdunpack((__m128i *) buffer, backdata, bit);
		for (i = 0; i < N; ++i) {
			if (data[i] != backdata[i]) {
			    printf("bug\n");
				return -1;
			}
		}

		for(i = N  ; i > 0; i--) {
			simdfastset((__m128i *) buffer, bit, data[N - i], i - 1);
		}
		simdunpack((__m128i *) buffer, backdata, bit);
		for (i = 0; i < N; ++i) {
			if (data[i] != backdata[N - i - 1]) {
			    printf("bug\n");
				return -1;
			}
		}
		simdpack(data, (__m128i *) buffer, bit);
		for(i = 1  ; i <= N; i++) {
			simdfastset((__m128i *) buffer, bit, data[i - 1], i - 1);
		}
		simdunpack((__m128i *) buffer, backdata, bit);
		for (i = 0; i < N; ++i) {
			if (data[i] != backdata[i]) {
			    printf("bug\n");
				return -1;
			}
		}

	}
	free(data);
	free(backdata);
	free(buffer);

	return 0;
}

#ifdef __SSE4_1__

int testsetd1() {
	int bit;
	size_t i;
	uint32_t newvalue;
	const size_t N = 128;
	uint32_t * data = malloc(N * sizeof(uint32_t));
	uint32_t * datazeroes = malloc(N * sizeof(uint32_t));

	uint32_t * backdata = malloc(N * sizeof(uint32_t));
	uint32_t * buffer = malloc((2 * N + 1024) * sizeof(uint32_t));

	srand(0);
	for (bit = 0; bit < 32; ++bit) {
		printf("simple set d1 %d \n",bit);
		data[0] = rand() & ((1 << bit) - 1);
		datazeroes[0] = 0;

		for (i = 1; i < N; ++i) {
			data[i] = data[i - 1] + (rand() & ((1 << bit) - 1));
			datazeroes[i] = 0;
		}
		for (i = 0; i < N; ++i) {
			backdata[i] = 0;
		}
		simdpackd1(0,datazeroes, (__m128i *) buffer, bit);
 	    for(i = 1  ; i <= N; i++) {
			simdfastsetd1(0,(__m128i *) buffer, bit, data[i - 1], i - 1);
			newvalue = simdselectd1(0, (const __m128i *) buffer, bit,i - 1);
			if( newvalue != data[i-1] ) {
				printf("bad set-select\n");
				return -1;
			}
		}
		simdunpackd1(0,(__m128i *) buffer, backdata, bit);
		for (i = 0; i < N; ++i) {
			if (data[i] != backdata[i])
				return -1;
		}
	}
	free(data);
	free(backdata);
	free(buffer);
        free(datazeroes);
	return 0;
}
#endif

int testsetFOR() {
	int bit;
	size_t i;
	uint32_t newvalue;
	const size_t N = 128;
	uint32_t * data = malloc(N * sizeof(uint32_t));
	uint32_t * datazeroes = malloc(N * sizeof(uint32_t));

	uint32_t * backdata = malloc(N * sizeof(uint32_t));
	uint32_t * buffer = malloc((2 * N + 1024) * sizeof(uint32_t));

	srand(0);
	for (bit = 0; bit < 32; ++bit) {
		printf("simple set FOR %d \n",bit);
		for (i = 0; i < N; ++i) {
			data[i] = (rand() & ((1 << bit) - 1));
			datazeroes[i] = 0;
		}
		for (i = 0; i < N; ++i) {
			backdata[i] = 0;
		}
		simdpackFOR(0,datazeroes, (__m128i *) buffer, bit);
 	    for(i = 1  ; i <= N; i++) {
 	    	simdfastsetFOR(0,(__m128i *) buffer, bit, data[i - 1], i - 1);
			newvalue = simdselectFOR(0, (const __m128i *) buffer, bit,i - 1);
			if( newvalue != data[i-1] ) {
				printf("bad set-select\n");
				return -1;
			}
		}
		simdunpackFOR(0,(__m128i *) buffer, backdata, bit);
		for (i = 0; i < N; ++i) {
			if (data[i] != backdata[i])
				return -1;
		}
	}
	free(data);
	free(backdata);
	free(buffer);
        free(datazeroes);
	return 0;
}

int testshortFORpack() {
	int bit;
	size_t i;
	__m128i * rb;
	size_t length;
	uint32_t offset = 7;
	srand(0);
	for (bit = 0; bit < 32; ++bit) {
		const size_t N = 128;
		uint32_t * data = malloc(N * sizeof(uint32_t));
		uint32_t * backdata = malloc(N * sizeof(uint32_t));
		uint32_t * buffer = malloc((2 * N + 1024) * sizeof(uint32_t));

		for (i = 0; i < N; ++i) {
			data[i] = (rand() & ((1 << bit) - 1)) + offset;
		}
		for (length = 0; length <= N; ++length) {
			for (i = 0; i < N; ++i) {
				backdata[i] = 0;
			}
			rb = simdpackFOR_length(offset,data, length, (__m128i *) buffer,
					bit);
		    if(((rb - (__m128i *) buffer)*sizeof(__m128i)) != (unsigned) simdpackFOR_compressedbytes(length,bit)) {
		      return -1;
		    }
			simdunpackFOR_length(offset,(__m128i *) buffer, length,
					backdata, bit);
			for (i = 0; i < length; ++i) {

				if (data[i] != backdata[i])
					return -1;
			}
		}
		free(data);
		free(backdata);
		free(buffer);
	}
	return 0;
}


#ifdef __AVX2__

int testbabyavx() {
	int bit;
	int trial;
	unsigned int i,j;
	const size_t N = AVXBlockSize;
	srand(0);
	printf("testbabyavx\n");
	printf("bit = ");
	for (bit = 0; bit < 32; ++bit) {
		printf(" %d ",bit);
		fflush(stdout);
		for(trial = 0; trial < 100; ++trial) {
			uint32_t * data = malloc(N * sizeof(uint32_t)+ 64 * sizeof(uint32_t));
			uint32_t * backdata = malloc(N * sizeof(uint32_t) + 64 * sizeof(uint32_t) );
			__m256i * buffer = malloc((2 * N + 1024) * sizeof(uint32_t) + 32);

			for (i = 0; i < N; ++i) {
				data[i] = rand() & ((uint32_t)(1 << bit) - 1);
			}
			for (i = 0; i < N; ++i) {
				backdata[i] = 0;
			}
            if(avxmaxbits(data) != maxbits_length(data,N)) {
            	printf("avxmaxbits is buggy\n");
				return -1;
            }

			avxpackwithoutmask(data, buffer, bit);
			avxunpack(buffer, backdata, bit);
			for (i = 0; i < AVXBlockSize; ++i) {
				if (data[i] != backdata[i]) {
					printf("bug\n");
					for (j = 0; j < N; ++j) {
						if (data[j] != backdata[j]) {
							printf("data[%d]=%d v.s. backdata[%d]=%d\n",j,data[j],j,backdata[j]);
						} else {
							printf("data[%d]=%d\n",j,data[j]);
						}
					}
					return -1;
				}
			}
			free(data);
			free(backdata);
			free(buffer);
		}
	}
	printf("\n");
	return 0;
}

int testavx2() {
    int N = 5000 * AVXBlockSize, gap;
    __m256i * buffer = malloc(AVXBlockSize * sizeof(uint32_t));
    uint32_t * datain = malloc(N * sizeof(uint32_t));
    uint32_t * backbuffer = malloc(AVXBlockSize * sizeof(uint32_t));
    for (gap = 1; gap <= 387420489; gap *= 3) {
        int k;
        printf(" gap = %u \n", gap);
        for (k = 0; k < N; ++k)
            datain[k] = k * gap;
        for (k = 0; k * AVXBlockSize < N; ++k) {
            /*
               First part works for general arrays (sorted or unsorted)
            */
            int j;
       	    /* we compute the bit width */
            const uint32_t b = avxmaxbits(datain + k * AVXBlockSize);
            if(avxmaxbits(datain + k * AVXBlockSize) != maxbits_length(datain + k * AVXBlockSize,AVXBlockSize)) {
            	printf("avxmaxbits is buggy %d %d \n",
            			avxmaxbits(datain + k * AVXBlockSize),
						maxbits_length(datain + k * AVXBlockSize,AVXBlockSize));
				return -1;
            }
            printf("bit width = %d\n",b);


            /* we read 256 integers at "datain + k * AVXBlockSize" and
               write b 256-bit vectors at "buffer" */
            avxpackwithoutmask(datain + k * AVXBlockSize, buffer, b);
            /* we read back b1 128-bit vectors at "buffer" and write 128 integers at backbuffer */
			avxunpack(buffer, backbuffer, b);/* uncompressed */
			for (j = 0; j < AVXBlockSize; ++j) {
				if (backbuffer[j] != datain[k * AVXBlockSize + j]) {
					int i;
					printf("bug in avxpack\n");
					for(i = 0; i < AVXBlockSize; ++i) {
						printf("data[%d]=%d got back %d %s\n",i,
								datain[k * AVXBlockSize + i],backbuffer[i],
								datain[k * AVXBlockSize + i]!=backbuffer[i]?"bug":"");
					}
					return -2;
				}
			}
        }
    }
    free(buffer);
    free(datain);
    free(backbuffer);
    printf("Code looks good.\n");
    return 0;
}
#endif /* avx2 */

int test() {
    int N = 5000 * SIMDBlockSize, gap;
    __m128i * buffer = malloc(SIMDBlockSize * sizeof(uint32_t));
    uint32_t * datain = malloc(N * sizeof(uint32_t));
    uint32_t * backbuffer = malloc(SIMDBlockSize * sizeof(uint32_t));
    for (gap = 1; gap <= 387420489; gap *= 3) {
        int k;
        printf(" gap = %u \n", gap);
        for (k = 0; k < N; ++k)
            datain[k] = k * gap;
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
                if (backbuffer[j] != datain[k * SIMDBlockSize + j]) {
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
                   if (backbuffer[j] != datain[k * SIMDBlockSize + j]) {
                       printf("bug in simdpack d1\n");
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

#ifdef __SSE4_1__
int testFOR() {
    int N = 5000 * SIMDBlockSize, gap;
    __m128i * buffer = malloc(SIMDBlockSize * sizeof(uint32_t));
    uint32_t * datain = malloc(N * sizeof(uint32_t));
    uint32_t * backbuffer = malloc(SIMDBlockSize * sizeof(uint32_t));
    uint32_t tmax, tmin, tb;
    for (gap = 1; gap <= 387420489; gap *= 2) {
        int k;
        printf(" gap = %u \n", gap);
        for (k = 0; k < N; ++k)
            datain[k] = k * gap;
        for (k = 0; k * SIMDBlockSize < N; ++k) {
            int j;
            simdmaxmin_length(datain + k * SIMDBlockSize,SIMDBlockSize,&tmin,&tmax);
       	    /* we compute the bit width */
            tb  = bits(tmax - tmin);


            /* we read 128 integers at "datain + k * SIMDBlockSize" and
               write b 128-bit vectors at "buffer" */
            simdpackFOR(tmin,datain + k * SIMDBlockSize, buffer, tb);

            for (j = 0; j < SIMDBlockSize; ++j) {
                        uint32_t selectedvalue = simdselectFOR(tmin,buffer,tb,j);
                    	if (selectedvalue != datain[k * SIMDBlockSize + j]) {
                            printf("bug in simdselectFOR\n");
                            return -3;
                        }
            }
            /* we read back b1 128-bit vectors at "buffer" and write 128 integers at backbuffer */
            simdunpackFOR(tmin,buffer, backbuffer, tb);/* uncompressed */
            for (j = 0; j < SIMDBlockSize; ++j) {
            	if (backbuffer[j] != datain[k * SIMDBlockSize + j]) {
                    printf("bug in simdpackFOR\n");
                    return -2;
                }
            }
        }
    }
    free(buffer);
    free(datain);
    free(backbuffer);
    printf("Code looks good.\n");
    return 0;
}
#endif

#define MAX 300
int test_simdmaxbitsd1_length() {
    uint32_t result, buffer[MAX + 1];
    int i, j;

    memset(&buffer[0], 0xff, sizeof(buffer));

    /* this test creates buffers of different length; each buffer is
     * initialized to result in the following deltas:
     * length 1: 2
     * length 2: 1 2
     * length 3: 1 1 2
     * length 4: 1 1 1 2
     * length 5: 1 1 1 1 2
     * etc. Each sequence's "maxbits" is 2. */
    for (i = 0; i < MAX; i++) {
      for (j = 0; j < i; j++)
        buffer[j] = j + 1;
      buffer[i] = i + 2;

      result = simdmaxbitsd1_length(0, &buffer[0], i + 1);
      if (result != 2) {
        printf("simdmaxbitsd1_length: unexpected result %u in loop %d\n",
                result, i);
        return -1;
      }
    }
    printf("simdmaxbitsd1_length: ok\n");
    return 0;
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

#ifdef __SSE4_1__
int test_simdpackedsearch() {
    uint32_t buffer[128];
    uint32_t result = 0;
    int b, i;
    uint32_t init = 0;
    __m128i initial = _mm_set1_epi32(init);

    /* initialize the buffer */
    for (i = 0; i < 128; i++)
        buffer[i] = (uint32_t)(i + 1);

    /* this test creates delta encoded buffers with different bits, then
     * performs lower bound searches for each key */
    for (b = 1; b <= 32; b++) {
        uint32_t out[128];
        /* delta-encode to 'i' bits */
        simdpackwithoutmaskd1(init, buffer, (__m128i *)out, b);
        initial = _mm_setzero_si128();
        printf("simdsearchd1: %d bits\n", b);

        /* now perform the searches */
        initial = _mm_set1_epi32(init);
        assert(simdsearchd1(&initial, (__m128i *)out, b, 0, &result) == 0);
        assert(result > 0);

        for (i = 1; i <= 128; i++) {
        	initial = _mm_set1_epi32(init);
            assert(simdsearchd1(&initial, (__m128i *)out, b,
                                    (uint32_t)i, &result) == i - 1);
            assert(result == (unsigned)i);
        }
        initial = _mm_set1_epi32(init);
        assert(simdsearchd1(&initial, (__m128i *)out, b, 200, &result)
                        == 128);
        assert(result > 200);
    }
    printf("simdsearchd1: ok\n");
    return 0;
}

int test_simdpackedsearchFOR() {
    uint32_t buffer[128];
    uint32_t result = 0;
    int b;
    uint32_t i;
    uint32_t maxv, tmin, tmax, tb;
    uint32_t out[128];

    /* this test creates delta encoded buffers with different bits, then
     * performs lower bound searches for each key */
    for (b = 1; b <= 32; b++) {
        /* initialize the buffer */
    	maxv = (b == 32)
    			? 0xFFFFFFFF
    					: ((1U<<b) - 1);
        for (i = 0; i < 128; i++)
            buffer[i] = maxv * (i + 1) / 128;
        simdmaxmin_length(buffer,SIMDBlockSize,&tmin,&tmax);
   	    /* we compute the bit width */
        tb  = bits(tmax - tmin);
        /* delta-encode to 'i' bits */
        simdpackFOR(tmin, buffer, (__m128i *)out, tb);
        printf("simdsearchd1: %d bits\n", b);

        /* now perform the searches */
        for (i = 0; i < 128; i++) {
        	assert(buffer[i] == simdselectFOR(tmin, (__m128i *)out, tb,i));
        }
        for (i = 0; i < 128; i++) {
            int x = simdsearchwithlengthFOR(tmin, (__m128i *)out, tb,
                                    128,buffer[i], &result) ;
            assert(simdselectFOR(tmin, (__m128i *)out, tb,x) == buffer[x]);
            assert(simdselectFOR(tmin, (__m128i *)out, tb,x) == result);
            assert(buffer[x] == result);
            assert(result == buffer[i]);
            assert(buffer[x] == buffer[i]);
        }
    }
    printf("simdsearchFOR: ok\n");
    return 0;
}

int test_simdpackedsearch_advanced() {
    uint32_t buffer[128];
    uint32_t backbuffer[128];
	uint32_t out[128];
    uint32_t result = 0;
    uint32_t b, i;
    uint32_t init = 0;
    __m128i initial = _mm_set1_epi32(init);


    /* this test creates delta encoded buffers with different bits, then
     * performs lower bound searches for each key */
    for (b = 0; b <= 32; b++) {
    	uint32_t prev = init;
        /* initialize the buffer */
        for (i = 0; i < 128; i++) {
            buffer[i] =  ((uint32_t)(1431655765 * i + 0xFFFFFFFF)) ;
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
        assert(simdmaxbitsd1(init, buffer)<=b);
        for (i = 0; i < 128; i++) {
        	out[i] = 0; /* memset would do too */
        }

        /* delta-encode to 'i' bits */
        simdpackwithoutmaskd1(init, buffer, (__m128i *)out, b);
        simdunpackd1(init,  (__m128i *)out, backbuffer, b);

        for (i = 0; i < 128; i++) {
        	assert(buffer[i] == backbuffer[i]);
        }

        printf("advanced simdsearchd1: %d bits\n", b);

        for (i = 0; i < 128; i++) {
        	int pos;
            initial = _mm_set1_epi32(init);
        	pos = simdsearchd1(&initial, (__m128i *)out, b,
                    buffer[i], &result);
        	assert(pos == simdsearchwithlengthd1(init, (__m128i *)out, b, 128,
                    buffer[i], &result));
        	assert(buffer[pos] == buffer[i]);
            if(pos > 0)
            	assert(buffer[pos - 1] < buffer[i]);
            assert(result == buffer[i]);
        }
        for (i = 0; i < 128; i++) {
        	int pos;
        	if(buffer[i] == 0) continue;
        	initial = _mm_set1_epi32(init);
        	pos = simdsearchd1(&initial, (__m128i *)out, b,
                    buffer[i] - 1, &result);
        	assert(pos == simdsearchwithlengthd1(init, (__m128i *)out, b, 128,
                    buffer[i] - 1, &result));
        	assert(buffer[pos] >= buffer[i]  - 1);
            if(pos > 0)
            	assert(buffer[pos - 1] < buffer[i]  - 1);
            assert(result == buffer[pos]);
        }
		for (i = 0; i < 128; i++) {
			int pos;
			if (buffer[i] + 1 == 0)
				continue;
			initial = _mm_set1_epi32(init);
			pos = simdsearchd1(&initial, (__m128i *) out, b,
					buffer[i] + 1, &result);
			assert(pos == simdsearchwithlengthd1(init, (__m128i *)out, b, 128,
                    buffer[i] + 1, &result));
			if(pos == 128) {
				assert(buffer[i] == buffer[127]);
			} else {
			  assert(buffer[pos] >= buffer[i] + 1);
			  if (pos > 0)
				assert(buffer[pos - 1] < buffer[i] + 1);
			  assert(result == buffer[pos]);
			}
		}
    }
    printf("advanced simdsearchd1: ok\n");
    return 0;
}

int test_simdpackedselect() {
    uint32_t buffer[128];
    uint32_t initial = 33;
    int b, i;

    /* initialize the buffer */
    for (i = 0; i < 128; i++)
        buffer[i] = (uint32_t)(initial + i);

    /* this test creates delta encoded buffers with different bits, then
     * performs lower bound searches for each key */
    for (b = 1; b <= 32; b++) {
        uint32_t out[128];
        /* delta-encode to 'i' bits */
        simdpackwithoutmaskd1(initial, buffer, (__m128i *)out, b);

        printf("simdselectd1: %d bits\n", b);

        /* now perform the searches */
        for (i = 0; i < 128; i++) {
            assert(simdselectd1(initial, (__m128i *)out, b, (uint32_t)i)
                            == initial + i);
        }
    }
    printf("simdselectd1: ok\n");
    return 0;
}

int test_simdpackedselect_advanced() {
    uint32_t buffer[128];
    uint32_t initial = 33;
    uint32_t b;
    int i;

    /* this test creates delta encoded buffers with different bits, then
     * performs lower bound searches for each key */
    for (b = 0; b <= 32; b++) {
        uint32_t prev = initial;
    	uint32_t out[128];
        /* initialize the buffer */
        for (i = 0; i < 128; i++) {
            buffer[i] =  ((uint32_t)(165576 * i)) ;
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

        printf("simdselectd1: %d bits\n", b);

        /* now perform the searches */
        for (i = 0; i < 128; i++) {
        	uint32_t valretrieved = simdselectd1(initial, (__m128i *)out, b, (uint32_t)i);
            assert(valretrieved == buffer[i]);
        }
    }
    printf("advanced simdselectd1: ok\n");
    return 0;
}
#endif


int main() {
    int r;
    r =  testsetFOR();
    if (r) {
         printf("test failure 1\n");
         return r;
    }

#ifdef __SSE4_1__
    r =  testsetd1();
    if (r) {
         printf("test failure 2\n");
         return r;
    }
#endif
    r =  testset();
    if (r) {
         printf("test failure 3\n");
         return r;
    }

    r = testshortFORpack();
    if (r) {
         printf("test failure 4\n");
         return r;
    }
    r = testshortpack();
    if (r) {
         printf("test failure 5\n");
         return r;
    }
    r = testlongpack();
    if (r) {
         printf("test failure 6\n");
         return r;
    }
#ifdef __SSE4_1__
    r = test_simdpackedsearchFOR();
    if (r) {
         printf("test failure 7\n");
         return r;
    }

    r = testFOR();
    if (r) {
         printf("test failure 8\n");
         return r;
    }
#endif
#ifdef __AVX2__
    r= testbabyavx();
    if (r) {
         printf("test failure baby avx\n");
         return r;
    }

    r = testavx2();
    if (r) {
         printf("test failure 9 avx\n");
         return r;
    }
#endif
    r = test();
    if (r) {
         printf("test failure 9\n");
         return r;
    }

    r = test_simdmaxbitsd1_length();
    if (r) {
         printf("test failure 10\n");
         return r;
    }
#ifdef __SSE4_1__
    r = test_simdpackedsearch();
    if (r) {
         printf("test failure 11\n");
         return r;
    }

    r = test_simdpackedsearch_advanced();
    if (r) {
         printf("test failure 12\n");
         return r;
    }

    r = test_simdpackedselect();
    if (r) {
         printf("test failure 13\n");
         return r;
    }

    r = test_simdpackedselect_advanced();
    if (r) {
         printf("test failure 14\n");
         return r;
    }
#endif
    printf("All tests OK!\n");


    return 0;
}
