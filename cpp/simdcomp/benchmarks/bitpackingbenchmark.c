#include <stdio.h>

#include "simdcomp.h"


#define RDTSC_START(cycles)                                                   \
    do {                                                                      \
        register unsigned cyc_high, cyc_low;                                  \
        __asm volatile(                                                       \
            "cpuid\n\t"                                                       \
            "rdtsc\n\t"                                                       \
            "mov %%edx, %0\n\t"                                               \
            "mov %%eax, %1\n\t"                                               \
            : "=r"(cyc_high), "=r"(cyc_low)::"%rax", "%rbx", "%rcx", "%rdx"); \
        (cycles) = ((uint64_t)cyc_high << 32) | cyc_low;                      \
    } while (0)

#define RDTSC_FINAL(cycles)                                                   \
    do {                                                                      \
        register unsigned cyc_high, cyc_low;                                  \
        __asm volatile(                                                       \
            "rdtscp\n\t"                                                      \
            "mov %%edx, %0\n\t"                                               \
            "mov %%eax, %1\n\t"                                               \
            "cpuid\n\t"                                                       \
            : "=r"(cyc_high), "=r"(cyc_low)::"%rax", "%rbx", "%rcx", "%rdx"); \
        (cycles) = ((uint64_t)cyc_high << 32) | cyc_low;                      \
    } while (0)




uint32_t * get_random_array_from_bit_width(uint32_t length, uint32_t bit) {
    uint32_t * answer = malloc(sizeof(uint32_t) * length);
    uint32_t mask = (uint32_t) ((UINT64_C(1) << bit) - 1);
    uint32_t i;
    for(i = 0; i < length; ++i) {
        answer[i] = rand() & mask;
    }
    return answer;
}

uint32_t * get_random_array_from_bit_width_d1(uint32_t length, uint32_t bit) {
    uint32_t * answer = malloc(sizeof(uint32_t) * length);
    uint32_t mask = (uint32_t) ((UINT64_C(1) << bit) - 1);
    uint32_t i;
    answer[0] = rand() & mask;
    for(i = 1; i < length; ++i) {
        answer[i] = answer[i-1] + (rand() & mask);
    }
    return answer;
}


void demo128() {
    const uint32_t length = 128;
    uint32_t bit;
    printf("# --- %s\n", __func__);
    printf("# compressing %d integers\n",length);
    printf("# format: bit width, pack in cycles per int, unpack in cycles per int\n");
    for(bit = 1; bit <= 32; ++bit) {
        uint32_t i;

        uint32_t * data = get_random_array_from_bit_width(length, bit);
        __m128i * buffer = malloc(length * sizeof(uint32_t));
        uint32_t * backdata = malloc(length * sizeof(uint32_t));
        uint32_t repeat = 500;
        uint64_t min_diff;
        printf("%d\t",bit);
        min_diff = (uint64_t)-1;
        for (i = 0; i < repeat; i++) {
            uint64_t cycles_start, cycles_final, cycles_diff;
            __asm volatile("" ::: /* pretend to clobber */ "memory");
            RDTSC_START(cycles_start);
            simdpackwithoutmask(data,buffer, bit);
            RDTSC_FINAL(cycles_final);
            cycles_diff = (cycles_final - cycles_start);
            if (cycles_diff < min_diff) min_diff = cycles_diff;
        }
        printf("%.2f\t",min_diff*1.0/length);
        min_diff = (uint64_t)-1;
        for (i = 0; i < repeat; i++) {
            uint64_t cycles_start, cycles_final, cycles_diff;
            __asm volatile("" ::: /* pretend to clobber */ "memory");
            RDTSC_START(cycles_start);
            simdunpack(buffer, backdata,bit);
            RDTSC_FINAL(cycles_final);
            cycles_diff = (cycles_final - cycles_start);
            if (cycles_diff < min_diff) min_diff = cycles_diff;
        }
        printf("%.2f\t",min_diff*1.0/length);

        free(data);
        free(buffer);
        free(backdata);
        printf("\n");
    }
    printf("\n\n"); /* two blank lines are required by gnuplot */
}

void demo128_d1() {
    const uint32_t length = 128;
    uint32_t bit;
    printf("# --- %s\n", __func__);
    printf("# compressing %d integers\n",length);
    printf("# format: bit width, pack in cycles per int, unpack in cycles per int\n");
    for(bit = 1; bit <= 32; ++bit) {
        uint32_t i;

        uint32_t * data = get_random_array_from_bit_width_d1(length, bit);
        __m128i * buffer = malloc(length * sizeof(uint32_t));
        uint32_t * backdata = malloc(length * sizeof(uint32_t));
        uint32_t repeat = 500;
        uint64_t min_diff;
        printf("%d\t",bit);
        min_diff = (uint64_t)-1;
        for (i = 0; i < repeat; i++) {
            uint64_t cycles_start, cycles_final, cycles_diff;
            __asm volatile("" ::: /* pretend to clobber */ "memory");
            RDTSC_START(cycles_start);
            simdpackwithoutmaskd1(0,data,buffer, bit);
            RDTSC_FINAL(cycles_final);
            cycles_diff = (cycles_final - cycles_start);
            if (cycles_diff < min_diff) min_diff = cycles_diff;
        }
        printf("%.2f\t",min_diff*1.0/length);
        min_diff = (uint64_t)-1;
        for (i = 0; i < repeat; i++) {
            uint64_t cycles_start, cycles_final, cycles_diff;
            __asm volatile("" ::: /* pretend to clobber */ "memory");
            RDTSC_START(cycles_start);
            simdunpackd1(0,buffer, backdata,bit);
            RDTSC_FINAL(cycles_final);
            cycles_diff = (cycles_final - cycles_start);
            if (cycles_diff < min_diff) min_diff = cycles_diff;
        }
        printf("%.2f\t",min_diff*1.0/length);

        free(data);
        free(buffer);
        free(backdata);
        printf("\n");
    }
    printf("\n\n"); /* two blank lines are required by gnuplot */
}

#ifdef __AVX2__
void demo256() {
    const uint32_t length = 256;
    uint32_t bit;
    printf("# --- %s\n", __func__);
    printf("# compressing %d integers\n",length);
    printf("# format: bit width, pack in cycles per int, unpack in cycles per int\n");
    for(bit = 1; bit <= 32; ++bit) {
        uint32_t i;

        uint32_t * data = get_random_array_from_bit_width(length, bit);
        __m256i * buffer = malloc(length * sizeof(uint32_t));
        uint32_t * backdata = malloc(length * sizeof(uint32_t));
        uint32_t repeat = 500;
        uint64_t min_diff;
        printf("%d\t",bit);
        min_diff = (uint64_t)-1;
        for (i = 0; i < repeat; i++) {
            uint64_t cycles_start, cycles_final, cycles_diff;
            __asm volatile("" ::: /* pretend to clobber */ "memory");
            RDTSC_START(cycles_start);
            avxpackwithoutmask(data,buffer, bit);
            RDTSC_FINAL(cycles_final);
            cycles_diff = (cycles_final - cycles_start);
            if (cycles_diff < min_diff) min_diff = cycles_diff;
        }
        printf("%.2f\t",min_diff*1.0/length);
        min_diff = (uint64_t)-1;
        for (i = 0; i < repeat; i++) {
            uint64_t cycles_start, cycles_final, cycles_diff;
            __asm volatile("" ::: /* pretend to clobber */ "memory");
            RDTSC_START(cycles_start);
            avxunpack(buffer, backdata,bit);
            RDTSC_FINAL(cycles_final);
            cycles_diff = (cycles_final - cycles_start);
            if (cycles_diff < min_diff) min_diff = cycles_diff;
        }
        printf("%.2f\t",min_diff*1.0/length);

        free(data);
        free(buffer);
        free(backdata);
        printf("\n");
    }
    printf("\n\n"); /* two blank lines are required by gnuplot */
}
#endif /* avx 2 */


int main() {
    demo128();
    demo128_d1();
#ifdef __AVX2__
    demo256();
#endif
    return 0;


}
