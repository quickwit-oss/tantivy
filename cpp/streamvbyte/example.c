#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include "streamvbyte.h"

int main() {
	int N = 5000;
	uint32_t * datain = malloc(N * sizeof(uint32_t));
	uint8_t * compressedbuffer = malloc(N * sizeof(uint32_t));
	uint32_t * recovdata = malloc(N * sizeof(uint32_t));
	for (int k = 0; k < N; ++k)
		datain[k] = 120;
	size_t compsize = streamvbyte_encode(datain, N, compressedbuffer); // encoding
	// here the result is stored in compressedbuffer using compsize bytes
	size_t compsize2 = streamvbyte_decode(compressedbuffer, recovdata,
					N); // decoding (fast)
	assert(compsize == compsize2);
	free(datain);
	free(compressedbuffer);
	free(recovdata);
	printf("Compressed %d integers down to %d bytes.\n",N,(int) compsize);
	return 0;
}
