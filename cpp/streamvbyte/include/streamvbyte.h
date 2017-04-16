
#ifndef VARINTDECODE_H_
#define VARINTDECODE_H_
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <stdint.h>// please use a C99-compatible compiler
#include <stddef.h>


// Encode an array of a given length read from in to bout in varint format.
// Returns the number of bytes written.
size_t streamvbyte_encode(const uint32_t *in, uint32_t length, uint8_t *out);

// Read "length" 32-bit integers in varint format from in, storing the result in out.
// Returns the number of bytes read.
size_t streamvbyte_decode(const uint8_t* in, uint32_t* out, uint32_t length);


#endif /* VARINTDECODE_H_ */
