/*
 * streamvbytedelta.h
 *
 *  Created on: Apr 14, 2016
 *      Author: lemire
 */

#ifndef INCLUDE_STREAMVBYTEDELTA_H_
#define INCLUDE_STREAMVBYTEDELTA_H_


// Encode an array of a given length read from in to bout in StreamVByte format.
// Returns the number of bytes written.
// this version uses differential coding (coding differences between values) starting at prev (you can often set prev to zero)
size_t streamvbyte_delta_encode(uint32_t *in, uint32_t length, uint8_t *out, uint32_t  prev);

// Read "length" 32-bit integers in StreamVByte format from in, storing the result in out.
// Returns the number of bytes read.
// this version uses differential coding (coding differences between values) starting at prev (you can often set prev to zero)
size_t streamvbyte_delta_decode(const uint8_t* in, uint32_t* out, uint32_t length, uint32_t  prev);



#endif /* INCLUDE_STREAMVBYTEDELTA_H_ */
