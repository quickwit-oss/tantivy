

// /usr/bin/c++    -Wall -Wcast-align -O3 -DNDEBUG -std=c++11 -DHAVE_CXX0X -msse4.1 -march=native -isysroot /Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX10.9.sdk -I/Users/pmasurel/github/FastPFor/headers    -o CMakeFiles/example.dir/example.cpp.o -c /Users/pmasurel/github/FastPFor/example.cpp

#include <iostream>
#include <stdint.h>


#include "codecfactory.h"
#include "intersection.h"

using namespace SIMDCompressionLib;

static shared_ptr<IntegerCODEC> codec =  CODECFactory::getFromName("s4-bp128-dm");


extern "C" {




  size_t encode_native(
       uint32_t* begin,
       const size_t num_els,
       uint32_t* output,
       const size_t output_capacity) {
        size_t output_length = output_capacity;
        codec -> encodeArray(begin,
                          num_els,
                          output,
                          output_length);
        return output_length;
  }

  size_t decode_native(
      const uint32_t* compressed_data,
      const size_t compressed_size,
      uint32_t* uncompressed,
      const size_t uncompressed_capacity) {
        size_t num_ints = uncompressed_capacity;
        codec -> decodeArray(compressed_data, compressed_size, uncompressed, num_ints);
        return num_ints;
  }
}
