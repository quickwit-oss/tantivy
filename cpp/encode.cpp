

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
       uint32_t* output) {
        size_t output_length = 10000;
        codec -> encodeArray(begin,
                          num_els,
                          output,
                          output_length);

        return output_length;
        //
        // if desired, shrink back the array:
        //compressed_output.resize(compressedsize);
        // compressed_output.shrink_to_fit();
        // display compression rate:
        // cout << setprecision(3);
        // cout << "You are using " << 32.0 * static_cast<double>(compressed_output.size()) /
        //      static_cast<double>(mydata.size()) << " bits per integer. " << endl;
        // //
        // You are done!... with the compression...
        //
        ///
        // // decompressing is also easy:
        // //
        // vector<uint32_t> mydataback(N);
        // size_t recoveredsize = mydataback.size();
        // //
        // codec.decodeArray(compressed_output.data(),
        //                   compressed_output.size(), mydataback.data(), recoveredsize);
        // mydataback.resize(recoveredsize);
        // //
        // // That's it for compression!
        // //
        // if (mydataback != mydata) throw runtime_error("bug!");
        //
        // //
        // // Next we are going to test out intersection...
        // //
        // vector<uint32_t> mydata2(N);
        // for (uint32_t i = 0; i < N; ++i) mydata2[i] = 6 * i;
        // intersectionfunction  inter = IntersectionFactory::getFromName("simd");// using SIMD intersection
        // //
        // // we are going to intersect mydata and mydata2 and write back
        // the result to mydata2
        //
        // size_t intersize = inter(mydata2.data(), mydata2.size(), mydata.data(), mydata.size(), mydata2.data());
        // mydata2.resize(intersize);
        // mydata2.shrink_to_fit();
        // cout << "Intersection size: " << mydata2.size() << "  integers. " << endl;
        // return mydata2.size();
  }
}
