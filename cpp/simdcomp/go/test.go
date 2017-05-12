/////////
// This particular file is in the public domain.
// Author: Daniel Lemire
////////

package main 

/*
#cgo LDFLAGS: -lsimdcomp
#include <simdcomp.h>
*/
import "C"
import "fmt"

//////////
// For this demo, we pack and unpack blocks of 128 integers
/////////
func main() {
        // I am going to use C types. Alternative might be to use unsafe.Pointer calls, see http://bit.ly/1ndw3W3
        // this is our original data
        var data [128]C.uint32_t
        for i := C.uint32_t(0); i < C.uint32_t(128); i++ {
            data[i] = i
        }





        ////////////
        // We first pack without differential coding
        ///////////
        // computing how many bits per int. is needed
        b  := C.maxbits(&data[0])
        ratio := 32.0/float64(b)
        fmt.Println("Bit width  ", b)
        fmt.Println(fmt.Sprintf("Compression ratio %f ", ratio))
         // we are now going to create a buffer to receive the packed data (each __m128i uses 128 bits)
        out := make([] C.__m128i,b)       
        C.simdpackwithoutmask( &data[0],&out[0],b);
        var recovereddata [128]C.uint32_t
        C.simdunpack(&out[0],&recovereddata[0],b)
        for i := 0; i < 128; i++ {
            if data[i] != recovereddata[i]  {
                  fmt.Println("Bug ")
                  return
            }
        } 

        ///////////
        // Next, we use differential coding
        //////////
        offset := C.uint32_t(0) // if you pack data from K to K + 128, offset should be the value at K-1. When K = 0, choose a default
        b1  := C.simdmaxbitsd1(offset,&data[0])
        ratio1 := 32.0/float64(b1)
        fmt.Println("Bit width  ", b1)
        fmt.Println(fmt.Sprintf("Compression ratio %f ", ratio1))
         // we are now going to create a buffer to receive the packed data (each __m128i uses 128 bits)
        out = make([] C.__m128i,b1)       
        C.simdpackwithoutmaskd1(offset, &data[0],&out[0],b1);
        C.simdunpackd1(offset,&out[0],&recovereddata[0],b1)
        for i := 0; i < 128; i++ {
            if data[i] != recovereddata[i]  {
                  fmt.Println("Bug ")
                  return
            }
        } 

        fmt.Println("test succesful.")
      
}
