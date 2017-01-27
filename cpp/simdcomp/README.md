The SIMDComp library
====================
[![Build Status](https://travis-ci.org/lemire/simdcomp.png)](https://travis-ci.org/lemire/simdcomp)

A simple C library for compressing lists of integers using binary packing and SIMD instructions.
The assumption is either that you have a list of 32-bit integers where most of them are small, or a list of 32-bit integers where differences between successive integers are small. No software is able to reliably compress an array of 32-bit random numbers.

This library can decode at least 4 billions of compressed integers per second on most
desktop or laptop processors. That is, it can decompress data at a rate of 15 GB/s.
This is significantly faster than generic codecs like gzip, LZO, Snappy or LZ4.

On a Skylake Intel processor, it can decode integers at a rate 0.3 cycles per integer,
which can easily translate into more than 8 decoded billions integers per second.

Contributors: Daniel Lemire, Nathan Kurz, Christoph Rupp, Anatol Belski, Nick White and others

What is it for?
-------------

This is a low-level library for fast integer compression. By design it does not define a compressed
format. It is up to the (sophisticated) user to create a compressed format.

Requirements
-------------

- Your processor should support SSE4.1 (It is supported by most Intel and AMD processors released since 2008.)
- It is possible to build the core part of the code if your processor support SSE2 (Pentium4 or better)
- C99 compliant compiler (GCC is assumed)
- A Linux-like distribution is assumed by the makefile

For a plain C version that does not use SIMD instructions, see https://github.com/lemire/LittleIntPacker

Usage
-------

Compression works over blocks of 128 integers.

For a complete working example, see example.c (you can build it and
run it with "make example; ./example").



1) Lists of integers in random order.

```C            
const uint32_t b = maxbits(datain);// computes bit width
simdpackwithoutmask(datain, buffer, b);//compressed to buffer, compressing 128 32-bit integers down to b*32 bytes
simdunpack(buffer, backbuffer, b);//uncompressed to backbuffer
```

While 128 32-bit integers are read, only b 128-bit words are written. Thus, the compression ratio is 32/b.

2) Sorted lists of integers.

We used differential coding: we store the difference between successive integers. For this purpose, we need an initial value (called offset).

```C            
uint32_t offset = 0;
uint32_t b1 = simdmaxbitsd1(offset,datain); // bit width
simdpackwithoutmaskd1(offset, datain, buffer, b1);//compressing 128 32-bit integers down to b1*32 bytes
simdunpackd1(offset, buffer, backbuffer, b1);//uncompressed
```

General example for arrays of arbitrary length:
```C
int compress_decompress_demo() {
  size_t k, N = 9999;
  __m128i * endofbuf;
  uint32_t * datain = malloc(N * sizeof(uint32_t));
  uint8_t * buffer;
  uint32_t * backbuffer = malloc(N * sizeof(uint32_t));
  uint32_t b;

  for (k = 0; k < N; ++k){        /* start with k=0, not k=1! */
    datain[k] = k;
  }

  b = maxbits_length(datain, N);
  buffer = malloc(simdpack_compressedbytes(N,b)); // allocate just enough memory
  endofbuf = simdpack_length(datain, N, (__m128i *)buffer, b);
  /* compressed data is stored between buffer and endofbuf using (endofbuf-buffer)*sizeof(__m128i) bytes */
  /* would be safe to do : buffer = realloc(buffer,(endofbuf-(__m128i *)buffer)*sizeof(__m128i)); */
  simdunpack_length((const __m128i *)buffer, N, backbuffer, b);

  for (k = 0; k < N; ++k){
    if(datain[k] != backbuffer[k]) {
      printf("bug\n");
      return -1;
    }
  }
  return 0;
}
```


3) Frame-of-Reference 

We also have frame-of-reference (FOR) functions (see simdfor.h header). They work like the bit packing
routines, but do not use differential coding so they allow faster search in some cases, at the expense
of compression.

Setup
---------


make
make test

and if you are daring:

make install

Go
--------

If you are a go user, there is a "go" folder where you will find a simple demo.

Other libraries
----------------

* Fast decoder for VByte-compressed integers https://github.com/lemire/MaskedVByte
* Fast integer compression in C using StreamVByte https://github.com/lemire/streamvbyte
* FastPFOR is a C++ research library well suited to compress unsorted arrays: https://github.com/lemire/FastPFor
* SIMDCompressionAndIntersection is a C++ research library well suited for sorted arrays (differential coding)
and computing intersections: https://github.com/lemire/SIMDCompressionAndIntersection
* TurboPFor is a C library that offers lots of interesting optimizations. Well worth checking! (GPL license) https://github.com/powturbo/TurboPFor
* Oroch is a C++ library that offers a usable API (MIT license) https://github.com/ademakov/Oroch


References
------------

* Daniel Lemire, Leonid Boytsov, Nathan Kurz, SIMD Compression and the Intersection of Sorted Integers, Software Practice & Experience 46 (6) 2016. http://arxiv.org/abs/1401.6399
* Daniel Lemire and Leonid Boytsov, Decoding billions of integers per second through vectorization, Software Practice & Experience 45 (1), 2015.  http://arxiv.org/abs/1209.2137 http://onlinelibrary.wiley.com/doi/10.1002/spe.2203/abstract
* Jeff Plaisance, Nathan Kurz, Daniel Lemire, Vectorized VByte Decoding, International Symposium on Web Algorithms 2015, 2015. http://arxiv.org/abs/1503.07387
* Wayne Xin Zhao, Xudong Zhang, Daniel Lemire, Dongdong Shan, Jian-Yun Nie, Hongfei Yan, Ji-Rong Wen, A General SIMD-based Approach to Accelerating Compression Algorithms, ACM Transactions on Information Systems 33 (3), 2015. http://arxiv.org/abs/1502.01916
* T. D. Wu, Bitpacking techniques for indexing genomes: I. Hash tables, Algorithms for Molecular Biology 11 (5), 2016. http://almob.biomedcentral.com/articles/10.1186/s13015-016-0069-5
