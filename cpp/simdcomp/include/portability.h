/**
 * This code is released under a BSD License.
 */
#ifndef SIMDBITCOMPAT_H_
#define SIMDBITCOMPAT_H_

#include <iso646.h> /* mostly for Microsoft compilers */
#include <string.h>

#if SIMDCOMP_DEBUG
# define SIMDCOMP_ALWAYS_INLINE inline
# define SIMDCOMP_NEVER_INLINE
# define SIMDCOMP_PURE
#else
# if defined(__GNUC__)
#  if __GNUC__ >= 3
#   define SIMDCOMP_ALWAYS_INLINE inline __attribute__((always_inline))
#   define SIMDCOMP_NEVER_INLINE __attribute__((noinline))
#   define SIMDCOMP_PURE __attribute__((pure))
#  else
#   define SIMDCOMP_ALWAYS_INLINE inline
#   define SIMDCOMP_NEVER_INLINE
#   define SIMDCOMP_PURE
#  endif
# elif defined(_MSC_VER)
#  define SIMDCOMP_ALWAYS_INLINE __forceinline
#  define SIMDCOMP_NEVER_INLINE
#  define SIMDCOMP_PURE
# else
#  if __has_attribute(always_inline)
#   define SIMDCOMP_ALWAYS_INLINE inline __attribute__((always_inline))
#  else
#   define SIMDCOMP_ALWAYS_INLINE inline
#  endif
#  if __has_attribute(noinline)
#   define SIMDCOMP_NEVER_INLINE __attribute__((noinline))
#  else
#   define SIMDCOMP_NEVER_INLINE
#  endif
#  if __has_attribute(pure)
#   define SIMDCOMP_PURE __attribute__((pure))
#  else
#   define SIMDCOMP_PURE
#  endif
# endif
#endif

#if defined(_MSC_VER) && _MSC_VER < 1600
typedef unsigned int uint32_t;
typedef unsigned char uint8_t;
typedef signed char int8_t;
#else
#include <stdint.h> /* part of Visual Studio 2010 and better, others likely anyway */
#endif

#if defined(_MSC_VER)
#define SIMDCOMP_ALIGNED(x) __declspec(align(x))
#else
#if defined(__GNUC__)
#define SIMDCOMP_ALIGNED(x) __attribute__ ((aligned(x)))
#endif
#endif

#if defined(_MSC_VER)
# include <intrin.h>
/* 64-bit needs extending */
# define SIMDCOMP_CTZ(result, mask) do { \
		unsigned long index; \
		if (!_BitScanForward(&(index), (mask))) { \
			(result) = 32U; \
		} else { \
			(result) = (uint32_t)(index); \
		} \
	} while (0)
#else
# define SIMDCOMP_CTZ(result, mask) \
	result = __builtin_ctz(mask)
#endif

#endif /* SIMDBITCOMPAT_H_ */

