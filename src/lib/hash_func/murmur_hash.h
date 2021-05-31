//-----------------------------------------------------------------------------
// MurmurHash2, by Austin Appleby
// (public domain, cf. http://murmurhash.googlepages.com/)
 
// Note - This code makes a few assumptions about how your machine behaves -
 
// 1. We can read a 4-byte value from any address without crashing
// 2. sizeof(int) == 4
 
// And it has a few limitations -
 
// 1. It will not work incrementally.
// 2. It will not produce the same results on little-endian and big-endian
//    machines.

#ifndef OCEANBASE_LIB_HASH_MURMURHASH_
#define OCEANBASE_LIB_HASH_MURMURHASH_

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <string>

namespace oceanbase
{
namespace common
{
// The MurmurHash 2 from Austin Appleby, faster and better mixed (but weaker
// crypto-wise with one pair of obvious differential) than both Lookup3 and
// SuperFastHash. Not-endian neutral for speed.
uint32_t murmurhash2(const void *data, int32_t len, uint32_t hash);

// MurmurHash2, 64-bit versions, by Austin Appleby
// The same caveats as 32-bit MurmurHash2 apply here - beware of alignment
// and endian-ness issues if used across multiple platforms.
// 64-bit hash for 64-bit platforms
uint64_t murmurhash64A(const void *key, int32_t len, uint64_t seed);

//public function, please only use this one
inline uint64_t murmurhash(const void *data, int32_t len, uint64_t hash)
{
  return murmurhash64A(data, len, hash);
}

uint32_t fnv_hash2(const void *data, int32_t len, uint32_t hash);
}//namespace common
}//namespace oceanbase
#endif // OCEANBASE_LIB_HASH_MURMURHASH_
