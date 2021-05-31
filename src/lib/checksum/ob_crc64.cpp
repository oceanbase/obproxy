/**
 * Copyright (c) 2021 OceanBase
 * OceanBase Database Proxy(ODP) is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 *
 * **************************************************************
 *
 * ob_crc64.cpp is for what ...
 *
 *   The method to compute the CRC64 is referred to as
 *      CRC-64-ISO:
 *  http://en.wikipedia.org/wiki/Cyclic_redundancy_check The
 *      generator polynomial is x^64 + x^4 + x^3 + x + 1.
 *      Reverse polynom: 0xd800000000000000ULL *
 */

#include <stdlib.h>
#include "lib/checksum/ob_crc64.h"
#include "lib/ob_define.h"

namespace oceanbase
{
namespace common
{

/*******************************************************************************
 *   Global Variables                                                           *
 *******************************************************************************/
/**
  * Lookup table (precomputed CRC64 values for each 8 bit string) computation
  * takes into account the fact that the reverse polynom has zeros in lower 8 bits:
  *
  * @code
  *    for (i = 0; i < 256; i++)
  *    {
  *        shiftRegister = i;
  *        for (j = 0; j < 8; j++)
  *        {
  *            if (shiftRegister & 1)
  *                shiftRegister = (shiftRegister >> 1) ^ Reverse_polynom;
  *            else
  *                shiftRegister >>= 1;
  *        }
  *        CRCTable[i] = shiftRegister;
  *    }
  * @endcode
  *
  */
static const uint64_t CRC64_TABLE_SIZE = 256;
static uint64_t s_crc64_table[CRC64_TABLE_SIZE] = {0};
static uint16_t s_optimized_crc64_table[CRC64_TABLE_SIZE] = {0};

void __attribute__((constructor)) ob_global_init_crc64_table()
{
  ob_init_crc64_table(OB_DEFAULT_CRC64_POLYNOM);
}

void ob_init_crc64_table(const uint64_t polynom)
{
  for (uint64_t i = 0; i < CRC64_TABLE_SIZE; i++) {
    uint64_t shift = i;
    for (uint64_t j = 0; j < 8; j++) {
      if (shift & 1) {
        shift = (shift >> 1) ^ polynom;
      } else {
        shift >>= 1;
      }
    }
    s_crc64_table[i] = shift;
    s_optimized_crc64_table[i] = static_cast<int16_t >((shift >> 48) & 0xffff);
  }
}

/*
static const uint64_t s_crc64_table[] =
{
    0x0000000000000000ULL, 0x01B0000000000000ULL, 0x0360000000000000ULL, 0x02D0000000000000ULL,
    0x06C0000000000000ULL, 0x0770000000000000ULL, 0x05A0000000000000ULL, 0x0410000000000000ULL,
    0x0D80000000000000ULL, 0x0C30000000000000ULL, 0x0EE0000000000000ULL, 0x0F50000000000000ULL,
    0x0B40000000000000ULL, 0x0AF0000000000000ULL, 0x0820000000000000ULL, 0x0990000000000000ULL,
    0x1B00000000000000ULL, 0x1AB0000000000000ULL, 0x1860000000000000ULL, 0x19D0000000000000ULL,
    0x1DC0000000000000ULL, 0x1C70000000000000ULL, 0x1EA0000000000000ULL, 0x1F10000000000000ULL,
    0x1680000000000000ULL, 0x1730000000000000ULL, 0x15E0000000000000ULL, 0x1450000000000000ULL,
    0x1040000000000000ULL, 0x11F0000000000000ULL, 0x1320000000000000ULL, 0x1290000000000000ULL,
    0x3600000000000000ULL, 0x37B0000000000000ULL, 0x3560000000000000ULL, 0x34D0000000000000ULL,
    0x30C0000000000000ULL, 0x3170000000000000ULL, 0x33A0000000000000ULL, 0x3210000000000000ULL,
    0x3B80000000000000ULL, 0x3A30000000000000ULL, 0x38E0000000000000ULL, 0x3950000000000000ULL,
    0x3D40000000000000ULL, 0x3CF0000000000000ULL, 0x3E20000000000000ULL, 0x3F90000000000000ULL,
    0x2D00000000000000ULL, 0x2CB0000000000000ULL, 0x2E60000000000000ULL, 0x2FD0000000000000ULL,
    0x2BC0000000000000ULL, 0x2A70000000000000ULL, 0x28A0000000000000ULL, 0x2910000000000000ULL,
    0x2080000000000000ULL, 0x2130000000000000ULL, 0x23E0000000000000ULL, 0x2250000000000000ULL,
    0x2640000000000000ULL, 0x27F0000000000000ULL, 0x2520000000000000ULL, 0x2490000000000000ULL,
    0x6C00000000000000ULL, 0x6DB0000000000000ULL, 0x6F60000000000000ULL, 0x6ED0000000000000ULL,
    0x6AC0000000000000ULL, 0x6B70000000000000ULL, 0x69A0000000000000ULL, 0x6810000000000000ULL,
    0x6180000000000000ULL, 0x6030000000000000ULL, 0x62E0000000000000ULL, 0x6350000000000000ULL,
    0x6740000000000000ULL, 0x66F0000000000000ULL, 0x6420000000000000ULL, 0x6590000000000000ULL,
    0x7700000000000000ULL, 0x76B0000000000000ULL, 0x7460000000000000ULL, 0x75D0000000000000ULL,
    0x71C0000000000000ULL, 0x7070000000000000ULL, 0x72A0000000000000ULL, 0x7310000000000000ULL,
    0x7A80000000000000ULL, 0x7B30000000000000ULL, 0x79E0000000000000ULL, 0x7850000000000000ULL,
    0x7C40000000000000ULL, 0x7DF0000000000000ULL, 0x7F20000000000000ULL, 0x7E90000000000000ULL,
    0x5A00000000000000ULL, 0x5BB0000000000000ULL, 0x5960000000000000ULL, 0x58D0000000000000ULL,
    0x5CC0000000000000ULL, 0x5D70000000000000ULL, 0x5FA0000000000000ULL, 0x5E10000000000000ULL,
    0x5780000000000000ULL, 0x5630000000000000ULL, 0x54E0000000000000ULL, 0x5550000000000000ULL,
    0x5140000000000000ULL, 0x50F0000000000000ULL, 0x5220000000000000ULL, 0x5390000000000000ULL,
    0x4100000000000000ULL, 0x40B0000000000000ULL, 0x4260000000000000ULL, 0x43D0000000000000ULL,
    0x47C0000000000000ULL, 0x4670000000000000ULL, 0x44A0000000000000ULL, 0x4510000000000000ULL,
    0x4C80000000000000ULL, 0x4D30000000000000ULL, 0x4FE0000000000000ULL, 0x4E50000000000000ULL,
    0x4A40000000000000ULL, 0x4BF0000000000000ULL, 0x4920000000000000ULL, 0x4890000000000000ULL,
    0xD800000000000000ULL, 0xD9B0000000000000ULL, 0xDB60000000000000ULL, 0xDAD0000000000000ULL,
    0xDEC0000000000000ULL, 0xDF70000000000000ULL, 0xDDA0000000000000ULL, 0xDC10000000000000ULL,
    0xD580000000000000ULL, 0xD430000000000000ULL, 0xD6E0000000000000ULL, 0xD750000000000000ULL,
    0xD340000000000000ULL, 0xD2F0000000000000ULL, 0xD020000000000000ULL, 0xD190000000000000ULL,
    0xC300000000000000ULL, 0xC2B0000000000000ULL, 0xC060000000000000ULL, 0xC1D0000000000000ULL,
    0xC5C0000000000000ULL, 0xC470000000000000ULL, 0xC6A0000000000000ULL, 0xC710000000000000ULL,
    0xCE80000000000000ULL, 0xCF30000000000000ULL, 0xCDE0000000000000ULL, 0xCC50000000000000ULL,
    0xC840000000000000ULL, 0xC9F0000000000000ULL, 0xCB20000000000000ULL, 0xCA90000000000000ULL,
    0xEE00000000000000ULL, 0xEFB0000000000000ULL, 0xED60000000000000ULL, 0xECD0000000000000ULL,
    0xE8C0000000000000ULL, 0xE970000000000000ULL, 0xEBA0000000000000ULL, 0xEA10000000000000ULL,
    0xE380000000000000ULL, 0xE230000000000000ULL, 0xE0E0000000000000ULL, 0xE150000000000000ULL,
    0xE540000000000000ULL, 0xE4F0000000000000ULL, 0xE620000000000000ULL, 0xE790000000000000ULL,
    0xF500000000000000ULL, 0xF4B0000000000000ULL, 0xF660000000000000ULL, 0xF7D0000000000000ULL,
    0xF3C0000000000000ULL, 0xF270000000000000ULL, 0xF0A0000000000000ULL, 0xF110000000000000ULL,
    0xF880000000000000ULL, 0xF930000000000000ULL, 0xFBE0000000000000ULL, 0xFA50000000000000ULL,
    0xFE40000000000000ULL, 0xFFF0000000000000ULL, 0xFD20000000000000ULL, 0xFC90000000000000ULL,
    0xB400000000000000ULL, 0xB5B0000000000000ULL, 0xB760000000000000ULL, 0xB6D0000000000000ULL,
    0xB2C0000000000000ULL, 0xB370000000000000ULL, 0xB1A0000000000000ULL, 0xB010000000000000ULL,
    0xB980000000000000ULL, 0xB830000000000000ULL, 0xBAE0000000000000ULL, 0xBB50000000000000ULL,
    0xBF40000000000000ULL, 0xBEF0000000000000ULL, 0xBC20000000000000ULL, 0xBD90000000000000ULL,
    0xAF00000000000000ULL, 0xAEB0000000000000ULL, 0xAC60000000000000ULL, 0xADD0000000000000ULL,
    0xA9C0000000000000ULL, 0xA870000000000000ULL, 0xAAA0000000000000ULL, 0xAB10000000000000ULL,
    0xA280000000000000ULL, 0xA330000000000000ULL, 0xA1E0000000000000ULL, 0xA050000000000000ULL,
    0xA440000000000000ULL, 0xA5F0000000000000ULL, 0xA720000000000000ULL, 0xA690000000000000ULL,
    0x8200000000000000ULL, 0x83B0000000000000ULL, 0x8160000000000000ULL, 0x80D0000000000000ULL,
    0x84C0000000000000ULL, 0x8570000000000000ULL, 0x87A0000000000000ULL, 0x8610000000000000ULL,
    0x8F80000000000000ULL, 0x8E30000000000000ULL, 0x8CE0000000000000ULL, 0x8D50000000000000ULL,
    0x8940000000000000ULL, 0x88F0000000000000ULL, 0x8A20000000000000ULL, 0x8B90000000000000ULL,
    0x9900000000000000ULL, 0x98B0000000000000ULL, 0x9A60000000000000ULL, 0x9BD0000000000000ULL,
    0x9FC0000000000000ULL, 0x9E70000000000000ULL, 0x9CA0000000000000ULL, 0x9D10000000000000ULL,
    0x9480000000000000ULL, 0x9530000000000000ULL, 0x97E0000000000000ULL, 0x9650000000000000ULL,
    0x9240000000000000ULL, 0x93F0000000000000ULL, 0x9120000000000000ULL, 0x9090000000000000ULL
};
//since all low 48 bit of the table element are 0, we can use only the high 16 bit element for optimization
 static const uint16_t s_optimized_crc64_table[] =
 {
     0x0000, 0x01b0, 0x0360, 0x02d0,
     0x06c0, 0x0770, 0x05a0, 0x0410,
     0x0d80, 0x0c30, 0x0ee0, 0x0f50,
     0x0b40, 0x0af0, 0x0820, 0x0990,
     0x1b00, 0x1ab0, 0x1860, 0x19d0,
     0x1dc0, 0x1c70, 0x1ea0, 0x1f10,
     0x1680, 0x1730, 0x15e0, 0x1450,
     0x1040, 0x11f0, 0x1320, 0x1290,
     0x3600, 0x37b0, 0x3560, 0x34d0,
     0x30c0, 0x3170, 0x33a0, 0x3210,
     0x3b80, 0x3a30, 0x38e0, 0x3950,
     0x3d40, 0x3cf0, 0x3e20, 0x3f90,
     0x2d00, 0x2cb0, 0x2e60, 0x2fd0,
     0x2bc0, 0x2a70, 0x28a0, 0x2910,
     0x2080, 0x2130, 0x23e0, 0x2250,
     0x2640, 0x27f0, 0x2520, 0x2490,
     0x6c00, 0x6db0, 0x6f60, 0x6ed0,
     0x6ac0, 0x6b70, 0x69a0, 0x6810,
     0x6180, 0x6030, 0x62e0, 0x6350,
     0x6740, 0x66f0, 0x6420, 0x6590,
     0x7700, 0x76b0, 0x7460, 0x75d0,
     0x71c0, 0x7070, 0x72a0, 0x7310,
     0x7a80, 0x7b30, 0x79e0, 0x7850,
     0x7c40, 0x7df0, 0x7f20, 0x7e90,
     0x5a00, 0x5bb0, 0x5960, 0x58d0,
     0x5cc0, 0x5d70, 0x5fa0, 0x5e10,
     0x5780, 0x5630, 0x54e0, 0x5550,
     0x5140, 0x50f0, 0x5220, 0x5390,
     0x4100, 0x40b0, 0x4260, 0x43d0,
     0x47c0, 0x4670, 0x44a0, 0x4510,
     0x4c80, 0x4d30, 0x4fe0, 0x4e50,
     0x4a40, 0x4bf0, 0x4920, 0x4890,
     0xd800, 0xd9b0, 0xdb60, 0xdad0,
     0xdec0, 0xdf70, 0xdda0, 0xdc10,
     0xd580, 0xd430, 0xd6e0, 0xd750,
     0xd340, 0xd2f0, 0xd020, 0xd190,
     0xc300, 0xc2b0, 0xc060, 0xc1d0,
     0xc5c0, 0xc470, 0xc6a0, 0xc710,
     0xce80, 0xcf30, 0xcde0, 0xcc50,
     0xc840, 0xc9f0, 0xcb20, 0xca90,
     0xee00, 0xefb0, 0xed60, 0xecd0,
     0xe8c0, 0xe970, 0xeba0, 0xea10,
     0xe380, 0xe230, 0xe0e0, 0xe150,
     0xe540, 0xe4f0, 0xe620, 0xe790,
     0xf500, 0xf4b0, 0xf660, 0xf7d0,
     0xf3c0, 0xf270, 0xf0a0, 0xf110,
     0xf880, 0xf930, 0xfbe0, 0xfa50,
     0xfe40, 0xfff0, 0xfd20, 0xfc90,
     0xb400, 0xb5b0, 0xb760, 0xb6d0,
     0xb2c0, 0xb370, 0xb1a0, 0xb010,
     0xb980, 0xb830, 0xbae0, 0xbb50,
     0xbf40, 0xbef0, 0xbc20, 0xbd90,
     0xaf00, 0xaeb0, 0xac60, 0xadd0,
     0xa9c0, 0xa870, 0xaaa0, 0xab10,
     0xa280, 0xa330, 0xa1e0, 0xa050,
     0xa440, 0xa5f0, 0xa720, 0xa690,
     0x8200, 0x83b0, 0x8160, 0x80d0,
     0x84c0, 0x8570, 0x87a0, 0x8610,
     0x8f80, 0x8e30, 0x8ce0, 0x8d50,
     0x8940, 0x88f0, 0x8a20, 0x8b90,
     0x9900, 0x98b0, 0x9a60, 0x9bd0,
     0x9fc0, 0x9e70, 0x9ca0, 0x9d10,
     0x9480, 0x9530, 0x97e0, 0x9650,
     0x9240, 0x93f0, 0x9120, 0x9090
};
*/

#define DO_1_STEP(uCRC64, pu8) uCRC64 = s_crc64_table[(uCRC64 ^ *pu8++) & 0xff] ^ (uCRC64 >> 8);
#define DO_2_STEP(uCRC64, pu8)  DO_1_STEP(uCRC64, pu8); DO_1_STEP(uCRC64, pu8);
#define DO_4_STEP(uCRC64, pu8)  DO_2_STEP(uCRC64, pu8); DO_2_STEP(uCRC64, pu8);
#define DO_8_STEP(uCRC64, pu8)  DO_4_STEP(uCRC64, pu8); DO_4_STEP(uCRC64, pu8);
#define DO_16_STEP(uCRC64, pu8)  DO_8_STEP(uCRC64, pu8); DO_8_STEP(uCRC64, pu8);

#ifdef __x86_64__
#define DO_1_OPTIMIZED_STEP(uCRC64, pu8)  \
  __asm__ __volatile__(   \
                          "movq (%1),%%mm0\n\t"  \
                          "pxor  %0, %%mm0\n\t"   \
                          "pextrw $0, %%mm0, %%eax\n\t"   \
                          "movzbq %%al, %%r8\n\t" \
                          "movzwl (%2,%%r8,2), %%ecx\n\t"        \
                          "pinsrw $0, %%ecx, %%mm3\n\t"      \
                          "pextrw $3, %%mm0, %%ebx\n\t"   \
                          "xorb %%bh, %%cl\n\t" \
                          "movzbq %%cl, %%r8\n\t" \
                          "movzwl (%2,%%r8,2), %%edx\n\t" \
                          "pinsrw $3, %%edx, %%mm4\n\t"    \
                          "movb %%ah, %%al\n\t"   \
                          "movzbq %%al, %%r8\n\t" \
                          "movzwl (%2, %%r8,2), %%ecx\n\t"    \
                          "pinsrw $0, %%ecx, %%mm4\n\t"    \
                          "movzbq %%bl, %%r8\n\t" \
                          "movzwl (%2, %%r8,2), %%edx\n\t"       \
                          "pinsrw $3, %%edx, %%mm3\n\t"    \
                          "pextrw $1, %%mm0, %%eax\n\t"   \
                          "movzbq %%al, %%r8\n\t" \
                          "movzwl (%2, %%r8,2), %%ecx\n\t" \
                          "pinsrw $1, %%ecx, %%mm3\n\t"    \
                          "movb %%ah, %%al\n\t"   \
                          "movzbq %%al, %%r8\n\t" \
                          "movzwl (%2, %%r8,2), %%edx\n\t" \
                          "pinsrw $1, %%edx, %%mm4\n\t"    \
                          "pextrw $2, %%mm0, %%ebx\n\t"   \
                          "movzbq %%bl, %%r8\n\t" \
                          "movzwl (%2, %%r8,2), %%ecx\n\t" \
                          "pinsrw $2, %%ecx, %%mm3\n\t"    \
                          "movb %%bh, %%al\n\t"   \
                          "movzbq %%al, %%r8\n\t" \
                          "movzwl (%2, %%r8,2), %%edx\n\t" \
                          "pinsrw $2, %%edx, %%mm4\n\t"    \
                          "psrlq $8, %%mm3\n\t" \
                          "pxor %%mm3, %%mm4\n\t" \
                          "movq %%mm4, %0\n\t"    \
                          :"+&y" (uCRC64) \
                          :"r"(pu8),"D"(s_optimized_crc64_table)   \
                          :"eax","ebx","ecx","edx","mm0","mm3","mm4","r8");
#else
#define DO_1_OPTIMIZED_STEP(uCRC64, pu8, u_data, operand1, operand2, table_result0)  \
  u_data = uCRC64 ^ (*(uint64_t*)pu8); \
  table_result0 = s_optimized_crc64_table[(uint8_t)u_data];    \
  operand1 = ((uint64_t)s_optimized_crc64_table[(uint8_t)(u_data >> 48)] << 40)     \
             |((uint64_t)s_optimized_crc64_table[(uint8_t)(u_data >> 32)] << 24)   \
             | ((uint64_t)s_optimized_crc64_table[(uint8_t)(u_data >> 16)] << 8)   \
             |( (uint64_t)table_result0 >> 8 );   \
  operand2 = ((uint64_t)s_optimized_crc64_table[(uint8_t)(u_data >> 56) ^ ((uint8_t)table_result0)] << 48)   \
             |((uint64_t)s_optimized_crc64_table[(uint8_t)(u_data >> 40)] << 32)   \
             | ((uint64_t)s_optimized_crc64_table[(uint8_t)(u_data >> 24)] << 16)  \
             |(s_optimized_crc64_table[(uint8_t)(u_data >> 8)]);  \
  uCRC64 = operand1 ^ operand2;
#endif
/**
  * Processes a multiblock of a CRC64 calculation.
  *
  * @returns Intermediate CRC64 value.
  * @param   uCRC64  Current CRC64 intermediate value.
  * @param   pv      The data block to process.
  * @param   cb      The size of the data block in bytes.
  */
uint64_t ob_crc64_optimized(uint64_t uCRC64, const void *pv, int64_t cb)
{
  const uint8_t *pu8 = (const uint8_t *)pv;
#ifndef __x86_64__
  uint64_t u_data = 0, operand1 = 0, operand2 = 0;
  uint16_t table_result0 = 0;
#endif

  if (pv != NULL && cb != 0) {
    while (cb >= 64) {
#ifdef __x86_64__
      DO_1_OPTIMIZED_STEP(uCRC64, pu8);
      pu8 += 8;
      DO_1_OPTIMIZED_STEP(uCRC64, pu8);
      pu8 += 8;
      DO_1_OPTIMIZED_STEP(uCRC64, pu8);
      pu8 += 8;
      DO_1_OPTIMIZED_STEP(uCRC64, pu8);
      pu8 += 8;
      DO_1_OPTIMIZED_STEP(uCRC64, pu8);
      pu8 += 8;
      DO_1_OPTIMIZED_STEP(uCRC64, pu8);
      pu8 += 8;
      DO_1_OPTIMIZED_STEP(uCRC64, pu8);
      pu8 += 8;
      DO_1_OPTIMIZED_STEP(uCRC64, pu8);
      pu8 += 8;

#else
      DO_1_OPTIMIZED_STEP(uCRC64, pu8, u_data, operand1, operand2, table_result0);
      pu8 += 8;
      DO_1_OPTIMIZED_STEP(uCRC64, pu8, u_data, operand1, operand2, table_result0);
      pu8 += 8;
      DO_1_OPTIMIZED_STEP(uCRC64, pu8, u_data, operand1, operand2, table_result0);
      pu8 += 8;
      DO_1_OPTIMIZED_STEP(uCRC64, pu8, u_data, operand1, operand2, table_result0);
      pu8 += 8;
      DO_1_OPTIMIZED_STEP(uCRC64, pu8, u_data, operand1, operand2, table_result0);
      pu8 += 8;
      DO_1_OPTIMIZED_STEP(uCRC64, pu8, u_data, operand1, operand2, table_result0);
      pu8 += 8;
      DO_1_OPTIMIZED_STEP(uCRC64, pu8, u_data, operand1, operand2, table_result0);
      pu8 += 8;
      DO_1_OPTIMIZED_STEP(uCRC64, pu8, u_data, operand1, operand2, table_result0);
      pu8 += 8;
#endif
      cb -= 64;
    }
#ifdef __x86_64__
    __asm__ __volatile__(
        "emms"
    );
#endif
    while (cb--) {
      DO_1_STEP(uCRC64, pu8);
    }
  }

  return uCRC64;
}

/**
  * Processes a multiblock of a CRC64 calculation.
  *
  * @returns Intermediate CRC64 value.
  * @param   uCRC64  Current CRC64 intermediate value.
  * @param   pv      The data block to process.
  * @param   cb      The size of the data block in bytes.
  */
uint64_t ob_crc64(uint64_t uCRC64, const void *pv, int64_t cb)
{
  return ob_crc64_sse42(uCRC64, pv, cb);
}

/**
  * Calculate CRC64 for a memory block.
  *
  * @returns CRC64 for the memory block.
  * @param   pv      Pointer to the memory block.
  * @param   cb      Size of the memory block in bytes.
  */
uint64_t ob_crc64(const void *pv, int64_t cb)
{
  uint64_t  uCRC64 = 0ULL;
  return ob_crc64(uCRC64, pv, cb);
}

/**
  * Get the static CRC64 table. This function is only used for testing purpose.
  *
  */
const uint64_t *ob_get_crc64_table()
{
  return s_crc64_table;
}

/**
 * CRC32 implementation from Facebook, based on the zlib
 * implementation.
 *
 * The below CRC32 implementation is based on the implementation
 * included with zlib with modifications to process 8 bytes at a
 * time and using SSE 4.2 extentions when available.  The
 * polynomial constant has been changed to match the one used by
 * SSE 4.2 and does not return the same value as the version
 * used by zlib.  This implementation only supports 64-bit
 * little-endian processors.  The original zlib copyright notice
 * follows.
 *
 * crc32.c -- compute the CRC-32 of a buf stream Copyright (C)
 * 1995-2005 Mark Adler For conditions of distribution and use,
 * see copyright notice in zlib.h
 *
 * Thanks to Rodney Brown <rbrown64@csc.com.au> for his
 * contribution of faster CRC methods: exclusive-oring 32 bits
 * of buf at a time, and pre-computing tables for updating the
 * shift register in one step with three exclusive-ors instead
 * of four steps with four exclusive-ors.  This results in about
 * a factor of two increase in speed on a Power PC G4 (PPC7455)
 * using gcc -O3.
 *
 * this code copy from:
 * http://bazaar.launchpad.net/~mysql/mysql-server/5.6/view/head:/storage/innobase/ut/ut0crc32.cc
 */
#if defined(__GNUC__) && defined(__x86_64__)
/* opcodes taken from objdump of "crc32b (%%rdx), %%rcx"
for RHEL4 support (GCC 3 doesn't support this instruction) */
#define crc32_sse42_byte \
  asm(".byte 0xf2, 0x48, 0x0f, 0x38, 0xf0, 0x0a" \
      : "=c"(crc) : "c"(crc), "d"(buf)); \
  len--, buf++

/* opcodes taken from objdump of "crc32q (%%rdx), %%rcx"
for RHEL4 support (GCC 3 doesn't support this instruction) */
#define crc32_sse42_quadword \
  asm(".byte 0xf2, 0x48, 0x0f, 0x38, 0xf1, 0x0a" \
      : "=c"(crc) : "c"(crc), "d"(buf)); \
  len -= 8, buf += 8
#endif /* defined(__GNUC__) && defined(__x86_64__) */

/*
inline static uint64_t crc64_sse42(uint64_t uCRC64,
                                   const char *buf, int64_t len)
{
  uint64_t crc = uCRC64;

  if (NULL != buf && len > 0) {
    while (len && ((uint64_t) buf & 7)) {
      crc32_sse42_byte;
    }

    while (len >= 32) {
      crc32_sse42_quadword;
      crc32_sse42_quadword;
      crc32_sse42_quadword;
      crc32_sse42_quadword;
    }

    while (len >= 8) {
      crc32_sse42_quadword;
    }

    while (len) {
      crc32_sse42_byte;
    }
  }

  return crc;
}
*/

static uint64_t crc64_sse42_manually(uint64_t crc, const char *buf, int64_t len)
{
  /**
   * crc32tab is generated by:
   *   // bit-reversed poly 0x1EDC6F41
   *   const uint32_t poly = 0x82f63b78;
   *   for (int n = 0; n < 256; n++) {
   *       uint32_t c = (uint32_t)n;
   *       for (int k = 0; k < 8; k++)
   *           c = c & 1 ? poly ^ (c >> 1) : c >> 1;
   *       crc32tab[n] = c;
   *   }
   */
  const static uint32_t crc32tab[] =
  {
    0x00000000L, 0xf26b8303L, 0xe13b70f7L, 0x1350f3f4L, 0xc79a971fL,
    0x35f1141cL, 0x26a1e7e8L, 0xd4ca64ebL, 0x8ad958cfL, 0x78b2dbccL,
    0x6be22838L, 0x9989ab3bL, 0x4d43cfd0L, 0xbf284cd3L, 0xac78bf27L,
    0x5e133c24L, 0x105ec76fL, 0xe235446cL, 0xf165b798L, 0x030e349bL,
    0xd7c45070L, 0x25afd373L, 0x36ff2087L, 0xc494a384L, 0x9a879fa0L,
    0x68ec1ca3L, 0x7bbcef57L, 0x89d76c54L, 0x5d1d08bfL, 0xaf768bbcL,
    0xbc267848L, 0x4e4dfb4bL, 0x20bd8edeL, 0xd2d60dddL, 0xc186fe29L,
    0x33ed7d2aL, 0xe72719c1L, 0x154c9ac2L, 0x061c6936L, 0xf477ea35L,
    0xaa64d611L, 0x580f5512L, 0x4b5fa6e6L, 0xb93425e5L, 0x6dfe410eL,
    0x9f95c20dL, 0x8cc531f9L, 0x7eaeb2faL, 0x30e349b1L, 0xc288cab2L,
    0xd1d83946L, 0x23b3ba45L, 0xf779deaeL, 0x05125dadL, 0x1642ae59L,
    0xe4292d5aL, 0xba3a117eL, 0x4851927dL, 0x5b016189L, 0xa96ae28aL,
    0x7da08661L, 0x8fcb0562L, 0x9c9bf696L, 0x6ef07595L, 0x417b1dbcL,
    0xb3109ebfL, 0xa0406d4bL, 0x522bee48L, 0x86e18aa3L, 0x748a09a0L,
    0x67dafa54L, 0x95b17957L, 0xcba24573L, 0x39c9c670L, 0x2a993584L,
    0xd8f2b687L, 0x0c38d26cL, 0xfe53516fL, 0xed03a29bL, 0x1f682198L,
    0x5125dad3L, 0xa34e59d0L, 0xb01eaa24L, 0x42752927L, 0x96bf4dccL,
    0x64d4cecfL, 0x77843d3bL, 0x85efbe38L, 0xdbfc821cL, 0x2997011fL,
    0x3ac7f2ebL, 0xc8ac71e8L, 0x1c661503L, 0xee0d9600L, 0xfd5d65f4L,
    0x0f36e6f7L, 0x61c69362L, 0x93ad1061L, 0x80fde395L, 0x72966096L,
    0xa65c047dL, 0x5437877eL, 0x4767748aL, 0xb50cf789L, 0xeb1fcbadL,
    0x197448aeL, 0x0a24bb5aL, 0xf84f3859L, 0x2c855cb2L, 0xdeeedfb1L,
    0xcdbe2c45L, 0x3fd5af46L, 0x7198540dL, 0x83f3d70eL, 0x90a324faL,
    0x62c8a7f9L, 0xb602c312L, 0x44694011L, 0x5739b3e5L, 0xa55230e6L,
    0xfb410cc2L, 0x092a8fc1L, 0x1a7a7c35L, 0xe811ff36L, 0x3cdb9bddL,
    0xceb018deL, 0xdde0eb2aL, 0x2f8b6829L, 0x82f63b78L, 0x709db87bL,
    0x63cd4b8fL, 0x91a6c88cL, 0x456cac67L, 0xb7072f64L, 0xa457dc90L,
    0x563c5f93L, 0x082f63b7L, 0xfa44e0b4L, 0xe9141340L, 0x1b7f9043L,
    0xcfb5f4a8L, 0x3dde77abL, 0x2e8e845fL, 0xdce5075cL, 0x92a8fc17L,
    0x60c37f14L, 0x73938ce0L, 0x81f80fe3L, 0x55326b08L, 0xa759e80bL,
    0xb4091bffL, 0x466298fcL, 0x1871a4d8L, 0xea1a27dbL, 0xf94ad42fL,
    0x0b21572cL, 0xdfeb33c7L, 0x2d80b0c4L, 0x3ed04330L, 0xccbbc033L,
    0xa24bb5a6L, 0x502036a5L, 0x4370c551L, 0xb11b4652L, 0x65d122b9L,
    0x97baa1baL, 0x84ea524eL, 0x7681d14dL, 0x2892ed69L, 0xdaf96e6aL,
    0xc9a99d9eL, 0x3bc21e9dL, 0xef087a76L, 0x1d63f975L, 0x0e330a81L,
    0xfc588982L, 0xb21572c9L, 0x407ef1caL, 0x532e023eL, 0xa145813dL,
    0x758fe5d6L, 0x87e466d5L, 0x94b49521L, 0x66df1622L, 0x38cc2a06L,
    0xcaa7a905L, 0xd9f75af1L, 0x2b9cd9f2L, 0xff56bd19L, 0x0d3d3e1aL,
    0x1e6dcdeeL, 0xec064eedL, 0xc38d26c4L, 0x31e6a5c7L, 0x22b65633L,
    0xd0ddd530L, 0x0417b1dbL, 0xf67c32d8L, 0xe52cc12cL, 0x1747422fL,
    0x49547e0bL, 0xbb3ffd08L, 0xa86f0efcL, 0x5a048dffL, 0x8ecee914L,
    0x7ca56a17L, 0x6ff599e3L, 0x9d9e1ae0L, 0xd3d3e1abL, 0x21b862a8L,
    0x32e8915cL, 0xc083125fL, 0x144976b4L, 0xe622f5b7L, 0xf5720643L,
    0x07198540L, 0x590ab964L, 0xab613a67L, 0xb831c993L, 0x4a5a4a90L,
    0x9e902e7bL, 0x6cfbad78L, 0x7fab5e8cL, 0x8dc0dd8fL, 0xe330a81aL,
    0x115b2b19L, 0x020bd8edL, 0xf0605beeL, 0x24aa3f05L, 0xd6c1bc06L,
    0xc5914ff2L, 0x37faccf1L, 0x69e9f0d5L, 0x9b8273d6L, 0x88d28022L,
    0x7ab90321L, 0xae7367caL, 0x5c18e4c9L, 0x4f48173dL, 0xbd23943eL,
    0xf36e6f75L, 0x0105ec76L, 0x12551f82L, 0xe03e9c81L, 0x34f4f86aL,
    0xc69f7b69L, 0xd5cf889dL, 0x27a40b9eL, 0x79b737baL, 0x8bdcb4b9L,
    0x988c474dL, 0x6ae7c44eL, 0xbe2da0a5L, 0x4c4623a6L, 0x5f16d052L,
    0xad7d5351L
  };

  for (int64_t i = 0; i < len; ++i) {
    crc = crc32tab[(crc ^ buf[i]) & 0xff] ^ (crc >> 8);
  }

  return crc;
}

uint64_t crc64_sse42_dispatch(uint64_t crc, const char *buf, int64_t len)
{
/*
  uint32_t a = 0;
  uint32_t b = 0;
  uint32_t c = 0;
  uint32_t d = 0;
  asm("cpuid": "=a"(a), "=b"(b), "=c"(c), "=d"(d) : "0"(1));
  if (c & (1 << 20)) {
    ob_crc64_sse42_func = &crc64_sse42;
    _OB_LOG(INFO, "Use CPU crc32 instructs for crc64 calculate");
  } else {
*/
    ob_crc64_sse42_func = &crc64_sse42_manually;
    _OB_LOG(INFO, "Use manual crc32 table lookup for crc64 calculate");
//  }
  return (*ob_crc64_sse42_func)(crc, buf, len);
}

ObCRC64Func ob_crc64_sse42_func = &crc64_sse42_dispatch;
}
}

