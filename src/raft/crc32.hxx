
#ifndef _JSAHN_CRC32_H
#define _JSAHN_CRC32_H

#include <stdint.h>
#if defined(__linux__) || defined(__APPLE__)
    #include <unistd.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

uint32_t crc32_1(const void* data, size_t len, uint32_t prev_value);
uint32_t crc32_8(const void* data, size_t len, uint32_t prev_value);
uint32_t crc32_8_last8(const void* data, size_t len, uint32_t prev_value);

#ifdef __cplusplus
}
#endif

#endif
