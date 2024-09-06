#pragma once
#ifndef ATTR_UNUSED
#if defined(__linux__) || defined(__APPLE__)
#define ATTR_UNUSED __attribute__((unused))
#elif defined(WIN32) || defined(_WIN32)
#define ATTR_UNUSED
#endif
#endif
