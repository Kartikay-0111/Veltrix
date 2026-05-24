#include "hdr_histogram.hpp"
// HdrHistogram is implemented entirely in the header (inline methods).
// This translation unit exists so CMake has a .cpp file to compile.
// The constexpr array definition must live here to satisfy the ODR.

constexpr double HdrHistogram::BOUNDARIES[HdrHistogram::NUM_BUCKETS + 1];