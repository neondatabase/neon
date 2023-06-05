#ifdef __x86_64__

#include <immintrin.h>

__attribute__((target("avx2")))
dist_t fstdistfunc_avx2(const coord_t *x, const coord_t *y, size_t n)
{
    float PORTABLE_ALIGN32 TmpRes[8];
    size_t qty16 = n / 16;
    const float *pEnd1 = x + (qty16 * 16);
    __m256 diff, v1, v2;
    __m256 sum = _mm256_set1_ps(0);

    while (x < pEnd1) {
        v1 = _mm256_loadu_ps(x);
        x += 8;
        v2 = _mm256_loadu_ps(y);
        y += 8;
        diff = _mm256_sub_ps(v1, v2);
        sum = _mm256_add_ps(sum, _mm256_mul_ps(diff, diff));

        v1 = _mm256_loadu_ps(x);
        x += 8;
        v2 = _mm256_loadu_ps(y);
        y += 8;
        diff = _mm256_sub_ps(v1, v2);
        sum = _mm256_add_ps(sum, _mm256_mul_ps(diff, diff));
    }
    _mm256_store_ps(TmpRes, sum);
    float res = TmpRes[0] + TmpRes[1] + TmpRes[2] + TmpRes[3] + TmpRes[4] + TmpRes[5] + TmpRes[6] + TmpRes[7];
    return (res);
}

#endif
