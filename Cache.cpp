//
// Created by pengyibo on 2019-08-26.
//

#include "Cache.h"

// therad safe,需要在锁的保护下
void PageCache::updateVersion(){
    /*static constexpr uint32_t MASK = (1L << 32u) - 1;
    uint32_t inc = version & MASK;
    version = (r << 32u) + inc;
    */
     ++version;
}

PageCache* PageCache::allocPageCache(){
    // 分配指定大小的pageCache, 目前需要的page至少有100万个
    // 后续可以按照使用频率模拟LRU然后随机进行sort后来选择

    static constexpr uint32_t SIZE = 1u << 20; // 1048576
    static constexpr uint32_t MASK = SIZE - 1;
    static std::vector<PageCache> pool(SIZE); //给定100 + 1 万个pagecache
    static std::atomic_uint32_t alloc_count(0); // alloc_count,代表当前分配到第几个元素了
    auto idx = alloc_count.fetch_add(1) & MASK;
    PageCache& page = pool[idx];
    page.updateVersion();
    return &page;
}


PageCache* PageCache::allocReadCache(){
    static constexpr uint32_t SIZE = 1u << 18; // 262144
    static constexpr uint32_t MASK = SIZE - 1;
    static std::vector<PageCache> pool(SIZE); //给定100 + 1 万个pagecache
    static std::atomic_uint32_t alloc_count(0); // alloc_count,代表当前分配到第几个元素了
    auto idx = alloc_count.fetch_add(1) & MASK;
    PageCache& page = pool[idx];
    page.updateVersion();
    return &page;
}