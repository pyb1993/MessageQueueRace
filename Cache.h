//
// Created by pengyibo on 2019-08-26.
//

#ifndef QUEUESTORE_CACHE_H
#define QUEUESTORE_CACHE_H


#include <cstdint>
#include <random>
#include "MutexLock.h"
/*
 * 同时提供读写Buffer的内存
 *
 *
 * */

class PageCache {
public:

    // therad safe,需要在锁的保护下
    void updateVersion();
    static PageCache* allocPageCache();
    //只分配ReadCache,避免和WriteCache发生冲突
    static PageCache* allocReadCache();


    // version用来确保当前是第几个版本,这样用来分析是否失效,每次获取这个pageCache都必须要增加一次
    char mem[4 * 1024];
    uint64_t version = 0;
    CONCURRENT::SpinLock pageLock;
};


#endif //QUEUESTORE_CACHE_H