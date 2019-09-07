//
// Created by pengyibo on 2019-08-26.
//

#ifndef QUEUESTORE_STORAGE_H
#define QUEUESTORE_STORAGE_H

#include "Queue.h"


class QueueStore {
public:
    using String = std::string;
    template <class T> using Vector = std::vector<T>;
    QueueStore(): queuesMap(){}
    ~QueueStore(){printf("Questore deconstructing\n");}
    void put(const String& queue_name, const MemBlock& message);
    Vector<MemBlock> get(const String& queue_name, long offset, long size);

private:
    CONCURRENT::ConcurrentHash<Queue> queuesMap;
};

#endif
