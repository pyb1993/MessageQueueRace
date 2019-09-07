//
// Created by pengyibo on 2019-08-26.
//

#include "Storage.h"


void QueueStore::put(const String& queue_name, const MemBlock& message){
    auto ret = queuesMap.get(queue_name);

    if(ret == nullptr){
        ret = new Queue();
        queuesMap.put(queue_name, ret);
    }

    ret->put(message);
}


QueueStore::Vector<MemBlock> QueueStore::get(const String& queue_name, long offset, long size){
    auto q_ptr = queuesMap.get(queue_name);
    if (q_ptr) return q_ptr->get(offset, size);
    return Vector<MemBlock>();
}



