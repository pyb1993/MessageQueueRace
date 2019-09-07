//
// Created by pengyibo on 2019-09-07.
//

#ifndef QUEUESTORE_QUEUE_H
#define QUEUESTORE_QUEUE_H

#include <atomic>
#include "IO.h"
#include "Cache.h"

class MemBlock {
public:
    char* ptr;    // new char[length]
    size_t size;  // length of msg

    static MemBlock allocMemBlock(const char* src, size_t size) {
        MemBlock msg;
        msg.ptr = static_cast<char *>(::malloc(size));
        ::memcpy(msg.ptr, src, size);
        msg.size = size;
        return msg;
    }
};


class PageIndex;
class Queue {
public:
    using IndexPtr = std::unique_ptr<PageIndex>;
    template <class T> using Vector = std::vector<T>;
    explicit Queue();
    void put(const MemBlock& message);
    Vector<MemBlock> get(uint32_t offset, uint32_t number);
    
private:
    uint16_t cur_data_slot_off_ = 0; // 下一个开始写入的位置
    PageCache* write_page; // 当前写入的buffer
    Vector<IndexPtr> paged_message_indices_; // todo index超过30个就进行一次合并,保证占用的内存不会太多

    // 函数部分
    int searchIndex(uint32_t msg_offset);
    bool needFlush(uint16_t& next_write_offset, uint16_t size);

    static uint16_t extractMessageLength(const char*& ptr);
    static uint32_t parseMsgs(const IndexPtr & index, uint32_t& offset, uint32_t& num, const char* ptr, Vector<MemBlock>& msgs);

};


#endif //QUEUESTORE_QUEUE_H
