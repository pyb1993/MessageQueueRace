//
// Created by pengyibo on 2019-09-07.
//

#include "Queue.h"

class PageIndex{
public:
    std::atomic_uint64_t page_address;
    uint64_t cache_version = 0; //表示这个version对应的cache的值,用来判断有没有失效
    uint64_t file_offset; // 距离file的位置(异步线程设置)
    uint32_t prev_total_msg_size; // 此队列在这个index之前有多少消息
    uint16_t file_idx; //到哪个file上面去了，这里如果要支持边读边写,那么需要考虑file的可见性(暂时先预分配1024个file)(异步线程设置)
    uint16_t msg_size; //当前积累的msg的数量(提前设置)
    uint16_t msg_bytes = 0; // 当前一共积累了多少消息

    PageIndex(uint64_t file_offset, uint32_t prev_total_msg_size, PageCache* page):
            page_address(0),
            file_offset(file_offset),
            prev_total_msg_size(prev_total_msg_size),
            file_idx(0),
            msg_size(0) {
        setPage(page);
    }

    uint32_t total_msg_size() const {return prev_total_msg_size + msg_size;}
    void setVersion(uint64_t version){cache_version = version;}
    void setPage(PageCache* page){page_address.store(uint64_t(page));}
    PageCache* page(){return (PageCache*)(page_address.load());}
};


Queue::Queue(): write_page(PageCache::allocPageCache()) {
    paged_message_indices_.reserve(8);
    paged_message_indices_.emplace_back(new PageIndex(0, 0, write_page));
    paged_message_indices_.back()->setVersion(write_page->version);
}


Queue::Vector<MemBlock> Queue::get(uint32_t offset, uint32_t number) {
    /*
     * todo 如果有读写并发,那么需要加读锁
     * 可以考虑针对PageCache加锁,这样锁的粒度会小很多,先进行一次查询,如果查询中了就不需加锁了,
     * 因为pageIndex本身在变化,所以需要主要可能会冲突,这个查询过程是需要加锁的
     * */

    auto first_page_idx = searchIndex(offset);
    if(first_page_idx == -1 || static_cast<size_t>(first_page_idx) == paged_message_indices_.size()){
        return Vector<MemBlock>();
    }

    int write_pageidx = searchIndex(offset + number);
    if(static_cast<size_t>(write_pageidx) == paged_message_indices_.size()){
        write_pageidx--;
    }

    //接下来开始读
    Vector<MemBlock> msgs;
    msgs.reserve(number);

    PageCache* page;
    for (size_t page_idx = first_page_idx; page_idx <= static_cast<size_t>(write_pageidx); ++page_idx) {
        auto& index = paged_message_indices_[page_idx];

        while (true) {
            page = index->page();

            //注意这里只对pageCache加锁,但是没有对index加锁,所以可能出现index并发的情况
            CONCURRENT::MutexLockGuard guard(page->pageLock);
            if (index->cache_version != page->version){
                // 目前这个version已经无效,需要重新申请一个page
                page = PageCache::allocReadCache();
                IO::getIoContext().loadPage(page, index->file_idx, index->file_offset, index->msg_bytes);
                index->setPage(page);
                index->setVersion(page->version);
                continue;
            }else{
                break;
            }
        }

        parseMsgs(index, offset, number, page->mem, msgs);
        // 这里缺少readAhead机制,在linux下有readAhead系统调用可以提前去加载cache,降低延迟
    }

    return msgs;
}


uint32_t Queue::parseMsgs(const IndexPtr & index, uint32_t& offset, uint32_t& num, const char* ptr, Vector<MemBlock>& msgs) {
    uint64_t cur_offset = index->prev_total_msg_size;
    uint16_t msg_num = index->msg_size;
    if (offset >= cur_offset + msg_num) return 0;

    auto begin = ptr;

    // 因为不知道想要定位的消息的具体位置,所以只能一个一个的进行读取
    while (cur_offset != offset && msg_num > 0) {
        uint16_t msg_size = extractMessageLength(begin);
        begin += msg_size;
        ++cur_offset;
        --msg_num;
    }

    // read related messages to msgs
    while (num > 0 && msg_num > 0) {
        uint16_t msg_size = extractMessageLength(begin);

        auto msg = MemBlock::allocMemBlock(begin, msg_size);
        msgs.push_back(msg);

        begin += msg_size;
        ++cur_offset, ++offset, --num, --msg_num;
    }

    return msg_num;
}


int Queue::searchIndex(uint32_t msg_offset) {
    // 先通过循环查找找到包含第一个消息的index以及包含最后一个消息的size

    size_t average_size = 60;//每个page大约66个左右,这个后续可以通过统计来进行优化
    int start = msg_offset / average_size; //得到搜索的开始位置
    int index_size = paged_message_indices_.size();
    if(start >= index_size){
        start = index_size - 1;
    }


#define CHECK_IN_RANGE(t, a, b) ( (a) <= (t) && (t) < (b) )

    //  找到  prev_msg_size <= msg_offset < total_msg_size
    if(paged_message_indices_[start]->total_msg_size() > msg_offset){
        while (start >= 0
               && !CHECK_IN_RANGE(msg_offset,
                                  paged_message_indices_[start]->prev_total_msg_size,
                                  paged_message_indices_[start]->total_msg_size()))
        {
            start--;
        }
    }else{
        while (start < index_size
               && !CHECK_IN_RANGE(msg_offset,
                                  paged_message_indices_[start]->prev_total_msg_size,
                                  paged_message_indices_[start]->total_msg_size()))
        {
            start++;
        }
    }
    return start;
#undef CHECK_IN_RANGE
}


void Queue::put(const MemBlock& message){
    assert(message.size <= 4000);

    char* msg_ptr = (char*)message.ptr;                  \
        size_t msg_size = message.size;
    uint16_t next_write_offset;

    // todo 如果存在并发写,那么需要考虑在这里加锁, spin_lock一下
    if (needFlush(next_write_offset, msg_size)){
        auto& page_index = paged_message_indices_.back();
        IO::getIoContext().flushPage(cur_data_slot_off_, write_page, page_index->file_idx, page_index->file_offset);

        // 注意这里更新了version,所以原来index的version已经失效了,读的时候会发现这一点并且重新申请
        write_page->updateVersion();
        paged_message_indices_.emplace_back(new PageIndex(0, page_index->total_msg_size(), write_page));// 此时file_offset实际上还不确定
        paged_message_indices_.back()->setVersion(write_page->version);

        cur_data_slot_off_ = msg_size + 2;
    }else{
        cur_data_slot_off_ += msg_size + 2;
    }

    write_page->mem[next_write_offset] = msg_size >> 8u;
    write_page->mem[next_write_offset + 1] = msg_size & 0xffu;
    ::memcpy(write_page->mem + next_write_offset + 2, msg_ptr, msg_size);

    // 修正这个index含有的msg的数目和字节数,读的时候需要使用
    ++paged_message_indices_.back()->msg_size;
    paged_message_indices_.back()->msg_bytes += msg_size + 2;
}


bool Queue::needFlush(uint16_t& next_write_offset, uint16_t size){
    // 随机一下,这样可以避免一起写入磁盘,写入范围是3.5K到4K
    auto index_size = paged_message_indices_.back()->msg_size;
    bool flush = index_size > 3500 && index_size  >= (4096 - (rand() & 511));
    if(flush || cur_data_slot_off_ + size > 4096){
        next_write_offset = 0;
        return true;
    }else{
        next_write_offset = cur_data_slot_off_;
        return false;
    }
}


uint16_t Queue::extractMessageLength(const char*& ptr) {
    uint16_t msg_size = *(ptr++);
    return (msg_size << 8u) + (uint8_t)(*(ptr++));
}



