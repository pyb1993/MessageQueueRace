//
// Created by pengyibo on 2019-09-06.
//

#ifndef QUEUESTORE_IO_H
#define QUEUESTORE_IO_H

#include <iostream>
#include <vector>
#include <thread>
#include <utility>
#include <chrono>
#include <unistd.h>
#include <fcntl.h>

#include "BlockingQueue.h"
#include "Cache.h"
#include "ConcurrentHash.h"


namespace IO{

    class FixedBuffer;
    class File;

    using BufferPtr = std::unique_ptr<FixedBuffer>;
    using FilePtr = std::unique_ptr<File>;

    class IOContext{
    public:
        IOContext();
        ~IOContext();

        // 将一个指定page读入到内存里面
        void loadPage(PageCache* cache, int idx, uint64_t offset, uint32_t bytes);
        void flushPage(size_t paged_msg_size, PageCache* page, uint16_t& file_idx, uint64_t& file_offset);


    private:
        void flushHandler();
        void wakeUpIoThreads();
        void flushCurBuffer();
        void setCurBuffer(uint64_t origin_file_off, uint16_t origin_file_idx);


        CONCURRENT::MutexLock BufferVectorMutex;
        CONCURRENT::Condition notify;
        CONCURRENT::MutexLock FileMutex;
        CONCURRENT::SpinLock CurBufferMutex;
        std::vector<BufferPtr> fullBuffers;
        std::vector<BufferPtr> emptyBuffers;
        FilePtr Files[1024];
        BufferPtr curBuffer;
        std::atomic_bool running_;
        std::vector<std::thread> flushThreads;

    };


    IOContext& getIoContext();
}






#endif //QUEUESTORE_IO_H
