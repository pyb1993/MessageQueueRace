//
// Created by pengyibo on 2019-09-06.
//

#include "IO.h"

namespace IO{
    static IOContext Ioctx;


    // 用来获取这个全局变量
    IOContext& getIoContext(){
        return Ioctx;
    }

    // 二级缓存
    class FixedBuffer
    {
    public:
        FixedBuffer(): cur_(data_){}

        void append(const char* /*restrict*/ buf, size_t len)
        {
            assert(avail() > len);
            memcpy(cur_, buf, len);
            cur_ += len;
            file_offset += len;
        }

        const char* data() const { return data_; }
        uint32_t length() const { return cur_ - data_; }
        size_t avail() const { return static_cast<int>(end() - cur_); }
        void reset() {
            cur_ = data_;
            bugs.clear();
            file_offset = 0;
            file_idx = -1;
        }
        void resetFileAttr(uint16_t fidx, uint64_t fileOffset){
            file_idx = fidx;
            file_offset = fileOffset;
        }

        uint16_t fileIdx(){return file_idx;}
        uint64_t fileOffset(){return file_offset;}

        std::vector<uint32_t > bugs;


    private:
        uint16_t file_idx = 0;
        uint64_t file_offset = 0; //代表这个buffer目前已经写到哪个位置了
        const char* end() const { return data_ + sizeof data_; }
        char data_[16 * 1024 * 1024];
        char* cur_;
    };


    // 这个类会封装文件相关的操作
    class File
    {
    public:
        File()= default;
        File(const std::string& name, int fid):filename_(name + std::to_string(fid) + ".log"){
            fd_ = ::open(filename_.c_str(), O_RDWR | O_CREAT | O_APPEND , 0645);
            fcntl(fd_, F_NOCACHE, 1);
            printf("new file %s\n", filename_.c_str());
        }

        ~File(){
            close(fd_);
        }

        void append(const char* logline, size_t len, size_t offset){
            if(len <= 0){
                return;
            }

            errno = 0;
            size_t n = writeToFile(logline, len, offset);
            assert(n == len);
            if(errno != 0){
                perror("error:");
            }

        }

        size_t writeToFile(const char* logline, size_t len, size_t offset)
        {
            return pwrite(fd_, logline, len, offset);
            //return write(fd_, logline, len);
        }


        void read(uint64_t offset, char* buf, size_t size) {
            ssize_t ret = ::pread(fd_, (void*)buf, size, offset);
            if (ret == -1) perror("read buffer failed");
        }

    private:
        const std::string filename_;
        int fd_;
    };


    /******************************************下面是IOContext的描述****************************************************/
    IOContext::IOContext(): BufferVectorMutex(),notify(BufferVectorMutex), curBuffer(new FixedBuffer),  running_(true){
        // 初始化一个Buffer
        for(int i = 0; i < 8; ++i){
            emptyBuffers.push_back(BufferPtr(new FixedBuffer));
        }

        // 单/多线程并发写文件(多线程有利于降低内存积攒Buffer压力,目前没有测出多线程写入能提高速度,反而降低了一点速度)
        for(int i = 0; i < 1; ++i){
            flushThreads.emplace_back(&IOContext::flushHandler, this);
        }
    }

    IOContext::~IOContext(){
        running_ = false;
        wakeUpIoThreads();
        for(auto& th : flushThreads){
            th.join();
        }
    }


    void IOContext::flushCurBuffer(){
        CONCURRENT::MutexLockGuard guard(BufferVectorMutex);
        fullBuffers.push_back(std::move(std::move(curBuffer)));
        if(!emptyBuffers.empty()){
            curBuffer = std::move(emptyBuffers.back());
            assert(curBuffer);

            emptyBuffers.pop_back();
            assert(curBuffer);
        }else{
            curBuffer.reset(new FixedBuffer);
        }
    }

    void IOContext::loadPage(PageCache* cache, int idx, uint64_t offset, uint32_t bytes){
        FilePtr& file = Files[idx];
        file->read(offset, cache->mem, bytes);
    }


    void IOContext::flushPage(size_t paged_msg_size, PageCache* page, uint16_t& file_idx, uint64_t& file_offset){
        CONCURRENT::MutexLockGuard guard(CurBufferMutex);
        if(curBuffer->avail() < paged_msg_size)
        {
            auto origin_file_off = curBuffer->fileOffset();
            auto origin_file_idx = curBuffer->fileIdx();

            // 将数据提交到另外一个线程
            flushCurBuffer();

            // 顺序不要反了, 先reset后 origin_file_off
            curBuffer->reset();
            setCurBuffer(origin_file_off, origin_file_idx);
            wakeUpIoThreads();
        }

        // 改为circularBuffer,这样就降低Mutex的开销
        file_idx = curBuffer->fileIdx();
        file_offset = curBuffer->fileOffset();
        curBuffer->append(page->mem, paged_msg_size);
    }

    void IOContext::setCurBuffer(uint64_t origin_file_off, uint16_t origin_file_idx){
        // 超过8G就换一个File
        if(origin_file_off >= 8 * 1024 * 1024 * 1024L){
            curBuffer->resetFileAttr(origin_file_idx + 1, 0);
        }else{
            curBuffer->resetFileAttr(origin_file_idx, origin_file_off);
        }
    }

    void IOContext::wakeUpIoThreads(){
        notify.notifyAll();
    }

    void IOContext::flushHandler(){
        while (running_){
            //  执行刷盘的逻辑
            std::vector<BufferPtr> buffersToWrite;

            {
                CONCURRENT::MutexLockGuard guard(BufferVectorMutex);
                // 这里写成if,避免去检查running_
                if (fullBuffers.empty()){
                    notify.waitForSeconds(1);
                }
            }

            {
                CONCURRENT::MutexLockGuard guard2(CurBufferMutex);
                if(curBuffer->avail() > 0){
                    auto origin_file_off = curBuffer->fileOffset();
                    auto origin_file_idx = curBuffer->fileIdx();
                    flushCurBuffer();
                    assert(curBuffer);
                    setCurBuffer(origin_file_off, origin_file_idx);
                }
                buffersToWrite.swap(fullBuffers);
            }

            static uint64_t writeAll = 0;
            // 全部写入磁盘
            auto t1 = std::chrono::high_resolution_clock::now();
            uint64_t writeSize = 0;
            for(auto& buffer : buffersToWrite){
                auto fid = buffer->fileIdx();

                // double check lock
                if (!Files[fid]){
                    CONCURRENT::MutexLockGuard guard1(FileMutex);
                    if (!Files[fid]){
                        Files[fid].reset(new File("messageQueue-", fid));
                    }
                }

                auto& filePtr = Files[fid];
                filePtr->append(buffer->data(), buffer->length(), buffer->fileOffset() - buffer->length());
                writeAll += buffer->length();
                writeSize += buffer->length();
            }

            auto t2 = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double, std::ratio<1, 1>> duration_s(t2 - t1);

            // printf("write full buffers done, num = %d, size = %lfMB,  seconds = %lf\n", buffersToWrite.size(), (writeSize + 0.0) / (1024*1024), duration_s.count());

            {
                CONCURRENT::MutexLockGuard guard(BufferVectorMutex);
                for(auto& buffer : buffersToWrite){
                    if(emptyBuffers.size() < 1000){
                        buffer->reset(); //千万不能写成buffer.reset()，这代码bufferPtr自己reset
                        emptyBuffers.push_back(std::move(buffer));
                    }
                }

                buffersToWrite.clear();
            }
        }

        printf("flushThread end\n");
    }


}


