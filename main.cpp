#include <iostream>
#include <thread>
#include <vector>
#include <chrono>
#include <csignal>
#include <ratio>
#include <string>
#include "ConcurrentHash.h"
#include "QueueStore.h"

/*
 * 测试concurrentHash的性能如何
 *
 * */

// 消除sprintf put 100s左右
// 10亿条消息 put 386s左右, rand 50s左右 range 98s左右
int msgNum = 1 * 100000000;
//队列的数量 100w
const int queueNum = 100000; // 100条消息

//每个队列发送的数据量
const int queueMsgNum = msgNum / queueNum;

//正确性检测的次数
const int checkNum = 1000000; // 46s左右
//消费阶段的总队列数量，20%
const int checkQueueNum = 200000;
//发送的线程数量
const int sendTsNum = 10;
//消费的线程数量
const int checkTsNum = 10;
//消费的线程数量
const int consumerTsNum = 10;

// 每个queue随机检查次数
const int perQueueCheckNum = 1;

const std::string messagePrefix = "12345678901234567890123456789012345678901234567890-%d-%d";
const std::string queueNamePrefix = "abc123-wyp-";

queue_store store;

static std::string& getString(int num){
    static std::string s0 = "A12345678901234567890123456789012345678901234567890123456789";
    static std::string s1 = "B12345678901234567890123456789012345678901234567890123456789";
    static std::string s2 = "C12345678901234567890123456789012345678901234567890123456789";
    static std::string s3 = "D12345678901234567890123456789012345678901234567890123456789";
    static std::string s4 = "E12345678901234567890123456789012345678901234567890123456789";
    static std::string s5 = "F12345678901234567890123456789012345678901234567890123456789";
    static std::string s6 = "G12345678901234567890123456789012345678901234567890123456789";
    static std::string s7 = "H12345678901234567890123456789012345678901234567890123456789";
    static std::string s8 = "I12345678901234567890123456789012345678901234567890123456789";
    static std::string s9 = "J12345678901234567890123456789012345678901234567890123456789";

    int idx = num % 10;
    switch (idx){
        case 0:
            return s0;
        case 1:
            return s1;
        case 2:
            return s2;
        case 3:
            return s3;
        case 4:
            return s4;
        case 5:
            return s5;
        case 6:
            return s6;
        case 7:
            return s7;
        case 8:
            return s8;
        case 9:
            return s9;
    }
    return s1;
}

MemBlock GenerateMemBlock(int queueIndex, int num) {
    MemBlock memBlock{};
    memBlock.ptr = new char[messagePrefix.size() + 20];
    memBlock.size = 60;
    //memBlock.size = sprintf((char *)memBlock.ptr, messagePrefix.c_str(), queueIndex, num);
    memBlock.ptr = const_cast<char*>(getString(num).c_str());
    return memBlock;
}


/*这种方式会导致几乎不会有啥竞争出现的...因为Queue都被错开了*/
void SendFun(int startQueue, int endQueue) {

    std::vector<std::string> names;
    std::unordered_map<int,int> offsets;
    for(int index = startQueue; index < endQueue + 2; ++index){
        names.push_back(queueNamePrefix + std::to_string(index));
    }

    for (int num = 0; num < queueMsgNum; ++num) {
        for (int queueIndex = startQueue; queueIndex < endQueue; ++queueIndex) {

            // cost();
            //store.put(queueNamePrefix + std::to_string(queueIndex), GenerateMemBlock(queueIndex, num));
            store.put(names[queueIndex - startQueue], GenerateMemBlock(queueIndex, num));
        }
    }
}

int cnt = 0;

void CheckSingleMemBlock(int queueIndex, int num, const MemBlock &memBlock) {
    char readlMessage[256];
    size_t msgSize = 60;
    //size_t msgSize = sprintf(readlMessage, messagePrefix.c_str(), queueIndex, num);

    // 这里可以改真正的msg : s -> realMessage
    if (memcmp(memBlock.ptr, getString(num).c_str() , msgSize) != 0) {
        printf("[##ERROR##] check message content error, Queue:%d QueueIndex:%d %s != %s\n", queueIndex, num,
               readlMessage, std::string((const char *)memBlock.ptr, memBlock.size).c_str());
        if(cnt++ == 3){
            _exit(1);
        }
        return;
    }
#ifdef DEBUG_FLAG
    printf("%s\n", readlMessage);
#endif
}


void CheckMessageCount(int offset, int count, size_t realCount) {
    size_t expectCount = std::min(queueMsgNum - offset, count);
    if (expectCount != realCount) {
        printf("[##ERROR##] check message count error, %d %d %d %d\n", offset, count,
               (int)realCount, (int)expectCount);
    }
}


void CheckResult(int queueIndex, int offset, int count, const std::vector<MemBlock> blockList) {
    CheckMessageCount(offset, count, blockList.size());
    for (size_t size = 0; size < blockList.size(); ++size) {
        CheckSingleMemBlock(queueIndex, offset + size, blockList[size]);
        delete[]((char *)blockList[size].ptr);
    }
}


void ConsumeCheck(std::vector<int> queueIndexList, std::unordered_map<int, int> offsets) {
    // 每次读10条消息，进行检查,直到检查完所有

    while (!offsets.empty()) {
        for (size_t index = 0; index < queueIndexList.size(); ++index) {
            int queueIndex = queueIndexList[index];
            auto offset_it = offsets.find(queueIndex);
            if (offset_it == offsets.end()) continue;

            int &startOffset = offset_it->second;
            int msgCount = 10;
            auto result = store.get(queueNamePrefix + std::to_string(queueIndex), startOffset, msgCount);
            CheckResult(queueIndex, startOffset, msgCount, result);
            startOffset += result.size();

            if (startOffset == queueMsgNum) {
                offsets.erase(offset_it);
            }
        }
    }
}

void RandCheck(std::vector<int> queueIndexList, int checkCount) {
    for (size_t index = 0; index < queueIndexList.size(); ++index) {
        for (int i = 0; i < checkCount; ++i) {
            int offset = random() % queueMsgNum;
            int msgCount = random() % 10 + 5; // 5 - 15
            int queueIndex = queueIndexList[index];
            auto result = store.get(queueNamePrefix + std::to_string(queueIndex), offset, msgCount);
            CheckResult(queueIndex, offset, msgCount, result);
        }
    }
}


/*
 * 遇到的问题,每次写入大约才100M的速度,过于缓慢
 * 但是对比日志程序,至少都能写入超过400M
 * 经过对比猜测 & profile:
 *  1 可能和sprintf(cpu)占用有关
 *  2 可能和fwrite有关. setNoBuffer
 *  现象比较奇怪,就是使用了sprintf或者to_string这种操作,会使得fwrite本身变得很慢
 *  设置NoBuffer,磁盘写入会比较慢，稳定在100+M/s
 *  使用默认全缓存,如果没有sprintf，那么比较快，否则也很慢
 *  设置localBuffer,保证速度较快
 *
 *  加入使用cost函数模拟cpu消耗,没有影响
 *  换成write后直接好了
 *
 *  为了更好的profile,可以先绕过sprintf,用getString代替
 *
 *
 * */

void printDuration(decltype(std::chrono::high_resolution_clock::now()) start){
    auto t1 = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::ratio<1, 1>> duration_s(t1 - start);
    std::cout <<"phase: " << duration_s.count() << " seconds" << std::endl;
}

int main() {
    auto start = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> producerThreads;
    for (int i = 0; i < sendTsNum; ++i) {
        int startQueue = i * queueNum / sendTsNum;
        int endQueue = startQueue + queueNum / sendTsNum;
        producerThreads.push_back(std::thread(SendFun, startQueue, endQueue));
    }

    for (int i = 0; i < sendTsNum; ++i) {
        producerThreads[i].join();
        printf("producer %d done \n", i);
    }

    printDuration(start);
    auto t1 = std::chrono::high_resolution_clock::now();


    std::vector<std::thread> randomCheckThreads;
    for (int i = 0; i < checkTsNum; ++i) {
        std::vector<int> queueIndexList;
        int thisThreadCheckNum = checkNum / checkTsNum;
        for (int j = 0; j < thisThreadCheckNum; ++j) {
            queueIndexList.push_back(random() % queueNum);
        }
        randomCheckThreads.push_back(std::thread(RandCheck, queueIndexList, perQueueCheckNum));
    }

    for (int i = 0; i < checkTsNum; ++i) {
        randomCheckThreads[i].join();
        printf("rand check %d done \n", i);

    }

    printDuration(t1);
    auto t2 = std::chrono::high_resolution_clock::now();


    std::vector<std::thread> consumerThreads;
    for (int i = 0; i < consumerTsNum; ++i) {
        int thisThreadConsumeNum = checkQueueNum / consumerTsNum;
        std::vector<int> queueIndexList;

        //offsets表示这个线程要消费的offset
        std::unordered_map<int, int> offsets;
        while (offsets.size() != size_t(thisThreadConsumeNum)) {
            int queueIndex = random() % queueNum;
            if (offsets.find(queueIndex) == offsets.end()) {
                offsets[queueIndex] = 0;
                queueIndexList.push_back(queueIndex);
            }
        }
        consumerThreads.push_back(std::thread(ConsumeCheck, queueIndexList, offsets));
    }

    for (int i = 0; i < consumerTsNum; ++i) {
        consumerThreads[i].join();
        printf("consumer %d done \n", i);
    }

    printDuration(t2);

    //test_concurrent_hash();

    return 0;
}