//
// Created by pengyibo on 2019-08-25.
//

#ifndef QUEUESTORE_CONCURRENTHASH_H
#define QUEUESTORE_CONCURRENTHASH_H

#include <unordered_map>
#include <utility>
#include <string>
#include <strhash.h>
#include "MutexLock.h"


/*
 * 一共分成1024个桶,每个桶维护 key 到 Queue的关系的映射
 *   Queue本身,包括元信息和Buffer(Buffer应该是可以替换的,因此可以考虑单独实现一个PagePool来解决(分桶,每个桶可以用原子变量来实现))
 *   Queue本身是直接分配的,算是new出来的,这里可以考虑用static的方式先分配好
 *
 * */

namespace CONCURRENT{
    static constexpr uint32_t SegNum = 4096;

    uint32_t MurmurHash2 ( const void * key, int len,uint32_t seed);



    class HashTable
    {

        class HashNode
        {
        public:
            char *key = nullptr;	// 指向实际的key,这里可以是string
            void *value = nullptr; // 指向实际的数据
            HashNode* next = nullptr;	// 这个hash_node上面的链表
            size_t hash; // 计算出来的hash值

            HashNode(char* key_, void* value_, size_t hash_, HashNode* next_): key(key_), value(value_), next(next_), hash(hash_){}
            HashNode(): key(nullptr), value(nullptr), next(nullptr), hash(0){}
        };

        class MemBlock{
        public:
            HashNode node;
            char buf[32];
        };


    public:
        HashNode* allocNode(const char* key, void* value, int len, uint32_t hash, HashNode* next){
            // 尝试了静态分配缓存进行优化,但是几乎没有效果(特别是访问顺序是按照分配的方式来的,如果在每一个table内部进行分配,实际上会导致cache不命中)
            char* key_cp = new char[len + 1];
            memcpy(key_cp, key, len);
            return new HashNode(key_cp, value, hash, next);
        }

        void* find(const char* key, int len, uint32_t prefixHash){
            //uint32_t hash = MurmurHash2(key, len, prefixHash);
            uint32_t hash = prefixHash;
            HashNode* node = table[(hash >> 12) & 255];

            while (node != nullptr){
                if(node->hash == hash && memcmp(node->key, key, len) == 0){
                    return node->value;
                }else{
                    node = node->next;
                }
            }

            return nullptr;
        }

        /*
         * 这里首先通过get来判断，如果没有相同key才会调用
         * 因此这里直接采取头部插入
         * */
        void put(const char* key, int len, void* value, uint32_t prefixHash){
            count++;
            //uint32_t hash = MurmurHash2(key, len, prefixHash);
            uint32_t hash = prefixHash;
            uint8_t idx = (hash >> 12) & 255;
            HashNode* node = table[idx];

            if(node == nullptr){
                table[idx] = allocNode(key, value, len, hash, nullptr);
            }else{
                node->next = allocNode(key, value, len, hash, node->next);
            }
        }

        size_t  size() const {
            return count;
        }


        size_t count = 0;
        HashNode* table[256]; // 4096个segment, 因此大约256足够装下了

    };



    template <class Value>
    class Segment{
        public:
            using Key = std::string ;
            using KeyRef =  const Key&;
            //using Hash = std::unordered_map<Key , Value>;
            using Hash = HashTable;

            using Str = std::string;
            SpinLock lock; // 这里用一个最简单的自旋锁来处理即可,因为冲突的机率比较小
            Hash map;

            Value get(KeyRef key, uint32_t hash){
                auto value = map.find(key.c_str(), key.length(), hash);
                return static_cast<Value>(value);
            }

            // 暂时不考虑并发写的情况,如果要考虑,那么
            void put( KeyRef key, Value val, uint32_t hash){
                //map[key] = val;
                map.put(key.c_str(), key.length(), val, hash);
            }


    };


    template <class Value>
    class ConcurrentHash {
    public:
        ConcurrentHash():segments(SegNum){}
        ConcurrentHash(const ConcurrentHash& other)=delete;


        Value* get(const std::string& key){
            uint32_t h = MurmurHash2(key.c_str(), key.length(), 0xA5E36FD4);
            uint32_t idx = h % SegNum;
            auto& seg = segments[idx];
            MutexLockGuard guard(seg.lock);
            return seg.get(key, h);
        }

        // 如果存在就不写入,否则就执行callable,得到返回结果
        // 因为有可能callable执行开销比较大或者有副作用,所以要保证一定是double check之后再执行
        // 或者外面进行synchronize来保证
        // 获取拷贝callabe有一点点开销,这个时候也可以考虑直接传值进来
        Value* putIfAbsent(const std::string& key, std::function<Value*(void)> callable){
            uint32_t h = MurmurHash2(key.c_str(), key.length(), 0xA5E36FD4);
            uint32_t idx = h % SegNum;

            auto& seg = segments[idx];
            MutexLockGuard guard(seg.lock);
            {
                Value* ret = nullptr;
                if((ret = seg.get(key, h)) == nullptr){
                    ret = callable();
                    seg.put(key, ret, h);
                    return ret;
                }

                return ret;
            }
        }

        void put(const std::string& key, Value* v){
            uint32_t h = MurmurHash2(key.c_str(), key.length(), 0xA5E36FD4);
            uint32_t idx = h % SegNum;

            auto& seg = segments[idx];
            Value* ret = v;
            MutexLockGuard guard(seg.lock);
            seg.put(key, ret, h);

        }

        // Data
        std::vector<Segment<Value*>> segments;
        const uint32_t feed = 0xA5E36FD4;
    };


    /*
     * 直接拷贝了一个MurmurHash2函数过来
     * */

    inline uint32_t MurmurHash2 ( const void * key, int len, uint32_t seed)
    {
        // 'm' and 'r' are mixing constants generated offline.
        // They're not really 'magic', they just happen to work well.
        static constexpr uint32_t m = 0x5bd1e995;
        static constexpr int r = 24;

        // Initialize the hash to a 'random' value

        uint32_t h = seed ^ len;

        // Mix 4 bytes at a time into the hash

        const unsigned char * data = (const unsigned char *)key;

        while(len >= 4)
        {
            uint32_t k = *(uint32_t*)data;

            k *= m;
            k ^= k >> r;
            k *= m;

            h *= m;
            h ^= k;

            data += 4;
            len -= 4;
        }

        // Handle the last few bytes of the input array
        switch(len)
        {
            case 3: h ^= data[2] << 16;
            case 2: h ^= data[1] << 8;
            case 1: h ^= data[0];
                h *= m;
        };

        // Do a few final mixes of the hash to ensure the last few
        // bytes are well-incorporated.

        h ^= h >> 13;
        h *= m;
        h ^= h >> 15;

        return h;
    }
};

#endif //QUEUESTORE_CONCURRENTHASH_H
