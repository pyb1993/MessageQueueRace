//
// Created by pengyibo on 2019-08-26.
//

#ifndef QUEUESTORE_BLOCKINGQUEUE_H
#define QUEUESTORE_BLOCKINGQUEUE_H

#include "Condition.h"
#include "MutexLock.h"
#include <deque>
#include <assert.h>

namespace CONCURRENT
{

    template<typename T>
    class BlockingQueue
    {
    public:
        BlockingQueue()
                : mutex_(),
                  notEmpty_(mutex_),
                  queue_()
        {
        }

        void put(T&& x)
        {
            MutexLockGuard lock(mutex_);
            queue_.push_back(std::move(x));
            notEmpty_.notify();
        }


        T takeMove(){
            MutexLockGuard lock(mutex_);
            // always use a while-loop, due to spurious wakeup
            while (queue_.empty())
            {
                notEmpty_.wait();
            }

            T front = std::move(queue_.front());
            queue_.pop_front();
            return front;
        }

        T take()
        {
            MutexLockGuard lock(mutex_);
            // always use a while-loop, due to spurious wakeup
            while (queue_.empty())
            {
                notEmpty_.wait();
            }
            assert(!queue_.empty());
            T front(queue_.front());
            queue_.pop_front();
            return front;
        }

        size_t size() const
        {
            MutexLockGuard lock(mutex_);
            return queue_.size();
        }

    private:
        mutable MutexLock mutex_;
        Condition         notEmpty_;
        std::deque<T>     queue_;
    };

}



#endif //QUEUESTORE_BLOCKINGQUEUE_H
