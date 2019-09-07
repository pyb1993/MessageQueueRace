//
// Created by pengyibo on 2019-08-25.
//

#ifndef QUEUESTORE_MUTEXLOCK_H
#define QUEUESTORE_MUTEXLOCK_H

#include <assert.h>
#include <pthread.h>
#include <atomic>
#include <stdio.h>

namespace CONCURRENT{

    class MutexLockBase{
    public:
        virtual void lock() = 0;
        virtual void unlock() = 0;
    };


    class MutexLock: public MutexLockBase
    {
    public:
        MutexLock()
        {
            pthread_mutex_init(&mutex_, NULL);
        }

        ~MutexLock()
        {
            pthread_mutex_destroy(&mutex_);
        }


        void lock()
        {
            pthread_mutex_lock(&mutex_);
        }

        void unlock()
        {
            pthread_mutex_unlock(&mutex_);
        }

        pthread_mutex_t* getPthreadMutex() /* non-const */
        {
            return &mutex_;
        }

    private:

        pthread_mutex_t mutex_;
    };



    class SpinLock: public MutexLockBase
    {
    public:
        void lock() {
            while (locked.test_and_set()) {
                cnt++;
                if((cnt & 7) == 0){
                    pthread_yield_np();
                }
            }

        }
        void unlock() {
            locked.clear();
        }

    private:
        int cnt = 0;
        std::atomic_flag locked = ATOMIC_FLAG_INIT ;
    };



    class MutexLockGuard
    {
    public:
        explicit MutexLockGuard(MutexLockBase& mutex) : mutex_(mutex)
        {
            mutex_.lock();
        }

        ~MutexLockGuard()
        {
            mutex_.unlock();
        }

    private:

        MutexLockBase& mutex_;
    };


}



#endif //QUEUESTORE_MUTEXLOCK_H
