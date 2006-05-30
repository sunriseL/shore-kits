// -*- C++ -*-
#ifndef __THREAD_H
#define __THREAD_H

// always the first include!
#include <pthread.h>
#include <cstdio>
#include <cstdlib>
#include <cerrno>

// include me last!!!
#include "namespace.h"

extern "C" void* start_thread(void *);

class thread_t {
 public:
    virtual void *run()=0;

    virtual ~thread_t() { }
};


void pthread_mutex_init_wrapper(pthread_mutex_t *mutex,
				const pthread_mutexattr_t *attr);
void pthread_mutex_lock_wrapper(pthread_mutex_t *mutex);
void pthread_mutex_unlock_wrapper(pthread_mutex_t *mutex);
void pthread_mutex_destroy_wrapper(pthread_mutex_t *mutex);

void pthread_cond_init_wrapper(pthread_cond_t *cond,
			       const pthread_condattr_t *attr);
void pthread_cond_destroy_wrapper(pthread_cond_t *cond);
void pthread_cond_signal_wrapper(pthread_cond_t *cond);
void pthread_cond_wait_wrapper(pthread_cond_t *cond,
			       pthread_mutex_t *mutex);


#include "namespace.h"
#endif  /* __THREAD_H */
