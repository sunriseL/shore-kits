

#ifndef __MUTEX_H__
#define __MUTEX_H__

// always the first include!
#include <pthread.h>

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>

//#include "dlmalloc.h"

////////////////////////////////////////////////////////////
// protect against mutex faults
//
int pthread_mutex_init_wrapper(pthread_mutex_t *mutex,
                               const pthread_mutexattr_t *attr);
int pthread_mutex_lock_wrapper(pthread_mutex_t *mutex);
int pthread_mutex_unlock_wrapper(pthread_mutex_t *mutex);
int pthread_mutex_destroy_wrapper(pthread_mutex_t *mutex);

int pthread_cond_init_wrapper(pthread_cond_t *cond,
                              const pthread_condattr_t *attr);
int pthread_cond_destroy_wrapper(pthread_cond_t *cond);
int pthread_cond_signal_wrapper(pthread_cond_t *cond);
int pthread_cond_wait_wrapper(pthread_cond_t *cond,
                              pthread_mutex_t *mutex);


#endif /* __MUTEX_H__ */
