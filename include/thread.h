// -*- C++ -*-
#ifndef __THREAD_H
#define __THREAD_H


// always the first include!
#include <pthread.h>
#include <cstdio>
#include <cstdlib>
#include <cerrno>
#include <cassert>

#include "utils.h"


// include me last!!!
#include "namespace.h"



// exported datatypes

class thread_t
{
private:
  char* thread_name;

public:
  thread_t(const char* thread_name);
  thread_t(const char* format, va_list ap);
  thread_t(const char* format, ...);

  virtual void* run(void)=0;
  virtual ~thread_t(void);
  
  const char* get_thread_name(void);
  
protected:
  thread_t(void);
  void init_thread_name(const char* format, va_list ap);
};



// exported functions

void thread_init(void);

const char* thread_get_self_name(void);

extern "C" void* start_thread(void *);

void pthread_mutex_init_wrapper   (pthread_mutex_t* mutex,
				   const pthread_mutexattr_t* attr);
void pthread_mutex_lock_wrapper   (pthread_mutex_t* mutex);
void pthread_mutex_unlock_wrapper (pthread_mutex_t* mutex);
void pthread_mutex_destroy_wrapper(pthread_mutex_t* mutex);

void pthread_cond_init_wrapper   (pthread_cond_t* cond,
				  const pthread_condattr_t* attr);
void pthread_cond_destroy_wrapper(pthread_cond_t* cond);
void pthread_cond_signal_wrapper (pthread_cond_t* cond);
void pthread_cond_wait_wrapper   (pthread_cond_t* cond,
				  pthread_mutex_t* mutex);


/**
 * @brief A critical section manager.
 *
 * Locks and unlocks the specified mutex upon construction and
 * following call to 'exit', respectively. Also verifies that the
 * mutex is unlocked exactly once during this object's lifetime.
 */
struct critical_section_state_t {
    typedef pthread_mutex_t *Param;
    Param _mutex;
    critical_section_state_t(Param mutex)
        : _mutex(mutex)
    {
    }
    void init() { pthread_mutex_lock_wrapper(_mutex); }
    void cleanup() { pthread_mutex_unlock_wrapper(_mutex); }
};

typedef init_cleanup_t<critical_section_state_t> critical_section_t;


#include "namespace.h"
#endif  /* __THREAD_H */
