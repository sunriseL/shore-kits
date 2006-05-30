
#include "thread.h"
#include "trace/trace.h"
#include "qpipe_panic.h"

#include <string.h>




/* internal constants */

#define MAX_STRERROR_STRING_SIZE 128




/* internal helper functions */

static void thread_error(const char* function_name, int err);




// include me last!!!
#include "namespace.h"


/**
 *  @brief : Receives a thread_t as its argument and it calls its
 *  run() function.
 *
 *  @param thread_object A thread_t*.
 *
 *  @return The value returned by thread_object->run().
 */
void* start_thread(void* thread_object)
{
  thread_t* thread = (thread_t*)thread_object;
  return thread->run();
}



void pthread_mutex_init_wrapper(pthread_mutex_t *mutex, const pthread_mutexattr_t* attr)
{

  pthread_mutexattr_t        mutex_attr;
  const pthread_mutexattr_t* ptr_mutex_attr;
  int err;

  
  if (attr == NULL && 0)
  {
    err = pthread_mutexattr_init(&mutex_attr);
    if (err)
      thread_error("pthread_mutexattr_init()", err);

    err = pthread_mutexattr_settype(&mutex_attr, PTHREAD_MUTEX_ERRORCHECK);
    if (err)
      thread_error("pthread_mutexattr_settype()", err);
    
    err = pthread_mutexattr_setpshared(&mutex_attr, PTHREAD_PROCESS_PRIVATE);
    if (err)
      thread_error("pthread_mutexattr_setpshared()", err);

    ptr_mutex_attr = &mutex_attr;
  }
  else
  {
    ptr_mutex_attr = attr;
  }


  err = pthread_mutex_init(mutex, ptr_mutex_attr);
  if (err)
    thread_error("pthread_mutex_init()", err);
}



void pthread_mutex_lock_wrapper(pthread_mutex_t *mutex)
{
  int err = pthread_mutex_lock(mutex);
  if (err)
    thread_error("pthread_mutex_lock()", err);
}



void pthread_mutex_unlock_wrapper(pthread_mutex_t *mutex)
{
  int err = pthread_mutex_unlock(mutex);
  if (err)
    thread_error("pthread_mutex_unlock()", err);
}



void pthread_mutex_destroy_wrapper(pthread_mutex_t *mutex)
{
  int err = pthread_mutex_destroy(mutex);
  if (err)
    thread_error("pthread_mutex_destroy()", err);
}



void pthread_cond_init_wrapper(pthread_cond_t *cond, const pthread_condattr_t *attr)
{
  int err = pthread_cond_init(cond, attr);
  if (err)
    thread_error("pthread_cond_init()", err);
}



void pthread_cond_destroy_wrapper(pthread_cond_t *cond)
{
  int err = pthread_cond_destroy(cond);
  if (err)
    thread_error("pthread_cond_destroy()", err);
}



void pthread_cond_signal_wrapper(pthread_cond_t *cond)
{
  int err = pthread_cond_signal(cond);
  if (err)
    thread_error("pthread_cond_signal()", err);
}



void pthread_cond_wait_wrapper(pthread_cond_t* cond, pthread_mutex_t* mutex)
{
  int err = pthread_cond_wait(cond, mutex);
  if (err)
    thread_error("pthread_cond_wait()", err);
}

#include "namespace.h"




/* definitions of internal helper functions */

static void thread_error(const char* function_name, int err)
{
  char error_buf[MAX_STRERROR_STRING_SIZE];
  strerror_r(err, error_buf, MAX_STRERROR_STRING_SIZE);
  TRACE(TRACE_ALWAYS, "%s failed: %s\n",
	function_name,
	error_buf);
  QPIPE_PANIC();
}
