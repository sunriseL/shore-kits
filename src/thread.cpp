
#include <pthread.h>
#include "thread.h"
#include "trace/trace.h"
#include "qpipe_panic.h"

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <cstdio>
#include <cstring>




/* internal constants */

#define MAX_STRERROR_STRING_SIZE 128



/* internal data structures */

static pthread_key_t THREAD_KEY_SELF;
static pthread_key_t THREAD_KEY_SELF_NAME;



/* internal helper functions */

static void thread_error(const char* function_name, int err);
static void thread_destroy(void* thread_object);



// include me last!!!
#include "namespace.h"



thread_t::thread_t(void)
{
  // do nothing (only here for subclassing)
}



thread_t::thread_t(const char* thread_name)
{
  // copy thread name
  if ( asprintf(&this->thread_name, "%s", thread_name) == -1 )
  {
    TRACE(TRACE_ALWAYS, "asprintf() failed\n");
    QPIPE_PANIC();
  }
}



thread_t::thread_t(const char* format, va_list ap)
{
  init_thread_name(format, ap);
}



thread_t::thread_t(const char* format, ...)
{
  va_list ap;
  va_start(ap, format);
  init_thread_name(format, ap);
  va_end(ap);
}



thread_t::~thread_t(void)
{
  free(thread_name);
}



void thread_t::init_thread_name(const char* format, va_list ap)
{
  // construct thread name
  if ( vasprintf(&thread_name, format, ap) == -1 )
  {
    TRACE(TRACE_ALWAYS, "vasprintf() failed\n");
    QPIPE_PANIC();
  }
}



const char* thread_t::get_thread_name(void)
{
  return thread_name;
}



/**
 *  @brief Initialize thread module.
 *
 *  @return void
 */
void thread_init(void)
{
  if ( pthread_key_create( &THREAD_KEY_SELF, thread_destroy ) )
  {
    TRACE(TRACE_ALWAYS, "pthread_key_create() failed on THREAD_KEY_SELF\n");
    QPIPE_PANIC();
  }
  if ( pthread_key_create( &THREAD_KEY_SELF_NAME, NULL ) )
  {
    TRACE(TRACE_ALWAYS, "pthread_key_create() failed on THREAD_KEY_SELF_NAME\n");
    QPIPE_PANIC();
  }

  int err;
  err = pthread_setspecific(THREAD_KEY_SELF_NAME, "root-thread");
  if (err)
    thread_error("pthread_setspecific()", err);
}
 


const char* thread_get_self_name(void)
{
  // It would be nice to verify that the name returned is not
  // NULL. However, the name of the root thread can be NULL if we have
  // not yet completely initialized it.
  const char* thread_name =
    (const char*)pthread_getspecific(THREAD_KEY_SELF_NAME);
  return thread_name;
}



/**
 *  @brief thread_main function for newly created threads. Receives a
 *  thread_t object as its argument and it calls its run() function.
 *
 *  @param thread_object A thread_t*.
 *
 *  @return The value returned by thread_object->run().
 */
void* start_thread(void* thread_object)
{
  thread_t* thread = (thread_t*)thread_object;

  int err;

  err = pthread_setspecific(THREAD_KEY_SELF, thread);
  if (err)
    thread_error("pthread_setspecific() on THREAD_KEY_SELF", err);
  
  err = pthread_setspecific(THREAD_KEY_SELF_NAME, thread->get_thread_name());
  if (err)
    thread_error("pthread_setspecific() on THREAD_KEY_SELF_NAME", err);
  
  return thread->run();
}

 

void pthread_mutex_init_wrapper(pthread_mutex_t* mutex, const pthread_mutexattr_t* attr)
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



void pthread_mutex_lock_wrapper(pthread_mutex_t* mutex)
{
  int err = pthread_mutex_lock(mutex);
  if (err)
    thread_error("pthread_mutex_lock()", err);
}



void pthread_mutex_unlock_wrapper(pthread_mutex_t* mutex)
{
  int err = pthread_mutex_unlock(mutex);
  if (err)
    thread_error("pthread_mutex_unlock()", err);
}



void pthread_mutex_destroy_wrapper(pthread_mutex_t* mutex)
{
  int err = pthread_mutex_destroy(mutex);
  if (err)
    thread_error("pthread_mutex_destroy()", err);
}



void pthread_cond_init_wrapper(pthread_cond_t* cond, const pthread_condattr_t* attr)
{
  int err = pthread_cond_init(cond, attr);
  if (err)
    thread_error("pthread_cond_init()", err);
}



void pthread_cond_destroy_wrapper(pthread_cond_t* cond)
{
  int err = pthread_cond_destroy(cond);
  if (err)
    thread_error("pthread_cond_destroy()", err);
}



void pthread_cond_signal_wrapper(pthread_cond_t* cond)
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



static void thread_destroy(void* thread_object)
{
  qpipe::thread_t* thread = (qpipe::thread_t*)thread_object;
  delete thread;
}
