#include "thread.h"
#include "namespace.h"

/** @fn    : start_thread(void*)
 *  @brief : Receives a thread as argument and it calls its run() function. When this function returns, it deletes the object.
 *  @note  : The passed thread_object is deleted here. The caller should delete only the thread hanlde.
 */

void* start_thread(void* thread_object) {
  thread_t* thread = (thread_t*)thread_object;
  return thread->run();
}



////////////////////////////////////////////////////////////
// protect against mutex faults
//
int pthread_mutex_init_wrapper(pthread_mutex_t *mutex, const pthread_mutexattr_t *attr)
{
  pthread_mutexattr_t         mutex_attr;
  const pthread_mutexattr_t * ptr_mutex_attr;

  // fprintf(stderr, "MUTEX init 0x%x by %d\n", mutex, pthread_self());
  if (attr == NULL && 0) {
    pthread_mutexattr_init(&mutex_attr);
    if (pthread_mutexattr_settype(&mutex_attr, PTHREAD_MUTEX_ERRORCHECK) != 0) {
      fprintf(stderr, "MUTEX ATTR TSET FAILED\n");
      exit(-1);
    }
    if (pthread_mutexattr_setpshared(&mutex_attr, PTHREAD_PROCESS_PRIVATE) != 0) {
      fprintf(stderr, "MUTEX ATTR PSET FAILED\n");
      exit(-1);
    }
    ptr_mutex_attr = &mutex_attr;
  }
  else {
    ptr_mutex_attr = attr;
  }

  int ret;
  if ((ret = pthread_mutex_init(mutex, ptr_mutex_attr)) != 0) {
    fprintf(stderr, "MUTEX INIT FAILED: ");
    switch (ret) {
      case EAGAIN: fprintf(stderr, "The system lacked the necessary resources to initialize another mutex.\n"); break;
      case ENOMEM: fprintf(stderr, "Insufficient memory exists to initialize the mutex.\n"); break;
      case EPERM:  fprintf(stderr, "The caller does not have the privilege to perform the operation.\n"); break;
      case EBUSY:  fprintf(stderr, "An attempt was detected to re-initialize a mutex previously initialized but not yet destroyed.\n"); break;
      case EINVAL: fprintf(stderr, "The value specified by attr or mutex is invalid.\n"); break;
      default:     fprintf(stderr, "Unknown error.\n");
    }
  }
  // fprintf(stderr, "MUTEX init 0x%x done by %d\n", mutex, pthread_self());

  return ret;
}



int pthread_mutex_lock_wrapper(pthread_mutex_t *mutex)
{
  int ret;
  //fprintf(stderr, "MUTEX lock 0x%x by %d\n", mutex, pthread_self());
  if ((ret = pthread_mutex_lock(mutex)) != 0) {
    perror("MUTEX LOCK FAILED\n");
  }
  //fprintf(stderr, "MUTEX lock 0x%x done by %d\n", mutex, pthread_self());
  return ret;
}



int pthread_mutex_unlock_wrapper(pthread_mutex_t *mutex)
{
  int ret;
  // fprintf(stderr, "MUTEX unlock 0x%x by %d\n", mutex, pthread_self());
  if ((ret = pthread_mutex_unlock(mutex)) != 0) {
    perror("MUTEX UNLOCK FAILED\n");
  }
  // fprintf(stderr, "MUTEX unlock 0x%x done by %d\n", mutex, pthread_self());
  return ret;
}



int pthread_mutex_destroy_wrapper(pthread_mutex_t *mutex)
{
  int ret;
  // fprintf(stderr, "MUTEX destroy 0x%x by %d\n", mutex, pthread_self());
  if ((ret = pthread_mutex_destroy(mutex)) != 0) {
    perror("MUTEX DESTROY FAILED\n");
  }
  // fprintf(stderr, "MUTEX destroy 0x%x done by %d\n", mutex, pthread_self());
  return ret;
}



int pthread_cond_init_wrapper(pthread_cond_t *cond, const pthread_condattr_t *attr)
{
  int ret;
  // fprintf(stderr, "COND init 0x%x by %d\n", cond, pthread_self());
  if ((ret = pthread_cond_init(cond, attr)) != 0) {
    perror("COND init FAILED\n");
  }
  // fprintf(stderr, "COND init 0x%x done by %d\n", cond, pthread_self());
  return ret;
}



int pthread_cond_destroy_wrapper(pthread_cond_t *cond)
{
  int ret;
  // fprintf(stderr, "COND destroy 0x%x by %d\n", cond, pthread_self());
  if ((ret = pthread_cond_destroy(cond)) != 0) {
    perror("COND destroy FAILED\n");
  }
  // fprintf(stderr, "COND destroy 0x%x done by %d\n", cond, pthread_self());
  return ret;
}



int pthread_cond_signal_wrapper(pthread_cond_t *cond)
{
  int ret;
  // fprintf(stderr, "COND signal 0x%x by %d\n", cond, pthread_self());
  if ((ret = pthread_cond_signal(cond)) != 0) {
    perror("COND signal FAILED\n");
  }
  // fprintf(stderr, "COND signal 0x%x done by %d\n", cond, pthread_self());
  return ret;
}


int cond_wait_count = 0;
int pthread_cond_wait_wrapper(pthread_cond_t *cond, pthread_mutex_t *mutex)
{
  int ret;
  // fprintf(stderr, "COND wait 0x%x by %d\n", cond, pthread_self());
  if ((ret = pthread_cond_wait(cond, mutex)) != 0) {
    perror("COND wait FAILED\n");
  }
  cond_wait_count++;
  
  // fprintf(stderr, "COND wait 0x%x done by %d\n", cond, pthread_self());
  return ret;
}

#include "namespace.h"
