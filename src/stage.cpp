#include "stage.h"

#include "namespace.h"

stage_packet_t::stage_packet_t(DbTxn* tid, char* packet_id, tuple_buffer_t* out_buffer) :
  xact_id(tid), packet_string_id(packet_id), output_buffer(out_buffer) {

  // mutex that protects the list of the linked packets for this packet
  pthread_mutex_init_wrapper(&mutex, NULL);
  next_packet = NULL;
  num_merged_packets = 1;

  // receives a unique id for the packet
  stage_packet_counter * spc = stage_packet_counter::instance();
  spc->get_next_packet_id(&unique_id);

}


stage_packet_t::~stage_packet_t() {

  pthread_mutex_destroy_wrapper(&mutex);
}

  
bool stage_packet_t::mergable(stage_packet_t* packet) { 
  // by default a packet is not mergable

  return false; 

  UNUSED( packet );
}
 

void stage_packet_t::link_packet(stage_packet_t* packet) {
    
  pthread_mutex_lock_wrapper(&mutex);

  // prepend new packet
  packet->next_packet = next_packet;
  next_packet = packet;
  num_merged_packets++;

  pthread_mutex_unlock_wrapper(&mutex);
}

/*
 * Constructor: Creates the main queue of the stage, and initiates the
 * mutex that protects the creation of the worker threads.
 */

qpipe_stage::qpipe_stage(const char* sname) : name(sname)
{
  stage_type = GENERAL_STAGE;
  
  /* main queue of the stage */
  queue = new stage_queue_t(name);

  /* mutex that serializes the creation of the worker threads */
  if (pthread_mutex_init_wrapper(&worker_creator_mutex, NULL)) {
    TRACE_ERROR("Error in creating worker_thread_mutex. Stage=%d\n", sname);
  }


  if (pthread_mutex_init_wrapper(&worker_thread_count_mutex, NULL)) {
      TRACE_ERROR("Error creating worker_thread_count_mutex. Stage=%d\n", sname);
  }
}


/*
 * Destructor: Deletes the main queue of the stage and send a corresponding message
 * to the monitor. The worker_threads do not have to be deleted here, since they are
 * deleted inside the start_thread() function.
 */

qpipe_stage::~qpipe_stage()
{

  /* The threadPool[i] threads are deleted inside the start_thread() function */
  delete queue;
  MON_STAGE_DESTR();
}


/**
 * init_pool(int n_threads): Creates the pool of threads.
 */

void qpipe_stage::init_pool(pthread_t* stage_handles, int n_threads)
{

  /* check the requested size of the thread pool */
  if (n_threads > THREAD_POOL_MAX) {
    TRACE_ERROR("Stage: %s. Initializing worker_thread pool. Cannot have more that %d threads in the pool.\n", name, THREAD_POOL_MAX);
    n_threads = THREAD_POOL_MAX;
  }

  num_pool_threads = n_threads;

  worker_thread_count = num_pool_threads;


  for (int i = 0; i < num_pool_threads; i++) {
    
    // acquire lock before creating worker_thread
    pthread_mutex_lock_wrapper(&worker_creator_mutex);

    threadPool[i] = new worker_thread_t(this, i);
    thread_pids[i] = 0;

    // NIKOS
    // pthread_t     thread;
    // pthread_create(&thread, NULL, start_thread, (void*)threadPool[i]);

    pthread_attr_t pattr;
    pthread_attr_init( &pattr );
    pthread_attr_setscope( &pattr, PTHREAD_SCOPE_SYSTEM );
    pthread_create(&stage_handles[i], &pattr, start_thread, (void*)threadPool[i]);

    // release lock
    pthread_mutex_unlock_wrapper(&worker_creator_mutex);
  }

  // sleep for a while to give some time to the worker threads to be initialized
  sleep(1);
  MON_STAGE_CONSTR();
}


