#include "stage.h"
#include "trace/trace.h"

#include "namespace.h"

/*
 * Constructor: Creates the main queue of the stage, and initiates the
 * mutex that protects the creation of the worker threads.
 */

stage_t::stage_t(const char* sname) : name(sname)
{
    /* main queue of the stage */
    queue = new stage_queue_t(name);

    /* mutex that serializes the creation of the worker threads */
    if (pthread_mutex_init_wrapper(&stage_lock, NULL)) {
        TRACE(TRACE_ALWAYS, "Error in creating stage_lock. Stage=%d\n", name);
    }
}


/*
 * Destructor: Deletes the main queue of the stage and send a corresponding message
 * to the monitor. The worker_threads do not have to be deleted here, since they are
 * deleted inside the start_thread() function.
 */

stage_t::~stage_t()
{

    /* The threadPool[i] threads are deleted inside the start_thread() function */
    delete queue;
}


bool stage_t::is_mergeable(packet_t *packet) {
    bool result;
    pthread_mutex_lock_wrapper(&stage_lock);
    result = packet->mergeable;
    pthread_mutex_unlock_wrapper(&stage_lock);
    return result;
}

int stage_t::output(packet_t *packet, const tuple_t &tuple) {
    // send the tuple to the output buffer
    return packet->output_buffer->put_tuple(tuple);
}

/** Automatically deletes a pointer when 'this' goes out of scope */
template <typename T>
struct scope_delete_t {
    T *ptr;
    scope_delete_t(T *p)
        : ptr(p)
    {
    }

    ~scope_delete_t() {
        if(ptr)
            delete ptr;
    }
    operator T*() {
        return ptr;
    }
    
    T *operator ->() {
        return ptr;
    }
};

int stage_t::process_next_packet() {
    // block until a new packet becomes available
    scope_delete_t<packet_t> packet = queue->remove();
    if(packet == NULL) {
        TRACE(TRACE_ALWAYS, "Null packet arrive at stage %s!", name);
        return 1;
    }

    // TODO: process rebinding instructions here

    // wait for a green light from the parent
    // TODO: what about the parents of merged packets?
    if(packet->output_buffer->wait_init()) {
        // make sure nobody else tries to merge
        not_mergeable(packet);

        // TODO: check if we have any merged packets before closing
        // the input buffer
        return 1;
    }

    // process the packet
    int early_termination = process_packet(packet);
    if(early_termination) {
        // TODO: something special?
    }

    // close the output buffer
    // TODO: also close merged packets' output buffers
    packet->output_buffer->send_eof();
    return 0;
}

#if 0
/**
 * init_pool(int n_threads): Creates the pool of threads.
 */

void stage_t::init_pool(pthread_t* stage_handles, int n_threads)
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
#endif

#include "namespace.h"
