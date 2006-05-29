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

void stage_t::enqueue(packet_t *packet) {
    queue->insert_packet(packet);
}

#ifdef CHANGE_MIND
// not implemented for now
bool stage_t::is_mergeable(packet_t *packet) {
    bool result;
    pthread_mutex_lock_wrapper(&stage_lock);
    result = packet->mergeable;
    pthread_mutex_unlock_wrapper(&stage_lock);
    return result;
}
#endif

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
    scope_delete_t<packet_t> packet = queue->remove_next_packet();
    if(packet == NULL) {
        TRACE(TRACE_ALWAYS, "Null packet arrive at stage %s!", name);
        return 1;
    }

    // TODO: process rebinding instructions here

    // wait for a green light from the parent
    // TODO: what about the parents of merged packets?
    if(packet->output_buffer->wait_init()) {
#ifdef MERGING_ADDED
        // make sure nobody else tries to merge
        not_mergeable(packet);
#endif

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

#include "namespace.h"
