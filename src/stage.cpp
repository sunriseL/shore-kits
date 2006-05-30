
#include "stage.h"
#include "trace/trace.h"
#include "qpipe_panic.h"



// include me last!!!
#include "namespace.h"



/*
 * Stage constructor.
 *
 * @param sname The name of this stage.
 */
stage_t::stage_t(const char* sname)
    : queue(this), side_queue(this)
{
  
    name = sname;
  
    // mutex that serializes the creation of the worker threads
    pthread_mutex_init_wrapper(&queue_lock, NULL);
}


/*
 * Destructor: Deletes the main queue of the stage and send a corresponding message
 * to the monitor. The worker_threads do not have to be deleted here, since they are
 * deleted inside the start_thread() function.
 */

stage_t::~stage_t(void)
{
    // There should be no worker threads accessing the packet queue when
    // this function is called. Otherwise, we get race conditions and
    // invalid memory accesses.
    pthread_mutex_destroy_wrapper(&queue_lock);
}

void stage_t::set_not_mergeable(packet_t *packet) {
    critical_section_t cs(&queue_lock);
    packet->mergeable = false;
    cs.exit();
}

void stage_t::enqueue(packet_t *packet)
{
    critical_section_t cs(&queue_lock);
    packet_list_t::iterator it;
    
    // merge with side queue?
    for(it=side_queue.begin(); it != side_queue.end(); ++it) {
        packet_t *other = *it;
        critical_section_t cs2(&other->merge_mutex);
        if(packet->mergeable && packet->is_mergeable(other)) 
            packet->merge(other);

        cs2.exit();
    }

    // TODO: stop now if merged
    
    // main queue?
    for(it=queue.begin(); it != queue.end(); ++it) {
        packet_t *other = *it;
        critical_section_t cs2(&other->merge_mutex);
        if(packet->mergeable && packet->is_mergeable(other)) 
            packet->merge(other);

        cs2.exit();
    }

    // TODO: stop now if merged
    
    // no sharing
    queue.enqueue(packet);
    cs.exit();
}


int stage_t::output(packet_t* packet, const tuple_t &tuple)
{
    critical_section_t cs(&packet->merge_mutex);
    packet_list_t::iterator it=packet->merged_packets.begin();
    cs.exit();

    // send the tuple to the output buffers
    int failed = 1;
    tuple_t out_tup;
    while(it != merged_packets.end()) {
        packet_t *other = *it;
        int ret = 0;

        // was this tuple selected?
        if(other->filter->select(tuple)) {
            ret = packet->output_buffer->alloc_tuple(out_tup);
            packet->filter->project(out_tup, tuple);
            // output succeeds unless all writes fail
            failed &= ret;
        }

        // delete a finished packet?
        if(ret != 0) {
            not_mergeable(other);
            critical_section_t cs(&packet->merge_mutex);
            it = merged_packets.erase(it);
            cs.exit();
            
            if(packet == other)
                not_mergeable(packet);
            else
                delete other;
        }
        else
            ++it;
    }
    
    return failed;
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



int stage_t::process_next_packet(void)
{

    // block until a new packet becomes available
    scope_delete_t<packet_t> packet = queue.dequeue();
    if(packet == NULL)
        {
            TRACE(TRACE_ALWAYS, "Removed NULL packet in stage %s!", name);
            QPIPE_PANIC();
        }
  

    // TODO: process rebinding instructions here
  
    // wait for a green light from the parent
    // TODO: what about the parents of merged packets?
    if( packet->output_buffer->wait_init() ) {
        // remove the packet from the side queue
        critical_section_t cs(&queue_lock);
        
        // make sure nobody else tries to merge
        not_mergeable(packet);
        packet->terminate();
        packet->side_queue.pop_tail();
        
        cs.exit();
        
        // TODO: check if we have any merged packets before closing
        // the input buffer
        return 1;
    }

    // perform stage-specified processing
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
