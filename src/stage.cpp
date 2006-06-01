/* -*- mode:C++; c-basic-offset:4 -*- */

#include "stage.h"
#include "trace/trace.h"
#include "qpipe_panic.h"
#include "utils.h"

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <cstdio>
#include <cstring>



// include me last!!!
#include "namespace.h"



/**
 *  @brief Stage constructor.
 *
 *  @param stage_name The name of this stage. The constructor will
 *  create a copy of this string.
 */

stage_t::stage_t(const char* stage_name) {

    // copy stage name
    if ( asprintf(&_stage_name, "%s", stage_name) == -1 ) {
	TRACE(TRACE_ALWAYS, "asprintf() failed on stage: %s\n",
	      stage_name);
	QPIPE_PANIC();
    }
  
    // mutex that serializes the creation of the worker threads
    pthread_mutex_init_wrapper(&stage_lock, NULL);

    // condition variable that worker threads can wait on to
    // deschedule until more packets arrive
    pthread_cond_init_wrapper(&stage_queue_packet_available, NULL);

    // the subclass must register itself with the dispatcher for all
    // the packet types it wishes to handle
}



/**
 *  @brief Stage destructor. Deletes the main queue of the stage and
 *  send a corresponding message to the monitor. The worker_thread_t's
 *  do not have to be deleted here, since they are deleted when they
 *  exit.
 */

stage_t::~stage_t(void) {

    free(_stage_name);

    // There should be no worker threads accessing the packet queue when
    // this function is called. Otherwise, we get race conditions and
    // invalid memory accesses.

    // destroy synch vars    
    pthread_mutex_destroy_wrapper(&stage_lock);
    pthread_cond_destroy_wrapper(&stage_queue_packet_available);
}



/**
 *  @brief Helper function used to remove the next packet in this
 *  stage's queue. If no packets are available, wait for one to
 *  appear.
 *
 *  This function acquires the stage_lock mutex and waits in the
 *  stage_queue_packet_available is no packets are available.
 */

packet_t* stage_t::dequeue(void) {

    TRACE(TRACE_PACKET_FLOW, "Trying to dequeue packet from stage %s\n",
	  get_name());
  
    // * * * BEGIN CRITICAL SECTION * * *
    pthread_mutex_lock_wrapper(&stage_lock);
  
    while ( stage_queue.empty() ) {
	pthread_cond_wait_wrapper( &stage_queue_packet_available, &stage_lock );
    }

    // remove packet
    packet_t* p = stage_queue.front();
    stage_queue.pop_front();

    pthread_mutex_unlock_wrapper(&stage_lock);
    // * * * END CRITICAL SECTION * * *


    TRACE(TRACE_PACKET_FLOW, "Dequeued packet %s from stage %s\n",
	  p->packet_id,
	  get_name());
  
    return p;
}



/**
 *  @brief Add the specified packet to the stage_queue and signal any
 *  waiting worker threads. The caller must be holding the stage_lock
 *  mutex.
 */
void stage_t::add_to_stage_queue(packet_t* packet) {

    stage_queue.push_back(packet);
    pthread_cond_signal_wrapper(&stage_queue_packet_available);
}



void stage_t::enqueue(packet_t *new_pack) {
    
    // error checking
    if(new_pack == NULL) {
	TRACE(TRACE_ALWAYS, "Called with NULL packet for stage %s!",
	      get_name());
	QPIPE_PANIC();
    }
  

    packet_list_t::iterator it;


    // * * * BEGIN CRITICAL SECTION * * *
    critical_section_t cs(&stage_lock);
    

    if ( !new_pack->mergeable ) {
	// We are forcing the new packet to not merge with others.
	add_to_stage_queue(new_pack);
	return;
    }


    // try merging with packets in merge_candidates before they
    // disappear or become non-mergeable
    for(it = merge_candidates.begin(); it != merge_candidates.end(); ++it) {

	// cand is a possible candidate for us to merge new_pack with
        packet_t *cand = *it;
    
	// * * * BEGIN NESTED CRITICAL SECTION * * *
	critical_section_t cs2(&cand->merge_mutex);
        if (cand->mergeable && cand->is_mergeable(new_pack)) { 
            cand->merge(new_pack);
	    return;
	}
	// * * * END NESTED CRITICAL SECTION * * *
    }
    

    // If we are here, we could not merge with any packet in the
    // merge_candidates set. Don't give up. We can still try merging
    // with a packet in the stage_queue.
    
    // main queue?
    for (it = stage_queue.begin(); it != stage_queue.end(); ++it) {

	// queue_pack is a possible candidate for us to merge new_pack
	// with
        packet_t *queue_pack = *it;

	// No need to acquire queue_pack's merge mutex. No other
	// thread can touch it until we release the stage_lock.
	
	// We need to check queue_pack's mergeability flag since some
	// packets are non-mergeable from the beginning. Don't need to
	// grab its merge_mutex because its mergeability status could
	// not have changed while it was in the queue.
        if (queue_pack->mergeable && queue_pack->is_mergeable(new_pack)) {
            queue_pack->merge(new_pack);
	    return;
	}
    }


    // No work sharing detected. We can now give up and insert the new
    // packet into the stage_queue.
    add_to_stage_queue(new_pack);
    
    // * * * END CRITICAL SECTION * * *
}



/**
 *  @brief Set the specified packet to be a non-mergeable one. This
 *  function should only be called by a worker thread for this stage,
 *  on a packet assigned to this stage.
 */

void stage_t::set_not_mergeable(packet_t *packet) {

    // Grab stage_lock so other threads see a consistant view of the
    // packet (for example, threads calling enqueue()).
    critical_section_t cs(&stage_lock);
    packet->mergeable = false;
    
    // * * * END CRITICAL SECTION * * *
}



int stage_t::process_next_packet(void) {

    // block until a new packet becomes available
    scope_delete_t<packet_t> packet = dequeue();

    if(packet == NULL) {
        // error checking
	TRACE(TRACE_ALWAYS, "Removed NULL packet in stage %s!", get_name());
	QPIPE_PANIC();
    }

    // TODO: process rebinding instructions here
  

    // Wait for a green light from the parent. What about the parents
    // of merged packets? Even if we wait for the packets we have,
    // there could be other packets merged with us later. We really
    // need to invoke wait_init() on every packet before outputing a
    // tuple to it.
    if( packet->output_buffer->wait_init() ) {
        // remove the packet from the side queue
        critical_section_t cs(&stage_lock);
        
        // make sure nobody else tries to merge
        set_not_mergeable(packet);
        packet->terminate();
        packet->merged_packets.pop_back();
        
        // TODO: check if we have any merged packets before closing
        // the input buffer
        return 1;
    }

    // perform stage-specified processing
    TRACE(TRACE_DEBUG, "wait_init() returned for %s\n", packet->packet_id);

    int early_termination = process_packet(packet);

    if(early_termination) {
        // TODO: terminate all packets in the chain
    }

    // close the output buffer
    // TODO: also close merged packets' output buffers
    packet->output_buffer->send_eof();
    return 0;
}



int stage_t::output(packet_t* packet, const tuple_t &tuple)
{

    packet_list_t::iterator it;
    {
        critical_section_t cs(&packet->merge_mutex);
        it = packet->merged_packets.begin();
    }

    // send the tuple to the output buffers
    int failed = 1;
    tuple_t out_tup;
    while(it != packet->merged_packets.end()) {
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
            set_not_mergeable(other);
            {
                critical_section_t cs(&packet->merge_mutex);
                it = packet->merged_packets.erase(it);
            }
            
            if(packet == other)
                set_not_mergeable(packet);
            else
                delete other;
        }
        else
            ++it;
    }
    
    return failed;
}



#include "namespace.h"
