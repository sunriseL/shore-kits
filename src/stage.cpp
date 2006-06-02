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
 *  @brief Helper function used to add the specified packet to the
 *  stage_queue and signal a waiting worker thread, if one exists.
 *
 *  THE CALLER MUST BE HOLDING THE stage_lock MUTEX.
 */
void stage_t::stage_queue_enqueue(packet_t* packet) {

    TRACE(TRACE_PACKET_FLOW, "Enqueuing packet %s in stage %s\n",
	  packet->packet_id,
	  get_name());

    stage_queue.push_back(packet);
    pthread_cond_signal_wrapper(&stage_queue_packet_available);
}



/**
 *  @brief Helper function used to remove the next packet in this
 *  stage's queue. If no packets are available, wait for one to
 *  appear.
 *
 *  THE CALLER MUST BE HOLDING THE stage_lock MUTEX.
 */

packet_t* stage_t::stage_queue_dequeue(void) {

    // wait for a packet to appear
    while ( stage_queue.empty() ) {
	pthread_cond_wait_wrapper( &stage_queue_packet_available, &stage_lock );
    }

    // remove available packet
    packet_t* packet = stage_queue.front();
    stage_queue.pop_front();
    
    TRACE(TRACE_PACKET_FLOW, "Dequeued packet %s in stage %s\n",
	  packet->packet_id,
	  get_name());
  
    return packet;
}






void stage_t::enqueue(packet_t* new_packet) {
    
    // error checking
    if(new_packet == NULL) {
	TRACE(TRACE_ALWAYS, "Called with NULL packet on stage %s!",
	      get_name());
	QPIPE_PANIC();
    }
  

    packet_list_t::iterator it;
    
    
    // * * * BEGIN CRITICAL SECTION * * *
    critical_section_t cs(&stage_lock);
    

    // check for non-mergeable packets
    if ( !new_packet->mergeable ) {
	// We are forcing the new packet to not merge with others.
	stage_queue_enqueue(new_packet);
	return;
    }




    // try merging with packets in merge_candidates before they
    // disappear or become non-mergeable
    for(it = merge_candidates.begin(); it != merge_candidates.end(); ++it) {

	// cand is a possible candidate for us to merge new_pack with
        packet_t* mc_packet = *it;
    
	// * * * BEGIN NESTED CRITICAL SECTION * * *
	critical_section_t cs2(&mc_packet->merge_mutex);
        if (mc_packet->mergeable && mc_packet->is_mergeable(new_packet)) {
            mc_packet->merge(new_packet);
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
        packet_t* sq_packet = *it;

	// No need to acquire queue_pack's merge mutex. No other
	// thread can touch it until we release the stage_lock.
	
	// We need to check queue_pack's mergeability flag since some
	// packets are non-mergeable from the beginning. Don't need to
	// grab its merge_mutex because its mergeability status could
	// not have changed while it was in the queue.
        if (sq_packet->mergeable && sq_packet->is_mergeable(new_packet)) {
            sq_packet->merge(new_packet);
	    return;
	}
    }


    // No work sharing detected. We can now give up and insert the new
    // packet into the stage_queue.
    stage_queue_enqueue(new_packet);
    
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


    // wait for a packet to become available
    pthread_mutex_lock_wrapper(&stage_lock);
    scope_delete_t<packet_t> packet = stage_queue_dequeue();
    pthread_mutex_lock_wrapper(&stage_lock);


    // error checking
    if(packet == NULL) {
	TRACE(TRACE_ALWAYS, "Dequeued NULL packet in %s stage queue!", get_name());
	QPIPE_PANIC();
    }

    
    // TODO: process rebinding instructions here
  

    int early_termination = process_packet(packet);
    if(early_termination) {
        // TODO: terminate all packets in the chain
    }
    
    
    // close the output buffer
    // TODO: also close merged packets' output buffers
    packet->output_buffer->send_eof();
    return 0;
}



int stage_t::output(packet_t* packet, const tuple_t &tuple) {

    packet_list_t::iterator it;
    {
        critical_section_t cs(&packet->merge_mutex);
        it = packet->merged_packets.begin();
    }

    // send the tuple to the output buffers
    int failed = 1;
    tuple_t out_tup;

    // we are not considering the case the tuple not pass any filter. 
    // In that case variable failed was untouched and incorrectly was returning fail.
    int filter_pass = 0;

    while(it != packet->merged_packets.end()) {
        packet_t *other = *it;
        int ret = 0;

        // was this tuple selected?
        if(other->filter->select(tuple)) {
            ret = packet->output_buffer->alloc_tuple(out_tup);

            packet->filter->project(out_tup, tuple);
            // output succeeds unless all writes fail
            failed &= ret;

	    // the tuple passed the filter of at least one consumer
	    filter_pass++;
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


    // if no tuple passed filter return success
    if (filter_pass > 0) {
	return failed;
    }
    else {
	return (0);
    }
}



#include "namespace.h"
