/* -*- mode:C++; c-basic-offset:4 -*- */

#include "stage_container.h"
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
 *  @param container_name The name of this stage container. Useful for
 *  printing debug information. The constructor will create a copy of
 *  this string, so the caller should deallocate it if necessary.
 */

stage_container_t::stage_container_t(const char* container_name,
				     stage_factory_t* stage_maker)
    : _stage_maker(stage_maker)
{

    // copy container name
    if ( asprintf(&_container_name, "%s", container_name) == -1 ) {
	TRACE(TRACE_ALWAYS, "asprintf() failed on stage: %s\n",
	      container_name);
	QPIPE_PANIC();
    }
  
    // mutex that serializes the creation of the worker threads
    pthread_mutex_init_wrapper(&_container_lock, NULL);
  
    // condition variable that worker threads can wait on to
    // deschedule until more packets arrive
    pthread_cond_init_wrapper(&_container_queue_nonempty, NULL);
}



/**
 *  @brief Stage container destructor. Should only be invoked after a
 *  shutdown() has successfully returned and no more worker threads
 *  are in the system. We will delete every entry of the container
 *  queue.
 */

stage_container_t::~stage_container_t(void) {

    // the container owns its name
    free(_container_name);

    // There should be no worker threads accessing the packet queue when
    // this function is called. Otherwise, we get race conditions and
    // invalid memory accesses.
  
    // destroy synch vars    
    pthread_mutex_destroy_wrapper(&_container_lock);
    pthread_cond_destroy_wrapper (&_container_queue_nonempty);
}



/**
 *  @brief Helper function used to add the specified packet as a new
 *  element in the _container_queue and signal a waiting worker
 *  thread.
 *
 *  THE CALLER MUST BE HOLDING THE _container_lock MUTEX.
 */
void stage_container_t::container_queue_enqueue(packet_t* packet) {

    packet_list_t* plist = new packet_list_t();
    plist->push_back(packet);
    _container_queue.push_back(plist);

    pthread_cond_signal_wrapper(&_container_queue_nonempty);
}



/**
 *  @brief Helper function used to remove the next packet in this
 *  container queue. If no packets are available, wait for one to
 *  appear.
 *
 *  THE CALLER MUST BE HOLDING THE stage_lock MUTEX.
 */

packet_list_t* stage_container_t::container_queue_dequeue() {

    // wait for a packet to appear
    while ( _container_queue.empty() ) {
	pthread_cond_wait_wrapper( &_container_queue_nonempty, &_container_lock );
    }

    // remove available packet
    packet_list_t* plist = _container_queue.front();
    _container_queue.pop_front();
  
    return plist;
}



void stage_container_t::enqueue(packet_t* packet) {

    // error checking
    if(packet == NULL) {
	TRACE(TRACE_ALWAYS, "Called with NULL packet on stage %s!",
	      get_name());
	QPIPE_PANIC();
    }


    // * * * BEGIN CRITICAL SECTION * * *
    critical_section_t cs(&_container_lock);


    // check for non-mergeable packets
    if ( !packet->is_merge_enabled() )  {
	// We are forcing the new packet to not merge with others.
	container_queue_enqueue(packet);
	return; 
	// * * * END CRITICAL SECTION * * *
    }


    // try merging with packets in merge_candidates before they
    // disappear or become non-mergeable
    for(list<stage_t*>::iterator it = _stages.begin(); it != _stages.end(); ++it) {
	// try to merge with this stage
	stage_t* stage = *it;
	if ( stage->try_merge(packet) ) {
	    /* packet was merged with an existing stage */
	    return;
	    // * * * END CRITICAL SECTION * * *
	}
    }


    // If we are here, we could not merge with any of the running
    // stages. Don't give up. We can still try merging with a packet in
    // the container_queue.
    for (list<packet_list_t*>::iterator it = _container_queue.begin(); it != _container_queue.end(); ++it) {

	packet_list_t* cq_plist = *it;
	packet_t* cq_packet = cq_plist->front();


	// No need to acquire queue_pack's merge mutex. No other
	// thread can touch it until we release _container_lock.
    
	// We need to check queue_pack's mergeability flag since some
	// packets are non-mergeable from the beginning. Don't need to
	// grab its merge_mutex because its mergeability status could
	// not have changed while it was in the queue.
	if ( cq_packet->is_mergeable(packet) ) {
	    // merge operation...
	    packet->terminate_inputs();
	    cq_plist->push_back(packet);
	    return;
	    // * * * END CRITICAL SECTION * * *
	}
    }


    // No work sharing detected. We can now give up and insert the new
    // packet into the stage_queue.
    container_queue_enqueue(packet);
    // * * * END CRITICAL SECTION * * *
};



void stage_container_t::run() {


    while (1) {
	
	// wait for a packet to become available
	critical_section_t cs(&_container_lock);
	packet_list_t* packets = container_queue_dequeue();
	cs.exit();
	
	
	// error checking
	if(packets == NULL) {
	    TRACE(TRACE_ALWAYS, "Dequeued NULL packet list in %s stage queue!", get_name());
	    QPIPE_PANIC();
	}
	if (packets->empty()) {
	    TRACE(TRACE_ALWAYS, "Dequeued empty packet_list_t in %s stage queue!", get_name());
	    QPIPE_PANIC();
	}
	

	// TODO: process rebinding instructions here

    
	pointer_guard_t <stage_t> stage = _stage_maker->create_stage(packets, this);
	if ( stage == NULL ) {
	    TRACE(TRACE_ALWAYS, "_stage_maker->create_stage() failed\n");
	    QPIPE_PANIC();
	}
	stage->run();


	// TODO: check for shutdown
    }
}



#include "namespace.h"
