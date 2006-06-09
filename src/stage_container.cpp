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
 *  THE CALLER MUST BE HOLDING THE _container_lock MUTEX.
 */
void stage_container_t::container_queue_enqueue_no_merge(packet_list_t* packets) {
    _container_queue.push_back(packets);
    pthread_cond_signal_wrapper(&_container_queue_nonempty);
}



/**
 *  @brief Helper function used to add the specified packet as a new
 *  element in the _container_queue and signal a waiting worker
 *  thread.
 *
 *  THE CALLER MUST BE HOLDING THE _container_lock MUTEX.
 */
void stage_container_t::container_queue_enqueue(packet_t* packet) {
    packet_list_t* packets = new packet_list_t();
    packets->push_back(packet);
    container_queue_enqueue_no_merge(packets);
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

/**
 * @brief Outputs a page of tuples. The caller retains ownership of the page.
 */
stage_t::result_t stage_container_t::stage_adaptor_t::output_page(tuple_page_t *page) {
    packet_list_t::iterator it, end;
    unsigned int next_tuple;
    

    critical_section_t cs(&_stage_adaptor_lock);
    it  = _packet_list->begin();
    end = _packet_list->end();
    _next_tuple += page->tuple_count();
    next_tuple = _next_tuple;
    cs.exit();

    
    // Any new packets which merge after this point will not 
    // receive this page.
    

    bool packets_remaining = false;
    tuple_t out_tup;

    while (it != end) {
	
        packet_t* packet = *it;
        bool terminate = false;

        // drain this page into the packet's output buffer
        tuple_page_t::iterator page_it = page->begin();
        for( ; page_it != page->end(); ++page_it) {
            
            // was this tuple selected?
            if(packet->filter->select(*page_it)) {
                if ( packet->output_buffer->alloc_tuple(out_tup) ) {
                    // alloc_tuple() failed!
                    terminate = true;
                    break;
                }

                // send tuple
                packet->filter->project(out_tup, *page_it);
            }
        }


        // check for completed packets
        if ( next_tuple == packet->_stage_next_tuple_needed ) {
            // this packet has received all data it needs!
            terminate = true;
        }
        
	// continue to next merged packet
        if(terminate) {
            terminate_packet(packet, 0);
            it = _packet_list->erase(it);
        }
        else
            ++it;
        
	packets_remaining = true;
    }
    
    
    if ( packets_remaining )
	return stage_t::RESULT_CONTINUE;
    return stage_t::RESULT_STOP;
}



void stage_container_t::enqueue(packet_t* packet) {


    assert(packet != NULL);


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
    list<stage_adaptor_t*>::iterator sit = _container_current_stages.begin();
    for( ; sit != _container_current_stages.end(); ++sit) {
	// try to merge with this stage
	stage_container_t::stage_adaptor_t* ad = *sit;
	if ( ad->try_merge(packet) ) {
	    /* packet was merged with this existing stage */
	    return;
	    // * * * END CRITICAL SECTION * * *
	}
    }


    // If we are here, we could not merge with any of the running
    // stages. Don't give up. We can still try merging with a packet in
    // the container_queue.
    list<packet_list_t*>::iterator cit = _container_queue.begin();
    for ( ; cit != _container_queue.end(); ++cit) {
	
	packet_list_t* cq_plist = *cit;
	packet_t* cq_packet = cq_plist->front();


	// No need to acquire queue_pack's merge mutex. No other
	// thread can touch it until we release _container_lock.
    
	// We need to check queue_pack's mergeability flag since some
	// packets are non-mergeable from the beginning. Don't need to
	// grab its merge_mutex because its mergeability status could
	// not have changed while it was in the queue.
	if ( cq_packet->is_mergeable(packet) ) {
	    // add this packet to the list of already merged packets
	    // in the container queue
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



/**
 *  @brief Try to merge the specified packet into this stage. This
 *  function assumes that packet has its mergeable flag set to
 *  true.
 *
 *  This method relies on the stage's current state (whether
 *  _stage_accepting_packets is true) as well as the
 *  is_mergeable() method used to compare two packets.
 *
 *  @param packet The packet to try and merge.
 *
 *  @return true if the merge was successful. false otherwise.
 */
bool stage_container_t::stage_adaptor_t::try_merge(packet_t* packet) {
 
   // * * * BEGIN CRITICAL SECTION * * *
    critical_section_t cs(&_stage_adaptor_lock);


    if ( !_still_accepting_packets ) {
	// stage not longer in a state where it can accept new packets
	return false;
	// * * * END CRITICAL SECTION * * *	
    }
    

    // The _still_accepting_packets flag cannot change since we are
    // holding the _stage_adaptor_lock. We can also safely access
    // _packet for the same reason.
    if ( !_packet->is_mergeable(packet) ) {
	// packet cannot share work with this stage
	return false;
	// * * * END CRITICAL SECTION * * *
    }


    // if we are here, we detected work sharing!
    _packet_list->push_front(packet);
    packet->_stage_next_tuple_on_merge = _next_tuple;
    if ( _next_tuple == 1 ) {
	// Stage has not produced any results yet. Safe to terminate
	// our own inputs.
	packet->terminate_inputs();
    }


    // * * * END CRITICAL SECTION * * *
    return true;
}



void stage_container_t::stage_adaptor_t::terminate_packet(packet_t* packet, int stage_done) {

    // TODO check for error and delete
    packet->output_buffer->send_eof();
    

    if ( !stage_done && (packet == _packet) ) {
	// primary packet... cannot close inputs until the stage is
	// done.
	return;
    }
    

    // if we are not the primary, we can simply progate shutdown to
    // children (just need to delete their subtrees and packet state).
    packet->terminate_inputs();
    delete packet;
}



void stage_container_t::stage_adaptor_t::run_stage(stage_t* stage) {

    assert( stage != NULL );
    stage->init(this);
    int process_ret = stage->process();
    stop_accepting_packets();

    switch(process_ret) {
    case stage_t::RESULT_STOP:
        break;
        
    case stage_t::RESULT_ERROR:
	TRACE(TRACE_ALWAYS, "process_packet() returned error. Aborting queries...\n");
	abort_queries();
	assert (_packet_list->empty());
	delete _packet_list;
	return;
        
    default:
        TRACE(TRACE_ALWAYS, "process_packet() returned an invalid result %d\n",
	      process_ret);
        QPIPE_PANIC();
    }

    // Walk through _stage_packets and re-enqueue the packets which
    // have not yet completed. We should try to avoid going to the
    // dispatcher again. We can change this later if there is a
    // compelling reason to.
    packet_list_t::iterator it;
    for (it = _packet_list->begin(); it != _packet_list->end(); ) {
	    
	// remove packet from list
	packet_t* packet = *it;
	    
	    
	// If packet is finished...
	if ( packet->_stage_next_tuple_on_merge == 1 ) {
	    it = _packet_list->erase(it);
	    terminate_packet(packet, 1);
	    continue;
	}

	    
	// otherwise, simply update what this packet needs
	packet->_stage_next_tuple_needed =
	    packet->_stage_next_tuple_on_merge;
	packet->_stage_next_tuple_on_merge = 0; // reinitialize
    }

    // re-enqueue incomplete packets if we have them
    if ( _packet_list->empty() )
	delete _packet_list;
    else
	_container->container_queue_enqueue_no_merge(_packet_list);
}



void stage_container_t::stage_adaptor_t::abort_queries() {


    // process_packet() succeeded!
    stop_accepting_packets();
    

    // Walk through _stage_packets and re-enqueue the packets which
    // have not yet completed. We should try to avoid going to the
    // dispatcher again. We can change this later if there is a
    // compelling reason to.
    packet_list_t::iterator it;
    for (it = _packet_list->begin(); it != _packet_list->end(); ) {
	    
	// remove packet from list
	packet_t* packet = *it;

	// If packet is finished...
	it = _packet_list->erase(it);
	
	// TODO need to invoke packet->client_buffer->terminate();
	
	terminate_packet(packet, 1);
    }
}



void stage_container_t::run() {


    while (1) {
	
	// wait for a packet to become available
	critical_section_t cs(&_container_lock);
	packet_list_t* packets = container_queue_dequeue();
	cs.exit();
	
	
	// error checking
	assert( packets != NULL );
	assert( !packets->empty() );


	// TODO: process rebinding instructions here

    
 	pointer_guard_t <stage_t> stage = _stage_maker->create_stage();
        size_t tuple_size = packets->front()->filter->input_tuple_size();
	stage_adaptor_t(this, packets, tuple_size).run_stage(stage);
	

	// TODO: check for container shutdown
    }
}



#include "namespace.h"
