/* -*- mode:C++; c-basic-offset:4 -*- */

#include "stage.h"
#include "trace/trace.h"
#include "qpipe_panic.h"
#include "utils.h"
#include "stage_container.h"


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
 *
 *  @param stage_packets The set of packets that this stage is
 *  responsible for processing.
 *
 *  @param stage_container The container holding this stage.
 *
 *  @param accepting_packets Whether this stage should allow the
 *  container to merge more packets with it. Stages implementing
 *  work-sharing should pass true, even though they may want to
 *  manually disable further merges at some point in the
 *  process_packet().
 */

stage_t::stage_t(const char* stage_name,
		 packet_list_t* stage_packets,
		 stage_container_t* stage_container,
		 bool accepting_packets)
    : _stage_packets(stage_packets),
      _stage_accepting_packets(accepting_packets),
      _stage_next_tuple(1),
      _stage_container(stage_container)
{
    
    // copy stage name
    if ( asprintf(&_stage_name, "%s", stage_name) == -1 ) {
	TRACE(TRACE_ALWAYS, "asprintf() failed on stage: %s\n",
	      stage_name);
	QPIPE_PANIC();
    }
  

    // mutex that serializes the creation of the worker threads
    pthread_mutex_init_wrapper(&_stage_lock, NULL);


    packet_list_t::iterator it;
    for (it = stage_packets->begin(); it != stage_packets->end(); ++it) {
        packet_t* packet = *it;
	// Copy _stage_next_tuple (1) into each packet's
	// _stage_next_tuple_on_merge field so we know we're done with
	// these packets when the stage's process() method is done.
	packet->_stage_next_tuple_on_merge = 1;
    }
}



/**
 *  @brief Stage destructor. The worker thread responsible for this
 *  stage should remove it from the enclosing stage_container before
 *  invoking this destructor. Otherwise, we need to worry about the
 *  dispatcher invoking our try_merge() method while we are in the
 *  middle of destroying ourselves.
 */

stage_t::~stage_t() {

    // There should be no packets in our list. We should have
    // re-enqueued them at the end of run().
    if ( !_stage_packets->empty() ) {
	TRACE(TRACE_ALWAYS, "_stage_packets non-empty!\n");
	QPIPE_PANIC();
    }
	
    // destroy name
    free(_stage_name);

    // destroy synch vars    
    pthread_mutex_destroy_wrapper(&_stage_lock);
}



void stage_t::stop_accepting_packets() {
    // Need to protect flag with _stage_lock. Luckily, we should never
    // be holding _stage_lock inside process().
    critical_section_t cs_no_accept(&_stage_lock);
    _stage_accepting_packets = false;
    cs_no_accept.exit();
}



/**
 *  @brief Write a tuple to each waiting output buffer in a chain of
 *  packets.
 *
 *  @return OUTPUT_RETURN_CONTINUE to indicate that we should continue
 *  processing the query. OUTPUT_RETURN_STOP if all packets have been
 *  serviced, sent EOFs, and deleted. OUTPUT_RETURN_ERROR on
 *  unrecoverable error. process() should probably propagate this
 *  error up.
 */
stage_t::output_t stage_t::output(const tuple_t &tuple) {
    
    packet_list_t::iterator it, end;
    unsigned int next_tuple;
    

    critical_section_t cs(&_stage_lock);
    it  = _stage_packets->begin();
    end = _stage_packets->end();
    next_tuple = ++_stage_next_tuple;
    cs.exit();

    
    // Any new packets which merge after this point will not be
    // receiving "tuple" this iteration.


    bool packets_remaining = false;
    tuple_t out_tup;


    while (it != end) {
	
        packet_t* packet = *it;	
	
        // was this tuple selected?
        if(packet->filter->select(tuple)) {
	    if ( packet->output_buffer->alloc_tuple(out_tup) ) {
		// alloc_tuple() failed! For now, assume that it is
		// because the consumer closed. If it is really due to
		// another error, we must invoke terminate_query(),
		// possibly close the output buffer, delete the output
		// buffer if necessary, and delete the packet.
		it = _stage_packets->erase(it);
		terminate_packet_query(packet);
		continue;
	    }

	    // send tuple
	    packet->filter->project(out_tup, tuple);
	}

	// check for early completion
	if ( next_tuple == packet->_stage_next_tuple_needed ) {
	    it = _stage_packets->erase(it);
	    destroy_completed_packet(packet);
	    continue;
	}

	// continue to next merged packet
	++it;
	packets_remaining = true;
	continue;
    }
    
    
    if ( packets_remaining )
	return OUTPUT_RETURN_CONTINUE;
    return OUTPUT_RETURN_STOP;
}



/**
 *  @brief packet should already have been removed from
 *  _staged_packets.
 */
void stage_t::destroy_completed_packet(packet_t* packet) {
    packet->output_buffer->send_eof();
    // TODO check for send_eof() error and delete output
    // buffer
    delete packet;
}



/**
 *  @brief packet should already have been removed from
 *  _staged_packets.
 */
void stage_t::terminate_packet_query(packet_t* packet) {
    packet->notify_client_of_abort();
    packet->output_buffer->send_eof();
    // TODO check for send_eof() error and delete output
    // buffer
    delete packet;
}



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
bool stage_t::try_merge(packet_t* packet) {
 
   // * * * BEGIN CRITICAL SECTION * * *
    critical_section_t cs(&_stage_lock);


    if ( !_stage_accepting_packets ) {
	// stage not longer in a state where it can accept new packets
	return false;
	// * * * END CRITICAL SECTION * * *	
    }
    

    // The _stage_accepting_packets flag cannot change since we are
    // holding the _stage_lock. We can also safely access
    // _stage_packets for the same reason.
    packet_t* head_packet = _stage_packets->front();
    if ( !head_packet->is_mergeable(packet) ) {
	// packet cannot share work with this stage's packet set
	return false;
	// * * * END CRITICAL SECTION * * *
    }


    // if we are here, we detected work sharing!

    // merge operation...
    packet->terminate_inputs();
    _stage_packets->push_front(packet);
    

    // Copy _stage_next_tuple into each packet's
    // _stage_next_tuple_on_merge field so we know how many tuples
    // were missed by this packet.
    packet->_stage_next_tuple_on_merge = _stage_next_tuple;

    
    // * * * END CRITICAL SECTION * * *
    return true;
}



void stage_t::run() {


    if ( process() ) {
	TRACE(TRACE_ALWAYS, "process() failed!\n");
    }

	
    // destroy local state and close input buffer(s)
    terminate_stage();
    
   
    // if we have not yet stopped accepting packets, stop now.
    stop_accepting_packets();


    // Walk through _stage_packets and re-enqueue the packets which
    // have not yet completed. We should try to avoid going to the
    // dispatcher again. We can change this later if there is a
    // compelling reason to.
    packet_list_t::iterator it;
    for (it = _stage_packets->begin(); it != _stage_packets->end(); ) {

	// remove packet from list
        packet_t* packet = *it;
	it = _stage_packets->erase(it);
	
	
	// If packet is finished...
	if ( packet->_stage_next_tuple_on_merge == 1 ) {
	    destroy_completed_packet(packet);
	    continue;
	}

	
	// Otherwise, re-enque the packet
	packet->_stage_next_tuple_needed =
	    packet->_stage_next_tuple_on_merge;
	packet->_stage_next_tuple_on_merge = 0; // reinitialize
	
	// Re-enqueue incomplete packet. The packet cannot come
	// back to us since we are no longer accepting
	// packets. CANNOT BE HOLDING _stage_lock WHEN WE MAKE
	// THIS CALL OR WE INTRODUCE DEADLOCK.
	_stage_container->enqueue(packet);
    }
}



#include "namespace.h"
